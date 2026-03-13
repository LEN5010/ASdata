import csv
from collections import defaultdict
from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
BLACKLIST_CSV = ROOT / "黑名单" / "uid_blacklist.csv"
OUT_DIR = ROOT / "分析结果"

SOURCE_LIVE = "乃琳_鸣潮"
HOSTS = ["嘉然", "乃琳", "贝拉"]
SUMMARY_PRESENT_THRESHOLDS = [1, 2, 3, 5]
SUMMARY_ACTIVE_THRESHOLDS = [1, 2, 3]

OUT_CSV = OUT_DIR / "fans_conversion_三人解释性阈值_摘要.csv"
OUT_LONG_CSV = OUT_DIR / "fans_conversion_三人解释性阈值_非零长表.csv"
OUT_SVG = OUT_DIR / "fans_conversion_三人解释性阈值.svg"
LEGACY_OUT_CSV = OUT_DIR / "fans_conversion_三人多阈值.csv"

FONT_FAMILY = "'Microsoft YaHei','PingFang SC','Noto Sans CJK SC',sans-serif"


def safe_str(v):
    if v is None:
        return ""
    return str(v).strip()


def safe_int(v, default=0):
    try:
        if v is None:
            return default
        t = str(v).strip()
        if t == "":
            return default
        return int(float(t))
    except Exception:
        return default


def load_csv_rows(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_blacklist(path: Path):
    if not path.exists():
        return set()
    uids = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            uid = safe_str(row.get("uid"))
            if uid:
                uids.add(uid)
    return uids


def is_valid_uid(uid: str) -> bool:
    return bool(uid) and uid.isdigit() and int(uid) > 0


def session_key(row):
    return (
        safe_str(row.get("live_id"))
        or safe_str(row.get("live_name"))
        or safe_str(row.get("start_date"))
    )


def session_ts(row):
    return safe_int(row.get("start_date"), 0) or safe_int(row.get("first_send_date"), 0)


def fmt_pct(value: float) -> str:
    return f"{value:.1%}"


def build_stats(detail_rows, blacklist, source_live, hosts):
    source_host, source_type = source_live.split("_", 1)

    source_first_ts = {}
    rows_by_uid = defaultdict(list)
    invalid_uid_count = 0
    blacklisted_count = 0

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        if uid in blacklist:
            blacklisted_count += 1
            continue
        if not is_valid_uid(uid):
            invalid_uid_count += 1
            continue

        host = safe_str(row.get("host"))
        live_type = safe_str(row.get("live_type"))
        sk = session_key(row)
        st = session_ts(row)
        if not host or not live_type or not sk or not st:
            continue

        record = {
            "uid": uid,
            "host": host,
            "live_type": live_type,
            "session_key": sk,
            "session_ts": st,
            "is_present": safe_int(row.get("is_present"), 0),
            "is_active": safe_int(row.get("is_active"), 0),
        }
        rows_by_uid[uid].append(record)

        if host == source_host and live_type == source_type and record["is_present"] >= 1:
            old = source_first_ts.get(uid)
            if old is None or st < old:
                source_first_ts[uid] = st

    source_users = sorted(source_first_ts)
    source_total = len(source_users)
    if source_total == 0:
        raise ValueError(f"source 用户数为0：{source_live}")

    user_host_present_sessions = defaultdict(set)
    user_host_active_sessions = defaultdict(set)

    for uid in source_users:
        first_ts = source_first_ts[uid]
        for row in rows_by_uid.get(uid, []):
            if row["session_ts"] <= first_ts:
                continue
            host = row["host"]
            if host not in hosts:
                continue
            if row["is_present"] >= 1:
                user_host_present_sessions[(uid, host)].add(row["session_key"])
            if row["is_active"] >= 1:
                user_host_active_sessions[(uid, host)].add(row["session_key"])

    max_present_threshold = 0
    max_active_threshold = 0
    for host in hosts:
        for uid in source_users:
            max_present_threshold = max(max_present_threshold, len(user_host_present_sessions.get((uid, host), set())))
            max_active_threshold = max(max_active_threshold, len(user_host_active_sessions.get((uid, host), set())))

    summary_rows = []
    long_rows = []
    for host in hosts:
        summary_row = {
            "host": host,
            "source_user_count": source_total,
            "post_present_max_nonzero_threshold": 0,
            "post_active_max_nonzero_threshold": 0,
        }

        for threshold in range(1, max_present_threshold + 1):
            count = sum(1 for uid in source_users if len(user_host_present_sessions.get((uid, host), set())) >= threshold)
            rate = round(count / source_total, 4)
            if threshold in SUMMARY_PRESENT_THRESHOLDS:
                summary_row[f"post_ge{threshold}_count"] = count
                summary_row[f"post_ge{threshold}_rate"] = rate
            if count > 0:
                summary_row["post_present_max_nonzero_threshold"] = threshold
                long_rows.append({
                    "host": host,
                    "metric": "post_present",
                    "threshold": threshold,
                    "source_user_count": source_total,
                    "user_count": count,
                    "user_rate": rate,
                })

        for threshold in range(1, max_active_threshold + 1):
            count = sum(1 for uid in source_users if len(user_host_active_sessions.get((uid, host), set())) >= threshold)
            rate = round(count / source_total, 4)
            if threshold in SUMMARY_ACTIVE_THRESHOLDS:
                summary_row[f"post_active_ge{threshold}_count"] = count
                summary_row[f"post_active_ge{threshold}_rate"] = rate
            if count > 0:
                summary_row["post_active_max_nonzero_threshold"] = threshold
                long_rows.append({
                    "host": host,
                    "metric": "post_active",
                    "threshold": threshold,
                    "source_user_count": source_total,
                    "user_count": count,
                    "user_rate": rate,
                })

        summary_rows.append(summary_row)

    meta = {
        "source_live": source_live,
        "source_user_count": source_total,
        "invalid_uid_count": invalid_uid_count,
        "blacklisted_count": blacklisted_count,
        "max_present_threshold": max_present_threshold,
        "max_active_threshold": max_active_threshold,
    }
    return summary_rows, long_rows, meta


def write_csv(rows, path: Path, fieldnames=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows and fieldnames is None:
        fieldnames = list(rows[0].keys())
    if not fieldnames:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def svg_start(width, height):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="white"/>',
    ]


def svg_end(lines, output_path: Path):
    lines.append("</svg>")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def add_text(lines, x, y, text, size=12, anchor="start", weight="normal", fill="#1f2328"):
    lines.append(
        f'<text x="{x}" y="{y}" font-family="{FONT_FAMILY}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{escape(str(text))}</text>'
    )


def add_line(lines, x1, y1, x2, y2, stroke="#d0d7de", stroke_width=1, dash=False):
    extra = ' stroke-dasharray="4 4"' if dash else ""
    lines.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{stroke_width}"{extra}/>')


def add_polyline(lines, points, color, stroke_width=3):
    point_str = " ".join(f"{x},{y}" for x, y in points)
    lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="{stroke_width}" points="{point_str}"/>')
    for x, y in points:
        lines.append(f'<circle cx="{x}" cy="{y}" r="3.5" fill="{color}"/>')


def build_threshold_svg(summary_rows, meta, out_svg: Path):
    colors = {"嘉然": "#5B8FF9", "乃琳": "#61DDAA", "贝拉": "#9270CA"}
    width = 1180
    height = 720
    left = 110
    right = 50
    top = 90
    mid_gap = 90
    chart_h = 220
    chart_w = width - left - right

    lines = svg_start(width, height)
    add_text(lines, width / 2, 36, f"{meta['source_live']} 人群三人后续承接解释性阈值", size=24, anchor="middle", weight="bold")
    add_text(lines, width / 2, 60, f"source_user_count={meta['source_user_count']} ｜ 口径：首次进入 source 后的 post_source 场次", size=12, anchor="middle", fill="#57606a")

    panels = [
        ("post_present", top, chart_h, SUMMARY_PRESENT_THRESHOLDS, "后续到场阈值（1 / 2 / 3 / 5 场）"),
        ("post_active", top + chart_h + mid_gap, chart_h, SUMMARY_ACTIVE_THRESHOLDS, "后续有效到场阈值（1 / 2 / 3 场）"),
    ]

    for metric, panel_top, panel_h, thresholds, title in panels:
        add_text(lines, left, panel_top - 20, title, size=16, weight="bold")
        for i in range(6):
            rate = 1 - i / 5
            y = panel_top + panel_h * i / 5
            add_line(lines, left, y, left + chart_w, y, dash=True)
            add_text(lines, left - 10, y + 4, fmt_pct(rate), size=11, anchor="end", fill="#57606a")
        for idx, threshold in enumerate(thresholds):
            x = left + chart_w * idx / max(len(thresholds) - 1, 1)
            add_line(lines, x, panel_top, x, panel_top + panel_h, stroke="#eef2f6")
            add_text(lines, x, panel_top + panel_h + 20, threshold, size=11, anchor="middle", fill="#57606a")

        for host in HOSTS:
            row = next(r for r in summary_rows if r["host"] == host)
            points = []
            for idx, threshold in enumerate(thresholds):
                key = f"post_ge{threshold}_rate" if metric == "post_present" else f"post_active_ge{threshold}_rate"
                value = float(row.get(key, 0) or 0)
                x = left + chart_w * idx / max(len(thresholds) - 1, 1)
                y = panel_top + panel_h * (1 - value)
                points.append((x, y))
            add_polyline(lines, points, colors[host])
            add_text(lines, points[-1][0] + 12, points[-1][1] + 4, host, size=12, fill=colors[host])

    svg_end(lines, out_svg)




def main():
    detail_rows = load_csv_rows(DETAIL_CSV)
    blacklist = load_blacklist(BLACKLIST_CSV)

    summary_rows, long_rows, meta = build_stats(detail_rows, blacklist, SOURCE_LIVE, HOSTS)

    write_csv(summary_rows, OUT_CSV)
    write_csv(summary_rows, LEGACY_OUT_CSV)
    write_csv(long_rows, OUT_LONG_CSV)
    build_threshold_svg(summary_rows, meta, OUT_SVG)

    print(f"[OK] summary csv: {OUT_CSV}")
    print(f"[OK] legacy csv : {LEGACY_OUT_CSV}")
    print(f"[OK] long csv   : {OUT_LONG_CSV}")
    print(f"[OK] plot svg   : {OUT_SVG}")
    print(f"[OK] source_user_count: {meta['source_user_count']}")


if __name__ == "__main__":
    main()
