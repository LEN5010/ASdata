import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from analysis_settings import BLACKLIST_WINDOW_END_MS, BLACKLIST_WINDOW_START_MS


ROOT = Path(__file__).resolve().parent
DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
OUTPUT_CSV = ROOT / "黑名单" / "uid_blacklist.csv"


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def fmt_date(ts_ms):
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")


def load_detail_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def build_blacklist_rows(detail_rows):
    stats = defaultdict(lambda: {
        "uid": "",
        "uname": "",
        "matched_session_count": 0,
        "first_hit_ts": 0,
        "last_hit_ts": 0,
    })

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        if not uid or not uid.isdigit():
            continue

        event_ts = (
            safe_int(row.get("first_send_date"), 0)
            or safe_int(row.get("start_date"), 0)
            or safe_int(row.get("last_send_date"), 0)
        )
        if event_ts < BLACKLIST_WINDOW_START_MS or event_ts > BLACKLIST_WINDOW_END_MS:
            continue

        stat = stats[uid]
        stat["uid"] = uid
        uname = safe_str(row.get("uname"))
        if uname and not stat["uname"]:
            stat["uname"] = uname
        stat["matched_session_count"] += 1
        stat["first_hit_ts"] = event_ts if stat["first_hit_ts"] == 0 else min(stat["first_hit_ts"], event_ts)
        stat["last_hit_ts"] = max(stat["last_hit_ts"], event_ts)

    rows = []
    for stat in sorted(stats.values(), key=lambda item: (item["first_hit_ts"], item["uid"])):
        rows.append({
            "uid": stat["uid"],
            "uname": stat["uname"],
            "matched_session_count": stat["matched_session_count"],
            "first_hit_ts": stat["first_hit_ts"],
            "first_hit_text": fmt_date(stat["first_hit_ts"]),
            "last_hit_ts": stat["last_hit_ts"],
            "last_hit_text": fmt_date(stat["last_hit_ts"]),
            "rule": "2025-06-09~2025-12-08 发言过",
        })
    return rows


def write_csv(rows, output_csv: Path):
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "uid",
        "uname",
        "matched_session_count",
        "first_hit_ts",
        "first_hit_text",
        "last_hit_ts",
        "last_hit_text",
        "rule",
    ]
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    detail_rows = load_detail_rows(DETAIL_CSV)
    rows = build_blacklist_rows(detail_rows)
    write_csv(rows, OUTPUT_CSV)
    print(f"[OK] 黑名单输出: {OUTPUT_CSV}")
    print(f"[OK] 黑名单 UID 数: {len(rows)}")
    print("[OK] 规则: 2025-06-09 00:00:00 至 2025-12-08 23:59:59 发言过")


if __name__ == "__main__":
    main()
