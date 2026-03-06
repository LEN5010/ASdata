# fans_plot.py
import os
import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".mplconfig"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".cache"))

import matplotlib.pyplot as plt
import numpy as np


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
BLACKLIST_CSV = ROOT / "黑名单" / "uid_blacklist.csv"
OUT_DIR = ROOT / "分析结果"

SOURCE_LIVE = "乃琳_鸣潮"
HOSTS = ["嘉然", "乃琳", "贝拉"]


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


def session_key(row):
    return (
        safe_str(row.get("live_id"))
        or safe_str(row.get("live_name"))
        or safe_str(row.get("start_date"))
    )


def build_stats(detail_rows, source_live, hosts):
    source_host, source_type = source_live.split("_", 1)

    source_users = set()
    user_host_present_sessions = defaultdict(set)
    user_host_active_sessions = defaultdict(set)

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        host = safe_str(row.get("host"))
        live_type = safe_str(row.get("live_type"))
        if not uid or not host or not live_type:
            continue

        sk = session_key(row)
        if not sk:
            continue

        is_present = safe_int(row.get("is_present"), 0)
        is_active = safe_int(row.get("is_active"), 0)

        if host == source_host and live_type == source_type and is_present >= 1:
            source_users.add(uid)

        if is_present >= 1:
            user_host_present_sessions[(uid, host)].add(sk)
        if is_active >= 1:
            user_host_active_sessions[(uid, host)].add(sk)

    source_total = len(source_users)
    if source_total == 0:
        raise ValueError(f"source 用户数为0：{source_live}")

    rows = []
    for host in hosts:
        ge1 = ge2 = ge3 = active_ge1 = 0
        for uid in source_users:
            present_n = len(user_host_present_sessions.get((uid, host), set()))
            active_n = len(user_host_active_sessions.get((uid, host), set()))
            if present_n >= 1:
                ge1 += 1
            if present_n >= 2:
                ge2 += 1
            if present_n >= 3:
                ge3 += 1
            if active_n >= 1:
                active_ge1 += 1

        rows.append({
            "host": host,
            "source_user_count": source_total,
            "ge1_count": ge1,
            "ge1_rate": round(ge1 / source_total, 4),
            "ge2_count": ge2,
            "ge2_rate": round(ge2 / source_total, 4),
            "ge3_count": ge3,
            "ge3_rate": round(ge3 / source_total, 4),
            "active_ge1_count": active_ge1,
            "active_ge1_rate": round(active_ge1 / source_total, 4),
        })

    return rows


def write_summary_csv(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "host", "source_user_count",
        "ge1_count", "ge1_rate",
        "ge2_count", "ge2_rate",
        "ge3_count", "ge3_rate",
        "active_ge1_count", "active_ge1_rate",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def plot_grouped_bar(rows, out_png: Path, source_live: str):
    hosts = [r["host"] for r in rows]
    ge1 = [r["ge1_count"] for r in rows]
    ge2 = [r["ge2_count"] for r in rows]
    ge3 = [r["ge3_count"] for r in rows]
    active = [r["active_ge1_count"] for r in rows]
    source_total = rows[0]["source_user_count"] if rows else 0

    x = np.arange(len(hosts))
    w = 0.2

    fig, ax = plt.subplots(figsize=(10, 6))
    b1 = ax.bar(x - 1.5 * w, ge1, w, label="到场>=1", color="#5B8FF9")
    b2 = ax.bar(x - 0.5 * w, ge2, w, label="到场>=2", color="#61DDAA")
    b3 = ax.bar(x + 0.5 * w, ge3, w, label="到场>=3", color="#65789B")
    b4 = ax.bar(x + 1.5 * w, active, w, label="有效到场>=1", color="#F6BD16")

    ax.set_xticks(x)
    ax.set_xticklabels(hosts, fontsize=11)
    ax.set_ylabel("人数")
    ax.set_title(f"{source_live} 人群 -> 三人多阈值沉淀人数（去黑名单）\nsource_user_count={source_total}", fontsize=13)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend()

    def add_labels(bars):
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h, f"{int(h)}", ha="center", va="bottom", fontsize=9)

    add_labels(b1)
    add_labels(b2)
    add_labels(b3)
    add_labels(b4)

    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=180, bbox_inches="tight")
    plt.close()


def main():
    detail_rows = load_csv_rows(DETAIL_CSV)
    blacklist = load_blacklist(BLACKLIST_CSV)

    clean_rows = [r for r in detail_rows if safe_str(r.get("uid")) not in blacklist]

    rows = build_stats(clean_rows, SOURCE_LIVE, HOSTS)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUT_DIR / "fans_conversion_三人多阈值.csv"
    out_png = OUT_DIR / "fans_conversion_三人多阈值_柱状图.png"

    write_summary_csv(rows, out_csv)
    plot_grouped_bar(rows, out_png, SOURCE_LIVE)

    print(f"[OK] summary csv: {out_csv}")
    print(f"[OK] plot png   : {out_png}")
    print(f"[OK] blacklist : {len(blacklist)}")
    print(f"[OK] detail rows after clean: {len(clean_rows)}")


if __name__ == "__main__":
    main()
