#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
BLACKLIST_CSV = ROOT / "黑名单" / "uid_blacklist.csv"
OUTPUT_CSV = ROOT / "分析结果" / "conversion_report_乃琳鸣潮.csv"

SOURCE_LIVE_FIXED = "乃琳_鸣潮"


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        return int(float(text))
    except Exception:
        return default


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def load_detail_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_blacklist_uids(csv_path: Path):
    if not csv_path.exists():
        return set()
    uids = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = safe_str(row.get("uid"))
            if uid:
                uids.add(uid)
    return uids


def build_conversion_rows_from_detail(detail_rows, source_live_fixed):
    # 1) 目标集合
    targets = set()
    # 2) source 用户集合（到场>=1）
    source_users = set()
    # 3) 用户-目标 到场场次数（去重到场次）
    user_target_attend_sessions = defaultdict(set)
    # 4) 用户-目标 有效场次数（去重到场次）
    user_target_active_sessions = defaultdict(set)
    # 5) 目标总场次数（去重到场次）
    target_sessions = defaultdict(set)

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        host = safe_str(row.get("host"))
        live_type = safe_str(row.get("live_type"))
        if not uid or not host or not live_type:
            continue

        target_name = f"{host}_{live_type}"
        targets.add(target_name)

        live_id = safe_str(row.get("live_id"))
        live_name = safe_str(row.get("live_name"))
        start_date = safe_str(row.get("start_date"))
        session_key = live_id or live_name or start_date
        if not session_key:
            continue

        is_present = safe_int(row.get("is_present"), 0)
        is_active = safe_int(row.get("is_active"), 0)

        target_sessions[target_name].add(session_key)

        if is_present >= 1:
            user_target_attend_sessions[(uid, target_name)].add(session_key)

        if is_active >= 1:
            user_target_active_sessions[(uid, target_name)].add(session_key)

        if target_name == source_live_fixed and is_present >= 1:
            source_users.add(uid)

    source_total = len(source_users)
    if source_total == 0:
        raise ValueError(f"source 用户数为0或不存在: {source_live_fixed}")

    targets = sorted(targets)
    rows = []

    for target_name in targets:
        if target_name == source_live_fixed:
            continue

        ge1 = ge2 = ge3 = active_ge1 = 0

        for uid in source_users:
            attend_count = len(user_target_attend_sessions.get((uid, target_name), set()))
            active_count = len(user_target_active_sessions.get((uid, target_name), set()))

            if attend_count >= 1:
                ge1 += 1
            if attend_count >= 2:
                ge2 += 1
            if attend_count >= 3:
                ge3 += 1
            if active_count >= 1:
                active_ge1 += 1

        rows.append({
            "source_live": source_live_fixed,
            "target_live": target_name,
            "target_total_sessions": len(target_sessions.get(target_name, set())),
            "source_user_count": source_total,
            "target_ge1_user_count": ge1,
            "target_ge1_rate": round(ge1 / source_total, 4),
            "target_ge2_user_count": ge2,
            "target_ge2_rate": round(ge2 / source_total, 4),
            "target_ge3_user_count": ge3,
            "target_ge3_rate": round(ge3 / source_total, 4),
            "target_active_ge1_user_count": active_ge1,
            "target_active_ge1_rate": round(active_ge1 / source_total, 4),
        })

    rows.sort(key=lambda x: (-x["target_ge1_rate"], -x["target_active_ge1_rate"], x["target_live"]))
    return rows


def write_csv(rows, output_csv_path: Path):
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_live", "target_live", "target_total_sessions", "source_user_count",
        "target_ge1_user_count", "target_ge1_rate",
        "target_ge2_user_count", "target_ge2_rate",
        "target_ge3_user_count", "target_ge3_rate",
        "target_active_ge1_user_count", "target_active_ge1_rate",
    ]
    with output_csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    if not DETAIL_CSV.exists():
        raise FileNotFoundError(f"找不到场次明细: {DETAIL_CSV}")

    detail_rows = load_detail_rows(DETAIL_CSV)
    blacklist_uids = load_blacklist_uids(BLACKLIST_CSV)

    # 去老粉（黑名单）
    detail_rows = [r for r in detail_rows if safe_str(r.get("uid")) not in blacklist_uids]

    rows = build_conversion_rows_from_detail(detail_rows, SOURCE_LIVE_FIXED)
    write_csv(rows, OUTPUT_CSV)

    print(f"已生成: {OUTPUT_CSV}")
    print(f"source 固定: {SOURCE_LIVE_FIXED}")
    print(f"黑名单UID数: {len(blacklist_uids)}")
    print(f"明细行数(过滤后): {len(detail_rows)}")
    print(f"输出行数: {len(rows)}")


if __name__ == "__main__":
    main()
