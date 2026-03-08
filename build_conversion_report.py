#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
BLACKLIST_CSV = ROOT / "黑名单" / "uid_blacklist.csv"
SUPPLEMENT_SOURCE_CSV = ROOT / "KG补充名单" / "uid_live_stats.csv"
LEGACY_OUTPUT_CSV = ROOT / "分析结果" / "conversion_report_乃琳鸣潮.csv"

SOURCE_LIVE_FIXED = "乃琳_鸣潮"
WINDOW_DAYS = (7, 14, 30)
HOST_ORDER = ["乃琳", "嘉然", "贝拉"]
ANALYSIS_DIR = ROOT / "分析结果" / f"{SOURCE_LIVE_FIXED}_后续承接分析"


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


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        return float(text)
    except Exception:
        return default


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_ts_ms(value, default=0):
    return safe_int(value, default)


def fmt_pct(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def fmt_date(ts_ms):
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")


def load_detail_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def recalc_present_active(row):
    danmu_count = safe_int(row.get("danmu_count"), 0)
    gift_count = safe_int(row.get("gift_count"), 0)
    gift_amount = safe_float(row.get("gift_amount"), 0.0)
    is_present = 1 if (danmu_count > 0 or gift_count > 0 or gift_amount > 0) else safe_int(row.get("is_present"), 0)
    is_active = 1 if (danmu_count >= 2 or gift_amount > 0) else 0
    return is_present, is_active


def load_source_supplement_rows(csv_path: Path, source_live: str):
    if not csv_path.exists():
        return []
    source_host, source_type = source_live.split("_", 1)
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            host = safe_str(row.get("host"))
            live_type = safe_str(row.get("live_type"))
            if host != source_host or live_type != source_type:
                continue
            cloned = dict(row)
            is_present, is_active = recalc_present_active(cloned)
            cloned["is_present"] = str(is_present)
            cloned["is_active"] = str(is_active)
            rows.append(cloned)
    return rows


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


def is_valid_uid(uid):
    uid = safe_str(uid)
    return bool(uid) and uid.isdigit() and int(uid) > 0


def make_session_key(row):
    return (
        safe_str(row.get("live_id"))
        or safe_str(row.get("live_name"))
        or safe_str(row.get("start_date"))
    )


def build_records(detail_rows, blacklist_uids):
    dedup = {}
    invalid_uid_count = 0
    blacklisted_count = 0

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        if uid in blacklist_uids:
            blacklisted_count += 1
            continue
        if not is_valid_uid(uid):
            invalid_uid_count += 1
            continue

        host = safe_str(row.get("host")) or "未知"
        live_type = safe_str(row.get("live_type")) or "其他"
        target_live = f"{host}_{live_type}"
        session_key = make_session_key(row)
        if not session_key:
            continue

        session_ts = parse_ts_ms(row.get("start_date"), 0) or parse_ts_ms(row.get("first_send_date"), 0)
        first_send_ts = parse_ts_ms(row.get("first_send_date"), 0) or session_ts
        last_send_ts = parse_ts_ms(row.get("last_send_date"), 0) or session_ts

        record = {
            "uid": uid,
            "uname": safe_str(row.get("uname")),
            "host": host,
            "live_type": live_type,
            "target_live": target_live,
            "session_key": session_key,
            "live_name": safe_str(row.get("live_name")) or session_key,
            "session_ts": session_ts,
            "first_send_ts": first_send_ts,
            "last_send_ts": last_send_ts,
            "is_present": safe_int(row.get("is_present"), 0),
            "is_active": safe_int(row.get("is_active"), 0),
            "danmu_count": safe_int(row.get("danmu_count"), 0),
            "gift_count": safe_int(row.get("gift_count"), 0),
            "gift_amount": round(safe_float(row.get("gift_amount"), 0.0), 4),
        }

        dedup_key = (uid, target_live, session_key)
        existing = dedup.get(dedup_key)
        if existing is None:
            dedup[dedup_key] = record
            continue

        existing["session_ts"] = min(existing["session_ts"], record["session_ts"]) if existing["session_ts"] and record["session_ts"] else max(existing["session_ts"], record["session_ts"])
        existing["first_send_ts"] = min(existing["first_send_ts"], record["first_send_ts"]) if existing["first_send_ts"] and record["first_send_ts"] else max(existing["first_send_ts"], record["first_send_ts"])
        existing["last_send_ts"] = max(existing["last_send_ts"], record["last_send_ts"])
        existing["is_present"] = max(existing["is_present"], record["is_present"])
        existing["is_active"] = max(existing["is_active"], record["is_active"])
        existing["danmu_count"] += record["danmu_count"]
        existing["gift_count"] += record["gift_count"]
        existing["gift_amount"] = round(existing["gift_amount"] + record["gift_amount"], 4)
        if not existing["uname"] and record["uname"]:
            existing["uname"] = record["uname"]

    records = sorted(
        dedup.values(),
        key=lambda x: (x["uid"], x["session_ts"], x["target_live"], x["session_key"]),
    )
    return records, invalid_uid_count, blacklisted_count


def pick_earlier_record(old_record, new_record):
    if old_record is None:
        return new_record
    old_key = (old_record["session_ts"], old_record["target_live"], old_record["session_key"])
    new_key = (new_record["session_ts"], new_record["target_live"], new_record["session_key"])
    return new_record if new_key < old_key else old_record


def classify_host_segment(hosts):
    ordered = tuple(host for host in HOST_ORDER if host in hosts)
    mapping = {
        tuple(): "无后续到场",
        ("乃琳",): "仅留在乃琳",
        ("嘉然",): "只去嘉然",
        ("贝拉",): "只去贝拉",
        ("乃琳", "嘉然"): "乃琳+嘉然",
        ("乃琳", "贝拉"): "乃琳+贝拉",
        ("嘉然", "贝拉"): "嘉然+贝拉",
        ("乃琳", "嘉然", "贝拉"): "三人都看",
    }
    return mapping.get(ordered, "+".join(ordered) if ordered else "无后续到场")


def analyze_records(records, source_live, window_days):
    source_host, _ = source_live.split("_", 1)
    window_ms_map = {day: day * 24 * 3600 * 1000 for day in window_days}

    target_sessions = defaultdict(dict)
    records_by_uid = defaultdict(list)
    first_seen_by_uid = {}
    first_active_by_uid = {}
    all_targets = set()

    for record in records:
        if record["is_present"] < 1:
            continue

        uid = record["uid"]
        all_targets.add(record["target_live"])
        records_by_uid[uid].append(record)

        target_meta = target_sessions[record["target_live"]].get(record["session_key"])
        if target_meta is None or (record["session_ts"], record["live_name"]) < (target_meta["session_ts"], target_meta["live_name"]):
            target_sessions[record["target_live"]][record["session_key"]] = {
                "session_key": record["session_key"],
                "live_name": record["live_name"],
                "session_ts": record["session_ts"],
            }

        first_seen_by_uid[uid] = pick_earlier_record(first_seen_by_uid.get(uid), record)
        if record["is_active"] >= 1:
            first_active_by_uid[uid] = pick_earlier_record(first_active_by_uid.get(uid), record)

    source_users = set()
    source_first_by_uid = {}
    source_records_by_uid = defaultdict(list)

    for uid, user_records in records_by_uid.items():
        for record in user_records:
            if record["target_live"] == source_live:
                source_users.add(uid)
                source_records_by_uid[uid].append(record)
                old = source_first_by_uid.get(uid)
                if old is None or (record["session_ts"], record["session_key"]) < (old["session_ts"], old["session_key"]):
                    source_first_by_uid[uid] = record

    source_total = len(source_users)
    if source_total == 0:
        raise ValueError(f"source 用户数为0或不存在: {source_live}")

    targets = sorted(t for t in all_targets if t != source_live)
    summary_counter = {
        target: Counter({
            "overlap_ge1": 0,
            "overlap_ge2": 0,
            "overlap_ge3": 0,
            "overlap_active_ge1": 0,
            "pre_source_overlap": 0,
            "pre_source_only": 0,
            "post_ge1": 0,
            "post_ge2": 0,
            "post_ge3": 0,
            "post_active_ge1": 0,
            "first_target": 0,
            **{f"post_d{day}_ge1": 0 for day in window_days},
            **{f"post_d{day}_active_ge1": 0 for day in window_days},
        })
        for target in targets
    }

    host_segment_counter = Counter()
    first_target_counter = Counter()
    source_profile_counter = Counter()
    cohort_overview = {}
    cohort_target_counter = defaultdict(Counter)

    analysis_start_ts = 0
    analysis_end_ts = 0
    for target in target_sessions.values():
        for meta in target.values():
            ts = meta["session_ts"]
            if not ts:
                continue
            analysis_start_ts = ts if analysis_start_ts == 0 else min(analysis_start_ts, ts)
            analysis_end_ts = max(analysis_end_ts, ts)

    for uid in source_users:
        user_records = sorted(
            records_by_uid[uid],
            key=lambda x: (x["session_ts"], x["target_live"], x["session_key"]),
        )
        source_first = source_first_by_uid[uid]
        source_first_ts = source_first["session_ts"]
        source_first_name = source_first["live_name"]
        first_seen = first_seen_by_uid.get(uid)

        source_present_sessions = [r for r in user_records if r["target_live"] == source_live]
        source_active_sessions = [r for r in source_present_sessions if r["is_active"] >= 1]

        if first_seen and first_seen["target_live"] == source_live:
            source_profile_counter["window_new_user"] += 1
        else:
            source_profile_counter["window_old_user"] += 1
        if source_active_sessions:
            source_profile_counter["source_active_user"] += 1
        if len(source_present_sessions) >= 2:
            source_profile_counter["source_ge2_user"] += 1
        if len(source_present_sessions) >= 3:
            source_profile_counter["source_ge3_user"] += 1

        per_target_records = defaultdict(list)
        post_non_source_records = []

        for record in user_records:
            if record["target_live"] == source_live:
                continue
            per_target_records[record["target_live"]].append(record)
            if record["session_ts"] > source_first_ts:
                post_non_source_records.append(record)

        first_post_record = min(
            post_non_source_records,
            key=lambda x: (x["session_ts"], x["target_live"], x["session_key"]),
            default=None,
        )
        if first_post_record:
            first_target_counter[first_post_record["target_live"]] += 1
            summary_counter[first_post_record["target_live"]]["first_target"] += 1
            source_profile_counter["post_any_non_source_user"] += 1
        else:
            first_target_counter["无后续非source目标"] += 1

        post_hosts = {record["host"] for record in post_non_source_records}
        if any(record["host"] == source_host for record in post_non_source_records):
            source_profile_counter["post_same_host_other_type_user"] += 1
        if any(record["host"] != source_host for record in post_non_source_records):
            source_profile_counter["post_cross_host_user"] += 1
        if any(record["host"] == "嘉然" for record in post_non_source_records):
            source_profile_counter["post_jiaran_user"] += 1
        if any(record["host"] == "贝拉" for record in post_non_source_records):
            source_profile_counter["post_bella_user"] += 1
        host_segment_counter[classify_host_segment(post_hosts)] += 1

        cohort_key = source_first["session_key"]
        cohort_label = source_first_name
        cohort_info = cohort_overview.setdefault(
            cohort_key,
            {
                "source_live": source_live,
                "source_session_key": cohort_key,
                "source_session_name": cohort_label,
                "source_session_start": source_first_ts,
                "source_session_start_text": fmt_date(source_first_ts),
                "cohort_user_count": 0,
                "window_new_user_count": 0,
                "source_active_user_count": 0,
            },
        )
        cohort_info["cohort_user_count"] += 1
        if first_seen and first_seen["target_live"] == source_live:
            cohort_info["window_new_user_count"] += 1
        if source_active_sessions:
            cohort_info["source_active_user_count"] += 1

        for target in targets:
            target_records = per_target_records.get(target, [])
            overlap_records = target_records
            pre_records = [r for r in target_records if r["session_ts"] < source_first_ts]
            post_records = [r for r in target_records if r["session_ts"] > source_first_ts]
            overlap_active_records = [r for r in overlap_records if r["is_active"] >= 1]
            post_active_records = [r for r in post_records if r["is_active"] >= 1]

            if overlap_records:
                summary_counter[target]["overlap_ge1"] += 1
            if len(overlap_records) >= 2:
                summary_counter[target]["overlap_ge2"] += 1
            if len(overlap_records) >= 3:
                summary_counter[target]["overlap_ge3"] += 1
            if overlap_active_records:
                summary_counter[target]["overlap_active_ge1"] += 1
            if pre_records:
                summary_counter[target]["pre_source_overlap"] += 1
            if pre_records and not post_records:
                summary_counter[target]["pre_source_only"] += 1
            if post_records:
                summary_counter[target]["post_ge1"] += 1
            if len(post_records) >= 2:
                summary_counter[target]["post_ge2"] += 1
            if len(post_records) >= 3:
                summary_counter[target]["post_ge3"] += 1
            if post_active_records:
                summary_counter[target]["post_active_ge1"] += 1

            for day, day_ms in window_ms_map.items():
                window_records = [
                    r for r in post_records
                    if r["session_ts"] <= source_first_ts + day_ms
                ]
                window_active_records = [r for r in window_records if r["is_active"] >= 1]
                if window_records:
                    summary_counter[target][f"post_d{day}_ge1"] += 1
                if window_active_records:
                    summary_counter[target][f"post_d{day}_active_ge1"] += 1

            cohort_stat = cohort_target_counter[(cohort_key, target)]
            if post_records:
                cohort_stat["post_ge1"] += 1
            if post_active_records:
                cohort_stat["post_active_ge1"] += 1
            for day, day_ms in window_ms_map.items():
                if any(r["session_ts"] <= source_first_ts + day_ms for r in post_records):
                    cohort_stat[f"post_d{day}_ge1"] += 1
            if first_post_record and first_post_record["target_live"] == target:
                cohort_stat["first_target"] += 1

    summary_rows = []
    for target in targets:
        host, live_type = target.split("_", 1)
        total_sessions = len(target_sessions[target])
        counter = summary_counter[target]
        row = {
            "source_live": source_live,
            "target_live": target,
            "target_host": host,
            "target_type": live_type,
            "target_total_sessions": total_sessions,
            "source_user_count": source_total,
            "overlap_ge1_user_count": counter["overlap_ge1"],
            "overlap_ge1_rate": fmt_pct(counter["overlap_ge1"], source_total),
            "overlap_ge2_user_count": counter["overlap_ge2"],
            "overlap_ge2_rate": fmt_pct(counter["overlap_ge2"], source_total),
            "overlap_ge3_user_count": counter["overlap_ge3"],
            "overlap_ge3_rate": fmt_pct(counter["overlap_ge3"], source_total),
            "overlap_active_ge1_user_count": counter["overlap_active_ge1"],
            "overlap_active_ge1_rate": fmt_pct(counter["overlap_active_ge1"], source_total),
            "pre_source_overlap_user_count": counter["pre_source_overlap"],
            "pre_source_overlap_rate": fmt_pct(counter["pre_source_overlap"], source_total),
            "pre_source_only_user_count": counter["pre_source_only"],
            "pre_source_only_rate": fmt_pct(counter["pre_source_only"], source_total),
            "post_ge1_user_count": counter["post_ge1"],
            "post_ge1_rate": fmt_pct(counter["post_ge1"], source_total),
            "post_ge2_user_count": counter["post_ge2"],
            "post_ge2_rate": fmt_pct(counter["post_ge2"], source_total),
            "post_ge3_user_count": counter["post_ge3"],
            "post_ge3_rate": fmt_pct(counter["post_ge3"], source_total),
            "post_active_ge1_user_count": counter["post_active_ge1"],
            "post_active_ge1_rate": fmt_pct(counter["post_active_ge1"], source_total),
            "first_target_user_count": counter["first_target"],
            "first_target_rate": fmt_pct(counter["first_target"], source_total),
            "post_ge1_per_target_session": round(counter["post_ge1"] / total_sessions, 2) if total_sessions else 0.0,
        }
        for day in window_days:
            row[f"post_d{day}_ge1_user_count"] = counter[f"post_d{day}_ge1"]
            row[f"post_d{day}_ge1_rate"] = fmt_pct(counter[f"post_d{day}_ge1"], source_total)
            row[f"post_d{day}_active_ge1_user_count"] = counter[f"post_d{day}_active_ge1"]
            row[f"post_d{day}_active_ge1_rate"] = fmt_pct(counter[f"post_d{day}_active_ge1"], source_total)
        summary_rows.append(row)

    summary_rows.sort(
        key=lambda x: (
            -x["post_ge1_rate"],
            -x["first_target_rate"],
            x["target_live"],
        )
    )

    first_target_rows = []
    for target_name, count in first_target_counter.items():
        if target_name == "无后续非source目标":
            host = "无"
            live_type = "无"
        else:
            host, live_type = target_name.split("_", 1)
        first_target_rows.append({
            "source_live": source_live,
            "first_target": target_name,
            "first_target_host": host,
            "first_target_type": live_type,
            "source_user_count": source_total,
            "user_count": count,
            "user_rate": fmt_pct(count, source_total),
        })
    first_target_rows.sort(key=lambda x: (-x["user_count"], x["first_target"]))

    host_segment_rows = []
    segment_order = [
        "无后续到场", "仅留在乃琳", "只去嘉然", "只去贝拉",
        "乃琳+嘉然", "乃琳+贝拉", "嘉然+贝拉", "三人都看",
    ]
    for segment in segment_order:
        count = host_segment_counter.get(segment, 0)
        host_segment_rows.append({
            "source_live": source_live,
            "segment": segment,
            "source_user_count": source_total,
            "user_count": count,
            "user_rate": fmt_pct(count, source_total),
        })

    cohort_overview_rows = sorted(cohort_overview.values(), key=lambda x: (x["source_session_start"], x["source_session_name"]))
    for row in cohort_overview_rows:
        row["window_new_user_rate"] = fmt_pct(row["window_new_user_count"], row["cohort_user_count"])
        row["source_active_user_rate"] = fmt_pct(row["source_active_user_count"], row["cohort_user_count"])

    cohort_target_rows = []
    for cohort_row in cohort_overview_rows:
        cohort_key = cohort_row["source_session_key"]
        cohort_size = cohort_row["cohort_user_count"]
        for target in targets:
            host, live_type = target.split("_", 1)
            counter = cohort_target_counter.get((cohort_key, target), Counter())
            row = {
                "source_live": source_live,
                "source_session_key": cohort_key,
                "source_session_name": cohort_row["source_session_name"],
                "source_session_start": cohort_row["source_session_start"],
                "source_session_start_text": cohort_row["source_session_start_text"],
                "cohort_user_count": cohort_size,
                "target_live": target,
                "target_host": host,
                "target_type": live_type,
                "post_ge1_user_count": counter.get("post_ge1", 0),
                "post_ge1_rate": fmt_pct(counter.get("post_ge1", 0), cohort_size),
                "post_active_ge1_user_count": counter.get("post_active_ge1", 0),
                "post_active_ge1_rate": fmt_pct(counter.get("post_active_ge1", 0), cohort_size),
                "first_target_user_count": counter.get("first_target", 0),
                "first_target_rate": fmt_pct(counter.get("first_target", 0), cohort_size),
            }
            for day in window_days:
                key = f"post_d{day}_ge1"
                row[f"post_d{day}_ge1_user_count"] = counter.get(key, 0)
                row[f"post_d{day}_ge1_rate"] = fmt_pct(counter.get(key, 0), cohort_size)
            cohort_target_rows.append(row)

    cohort_target_rows.sort(key=lambda x: (x["source_session_start"], -x["post_ge1_rate"], x["target_live"]))

    source_profile_rows = []
    profile_items = [
        ("source_user_count", source_total, "source 人群规模"),
        ("window_new_user_count", source_profile_counter["window_new_user"], "当前窗口内首次观测即出现在 source 的用户数"),
        ("window_new_user_rate", fmt_pct(source_profile_counter["window_new_user"], source_total), "当前窗口内首次观测即出现在 source 的占比"),
        ("window_old_user_count", source_profile_counter["window_old_user"], "当前窗口内在 source 之前已在别处观测到的用户数"),
        ("window_old_user_rate", fmt_pct(source_profile_counter["window_old_user"], source_total), "当前窗口内在 source 前已被观测到的占比"),
        ("source_active_user_count", source_profile_counter["source_active_user"], "在 source 至少有 1 场有效到场的用户数"),
        ("source_active_user_rate", fmt_pct(source_profile_counter["source_active_user"], source_total), "在 source 至少有 1 场有效到场的占比"),
        ("source_ge2_user_count", source_profile_counter["source_ge2_user"], "在 source 到场不少于 2 场的用户数"),
        ("source_ge2_user_rate", fmt_pct(source_profile_counter["source_ge2_user"], source_total), "在 source 到场不少于 2 场的占比"),
        ("source_ge3_user_count", source_profile_counter["source_ge3_user"], "在 source 到场不少于 3 场的用户数"),
        ("source_ge3_user_rate", fmt_pct(source_profile_counter["source_ge3_user"], source_total), "在 source 到场不少于 3 场的占比"),
        ("post_any_non_source_user_count", source_profile_counter["post_any_non_source_user"], "首次 source 之后至少去过 1 个非 source 目标的用户数"),
        ("post_any_non_source_user_rate", fmt_pct(source_profile_counter["post_any_non_source_user"], source_total), "首次 source 之后至少去过 1 个非 source 目标的占比"),
        ("post_same_host_other_type_user_count", source_profile_counter["post_same_host_other_type_user"], "首次 source 之后去过乃琳其他类型直播的用户数"),
        ("post_same_host_other_type_user_rate", fmt_pct(source_profile_counter["post_same_host_other_type_user"], source_total), "首次 source 之后去过乃琳其他类型直播的占比"),
        ("post_cross_host_user_count", source_profile_counter["post_cross_host_user"], "首次 source 之后去过其他成员直播的用户数"),
        ("post_cross_host_user_rate", fmt_pct(source_profile_counter["post_cross_host_user"], source_total), "首次 source 之后去过其他成员直播的占比"),
        ("post_jiaran_user_count", source_profile_counter["post_jiaran_user"], "首次 source 之后去过嘉然直播的用户数"),
        ("post_jiaran_user_rate", fmt_pct(source_profile_counter["post_jiaran_user"], source_total), "首次 source 之后去过嘉然直播的占比"),
        ("post_bella_user_count", source_profile_counter["post_bella_user"], "首次 source 之后去过贝拉直播的用户数"),
        ("post_bella_user_rate", fmt_pct(source_profile_counter["post_bella_user"], source_total), "首次 source 之后去过贝拉直播的占比"),
        ("analysis_start", fmt_date(analysis_start_ts), "当前分析窗口起点"),
        ("analysis_end", fmt_date(analysis_end_ts), "当前分析窗口终点"),
    ]
    for metric, value, desc in profile_items:
        source_profile_rows.append({
            "source_live": source_live,
            "metric": metric,
            "value": value,
            "description": desc,
        })

    return {
        "summary_rows": summary_rows,
        "first_target_rows": first_target_rows,
        "host_segment_rows": host_segment_rows,
        "cohort_overview_rows": cohort_overview_rows,
        "cohort_target_rows": cohort_target_rows,
        "source_profile_rows": source_profile_rows,
        "source_user_count": source_total,
        "analysis_start_ts": analysis_start_ts,
        "analysis_end_ts": analysis_end_ts,
    }


def write_csv(rows, output_csv_path: Path, fieldnames=None):
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None and rows:
        fieldnames = list(rows[0].keys())
    if fieldnames is None:
        return
    with output_csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)




def main():
    if not DETAIL_CSV.exists():
        raise FileNotFoundError(f"找不到场次明细: {DETAIL_CSV}")

    detail_rows = load_detail_rows(DETAIL_CSV)
    supplement_rows = load_source_supplement_rows(SUPPLEMENT_SOURCE_CSV, SOURCE_LIVE_FIXED)
    all_rows = detail_rows + supplement_rows
    blacklist_uids = load_blacklist_uids(BLACKLIST_CSV)
    records, invalid_uid_count, blacklisted_count = build_records(all_rows, blacklist_uids)
    analysis_result = analyze_records(records, SOURCE_LIVE_FIXED, WINDOW_DAYS)

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    cohort_overview_csv = ANALYSIS_DIR / "00_source_cohorts.csv"
    summary_csv = ANALYSIS_DIR / "01_summary_by_target.csv"
    first_target_csv = ANALYSIS_DIR / "02_first_target.csv"
    cohort_target_csv = ANALYSIS_DIR / "03_cohort_by_target.csv"
    host_segment_csv = ANALYSIS_DIR / "04_host_flow_segments.csv"
    profile_csv = ANALYSIS_DIR / "05_source_profile.csv"

    write_csv(analysis_result["cohort_overview_rows"], cohort_overview_csv)
    write_csv(analysis_result["summary_rows"], summary_csv)
    write_csv(analysis_result["summary_rows"], LEGACY_OUTPUT_CSV)
    write_csv(analysis_result["first_target_rows"], first_target_csv)
    write_csv(analysis_result["cohort_target_rows"], cohort_target_csv)
    write_csv(analysis_result["host_segment_rows"], host_segment_csv)
    write_csv(analysis_result["source_profile_rows"], profile_csv)

    print(f"[OK] source 固定: {SOURCE_LIVE_FIXED}")
    print(f"[OK] 原始明细行数: {len(detail_rows)}")
    print(f"[OK] 补充source行数: {len(supplement_rows)}")
    print(f"[OK] 黑名单过滤行数: {blacklisted_count}")
    print(f"[OK] 非法UID过滤行数: {invalid_uid_count}")
    print(f"[OK] 清洗后 user-session 行数: {len(records)}")
    print(f"[OK] source 用户数: {analysis_result['source_user_count']}")
    print(f"[OK] 输出目录: {ANALYSIS_DIR}")
    print(f"[OK] 兼容旧报表: {LEGACY_OUTPUT_CSV}")


if __name__ == "__main__":
    main()
