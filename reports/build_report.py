from __future__ import annotations

import argparse
import os
import shutil
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from analysis_settings import RETURN_START_MS, SOURCE_LIVE_FIXED
from user_tagging import build_user_tag_index

from reports.core import (
    build_records,
    build_single_pass_exclusion_uids,
    fmt_date,
    fmt_pct,
    load_blacklist_uids,
    load_detail_rows,
    safe_float,
    safe_int,
    safe_str,
    write_csv,
)


ROOT = Path(__file__).resolve().parent.parent
OUT_ROOT = ROOT / "分析结果" / "核心报告"
LEGACY_ROOT = ROOT / "分析结果" / "legacy"
BIRTHDAY_XML = ROOT / "生日会" / "20260314-194426-嘉然今天吃什么-嘉然生日会以爱之名843.xml"
MPL_DIR = ROOT / ".mplconfig"
MPL_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


plt.rcParams["font.sans-serif"] = ["PingFang SC", "Microsoft YaHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 150


CORE_THRESHOLDS = [3, 5, 7]
PRIMARY_CORE_THRESHOLD = 5
BIRTHDAY_CATEGORY_ORDER = ["黑名单长期观众", "510回旋", "广义回旋", "kg"]
BIRTHDAY_COLORS = {
    "黑名单长期观众": "#6C757D",
    "510回旋": "#E76F51",
    "广义回旋": "#4C78A8",
    "kg": "#2A9D8F",
}


def unique_sessions(records, host=None, target_live=None, min_ts=None):
    dedup = {}
    for record in records:
        if host is not None and record["host"] != host:
            continue
        if target_live is not None and record["target_live"] != target_live:
            continue
        if min_ts is not None and record["session_ts"] < min_ts:
            continue
        key = record["session_key"]
        old = dedup.get(key)
        if old is None:
            dedup[key] = dict(record)
            continue
        old["is_present"] = max(old["is_present"], record["is_present"])
        old["is_active"] = max(old["is_active"], record["is_active"])
        old["session_ts"] = min(old["session_ts"], record["session_ts"])
    return sorted(dedup.values(), key=lambda x: (x["session_ts"], x["session_key"]))


def maybe_archive_legacy_outputs():
    targets = [
        ROOT / "分析结果" / "乃琳_鸣潮_后续承接分析",
        ROOT / "分析结果" / "乃琳_鸣潮_后续承接分析_去0211单次且无后续",
        ROOT / "分析结果" / "conversion_report_乃琳鸣潮.csv",
        ROOT / "分析结果" / "fans_conversion_三人多阈值.csv",
        ROOT / "分析结果" / "fans_conversion_三人解释性阈值.svg",
        ROOT / "分析结果" / "fans_conversion_三人解释性阈值_摘要.csv",
        ROOT / "分析结果" / "fans_conversion_三人解释性阈值_非零长表.csv",
    ]
    LEGACY_ROOT.mkdir(parents=True, exist_ok=True)
    for target in targets:
        if not target.exists():
            continue
        destination = LEGACY_ROOT / target.name
        if destination.exists():
            continue
        target.rename(destination)


def safe_float_pct(value):
    try:
        return f"{float(value):.1%}"
    except Exception:
        return "0.0%"


def build_user_universe(records, source_live):
    tags = build_user_tag_index(records, source_live)
    records_by_uid = defaultdict(list)
    for record in records:
        if record["is_present"] >= 1:
            records_by_uid[record["uid"]].append(record)

    universe = {}
    source_users = []
    pure_new_users = []
    for uid, user_records in records_by_uid.items():
        ordered = sorted(user_records, key=lambda x: (x["session_ts"], x["target_live"], x["session_key"]))
        first_record = ordered[0]
        tags_row = tags.get(uid, {})
        is_window_new = int(first_record["session_ts"] >= RETURN_START_MS)
        is_source_pure_new = int(is_window_new == 1 and first_record["target_live"] == source_live)
        is_510 = safe_int(tags_row.get("is_510_return_user"), 0)
        is_broad = safe_int(tags_row.get("is_broad_return_user"), 0)
        is_broad_exclusive = int(is_broad == 1 and is_510 == 0)

        source_records = [record for record in ordered if record["target_live"] == source_live]
        source_first = None
        source_present_count = 0
        source_active_count = 0
        as_present_count = 0
        as_active_count = 0
        if source_records:
            source_records.sort(key=lambda x: (x["session_ts"], x["session_key"]))
            source_first = source_records[0]
            source_sessions = unique_sessions(source_records)
            as_sessions = unique_sessions(ordered, min_ts=source_first["session_ts"])
            source_present_count = sum(record["is_present"] >= 1 for record in source_sessions)
            source_active_count = sum(record["is_active"] >= 1 for record in source_sessions)
            as_present_count = sum(record["is_present"] >= 1 for record in as_sessions)
            as_active_count = sum(record["is_active"] >= 1 for record in as_sessions)

        row = {
            "uid": uid,
            "uname": first_record["uname"],
            "_ordered_records": ordered,
            "first_seen_ts": first_record["session_ts"],
            "first_seen_text": fmt_date(first_record["session_ts"]),
            "first_seen_live": first_record["target_live"],
            "last_pre_return_ts": safe_int(tags_row.get("last_pre_return_ts"), 0),
            "last_pre_return_text": fmt_date(safe_int(tags_row.get("last_pre_return_ts"), 0)),
            "last_pre_return_live": safe_str(tags_row.get("last_pre_return_target_live")),
            "first_post_return_ts": safe_int(tags_row.get("first_post_return_ts"), 0),
            "first_post_return_text": fmt_date(safe_int(tags_row.get("first_post_return_ts"), 0)),
            "first_post_return_live": safe_str(tags_row.get("first_post_return_target_live")),
            "is_window_new": is_window_new,
            "is_source_pure_new": is_source_pure_new,
            "is_510_return": is_510,
            "is_broad_return_exclusive": is_broad_exclusive,
            "source_first_ts": source_first["session_ts"] if source_first else 0,
            "source_first_text": fmt_date(source_first["session_ts"]) if source_first else "",
            "source_first_key": source_first["session_key"] if source_first else "",
            "source_first_name": source_first["live_name"] if source_first else "",
            "source_present_count": source_present_count,
            "source_active_count": source_active_count,
            "as_present_count_after_source": as_present_count,
            "as_active_count_after_source": as_active_count,
        }
        for threshold in CORE_THRESHOLDS:
            row[f"is_core_ge{threshold}"] = int(is_source_pure_new == 1 and as_active_count >= threshold)

        universe[uid] = row
        if source_first:
            source_users.append(row)
        if is_source_pure_new == 1:
            pure_new_users.append(row)

    source_users.sort(key=lambda x: (x["source_first_ts"], x["uid"]))
    pure_new_users.sort(key=lambda x: (x["source_first_ts"], x["uid"]))
    return universe, source_users, pure_new_users


def build_definition_rows(source_users, pure_new_users):
    touched_total = len(source_users)
    kg_total = len(pure_new_users)
    core_primary_total = sum(user[f"is_core_ge{PRIMARY_CORE_THRESHOLD}"] == 1 for user in pure_new_users)
    rows = [
        {
            "item": "乃琳鸣潮触达用户",
            "definition": "至少进入过一次 乃琳_鸣潮 的用户",
            "user_count": touched_total,
            "user_rate_in_scope": 1.0,
            "user_rate_in_kg": "",
            "denominator_count": "",
            "numerator_count": "",
            "conversion_rate": "",
        },
        {
            "item": "kg",
            "definition": "2025-12-09 及之后首次被观测到，且首次互动直播就是 乃琳_鸣潮，此前没有任何看播记录",
            "user_count": kg_total,
            "user_rate_in_scope": fmt_pct(kg_total, touched_total),
            "user_rate_in_kg": "",
            "denominator_count": "",
            "numerator_count": "",
            "conversion_rate": "",
        },
        {
            "item": f"核心kg_ge{PRIMARY_CORE_THRESHOLD}",
            "definition": f"kg 用户在首次进入 乃琳_鸣潮 后，累计在 A-SOUL 任意直播有效到场>={PRIMARY_CORE_THRESHOLD}",
            "user_count": core_primary_total,
            "user_rate_in_scope": fmt_pct(core_primary_total, touched_total),
            "user_rate_in_kg": fmt_pct(core_primary_total, kg_total),
            "denominator_count": "",
            "numerator_count": "",
            "conversion_rate": "",
        },
        {
            "item": "510回旋",
            "definition": "2022-06-01 及以前出现过，2022-06-02~2025-12-08 完全沉默，2025-12-09 后重新出现",
            "user_count": sum(user["is_510_return"] == 1 for user in source_users),
            "user_rate_in_scope": fmt_pct(sum(user["is_510_return"] == 1 for user in source_users), touched_total),
            "user_rate_in_kg": "",
            "denominator_count": "",
            "numerator_count": "",
            "conversion_rate": "",
        },
        {
            "item": "广义回旋",
            "definition": "2025-12-09 前出现过，2025-12-09 后再次出现，且不属于 510回旋",
            "user_count": sum(user["is_broad_return_exclusive"] == 1 for user in source_users),
            "user_rate_in_scope": fmt_pct(sum(user["is_broad_return_exclusive"] == 1 for user in source_users), touched_total),
            "user_rate_in_kg": "",
            "denominator_count": "",
            "numerator_count": "",
            "conversion_rate": "",
        },
        {
            "item": "主承接口径",
            "definition": f"kg -> 首次进入 乃琳_鸣潮 后，在 A-SOUL 任意直播累计有效到场>={PRIMARY_CORE_THRESHOLD}",
            "user_count": "",
            "user_rate_in_scope": "",
            "user_rate_in_kg": "",
            "denominator_count": kg_total,
            "numerator_count": core_primary_total,
            "conversion_rate": fmt_pct(core_primary_total, kg_total),
        },
    ]
    return rows


def build_core_new_rows(pure_new_users):
    total = len(pure_new_users)
    rows = []
    for threshold in CORE_THRESHOLDS:
        count = sum(user[f"is_core_ge{threshold}"] == 1 for user in pure_new_users)
        rows.append({
            "threshold": threshold,
            "segment": f"核心kg_ge{threshold}",
            "definition": f"kg 用户在首次进入 乃琳_鸣潮 后，累计在 A-SOUL 任意直播有效到场>={threshold}",
            "user_count": count,
            "user_rate_in_kg": fmt_pct(count, total),
        })
    return rows


def build_return_rows(universe, source_users):
    rows = []
    scopes = [("全站", list(universe.values())), ("乃琳鸣潮触达内", source_users)]
    for scope_name, users in scopes:
        for segment, selector in [
            ("510回旋", lambda user: user["is_510_return"] == 1),
            ("广义回旋", lambda user: user["is_broad_return_exclusive"] == 1),
        ]:
            selected = [user for user in users if selector(user)]
            row = {
                "scope": scope_name,
                "segment": segment,
                "raw_user_count": len(selected),
                "raw_user_rate_in_scope": fmt_pct(len(selected), len(users)),
            }
            for threshold in CORE_THRESHOLDS:
                core_count = sum(user["as_active_count_after_source"] >= threshold for user in selected)
                row[f"core_ge{threshold}_count"] = core_count
                row[f"core_ge{threshold}_rate_in_segment"] = fmt_pct(core_count, len(selected))
            rows.append(row)
    return rows


def build_uid_list_rows(users):
    rows = []
    for user in users:
        row = {
            "uid": user["uid"],
            "uname": user["uname"],
            "first_seen_text": user["first_seen_text"],
            "first_seen_live": user["first_seen_live"],
            "last_pre_return_text": user["last_pre_return_text"],
            "last_pre_return_live": user["last_pre_return_live"],
            "first_post_return_text": user["first_post_return_text"],
            "first_post_return_live": user["first_post_return_live"],
            "source_first_text": user["source_first_text"],
            "source_present_count": user["source_present_count"],
            "source_active_count": user["source_active_count"],
            "as_present_count_after_source": user["as_present_count_after_source"],
            "as_active_count_after_source": user["as_active_count_after_source"],
            "is_source_pure_new": user["is_source_pure_new"],
            "is_510_return": user["is_510_return"],
            "is_broad_return_exclusive": user["is_broad_return_exclusive"],
        }
        for threshold in CORE_THRESHOLDS:
            row[f"is_core_ge{threshold}"] = user[f"is_core_ge{threshold}"]
        rows.append(row)
    return rows


def build_host_flow_rows(kg_users, universe):
    segments = [
        "无后续到场",
        "仅留在乃琳",
        "只去嘉然",
        "只去贝拉",
        "乃琳+嘉然",
        "乃琳+贝拉",
        "嘉然+贝拉",
        "三人都看",
    ]
    counts = {segment: 0 for segment in segments}

    for user in kg_users:
        uid = user["uid"]
        source_first_ts = safe_int(user.get("source_first_ts"), 0)
        user_row = universe.get(uid)
        if not user_row:
            continue

        # Reuse the stored per-user records indirectly by reading from the unified row set.
        # We only care whether the user later reached hosts among 乃琳 / 嘉然 / 贝拉.
        later_hosts = set()
        for record in user_row.get("_ordered_records", []):
            if safe_int(record.get("session_ts"), 0) <= source_first_ts:
                continue
            host = safe_str(record.get("host"))
            if host in {"乃琳", "嘉然", "贝拉"} and safe_int(record.get("is_present"), 0) >= 1:
                later_hosts.add(host)

        if not later_hosts:
            counts["无后续到场"] += 1
        elif later_hosts == {"乃琳"}:
            counts["仅留在乃琳"] += 1
        elif later_hosts == {"嘉然"}:
            counts["只去嘉然"] += 1
        elif later_hosts == {"贝拉"}:
            counts["只去贝拉"] += 1
        elif later_hosts == {"乃琳", "嘉然"}:
            counts["乃琳+嘉然"] += 1
        elif later_hosts == {"乃琳", "贝拉"}:
            counts["乃琳+贝拉"] += 1
        elif later_hosts == {"嘉然", "贝拉"}:
            counts["嘉然+贝拉"] += 1
        else:
            counts["三人都看"] += 1

    total = len(kg_users)
    rows = []
    for segment in segments:
        rows.append({
            "segment": segment,
            "kg_user_count": total,
            "user_count": counts[segment],
            "user_rate": fmt_pct(counts[segment], total),
        })
    return rows


def build_kg_trend_rows(kg_users):
    per_session = {}
    for user in kg_users:
        session_key = safe_str(user.get("source_first_key"))
        if not session_key:
            continue
        row = per_session.setdefault(session_key, {
            "source_session_key": session_key,
            "source_session_name": safe_str(user.get("source_first_name")),
            "source_session_start": safe_int(user.get("source_first_ts"), 0),
            "source_session_start_text": safe_str(user.get("source_first_text")),
            "kg_user_count": 0,
            f"corekg_ge{PRIMARY_CORE_THRESHOLD}_user_count": 0,
        })
        row["kg_user_count"] += 1
        if safe_int(user.get(f"is_core_ge{PRIMARY_CORE_THRESHOLD}"), 0) == 1:
            row[f"corekg_ge{PRIMARY_CORE_THRESHOLD}_user_count"] += 1

    ordered = sorted(per_session.values(), key=lambda x: (x["source_session_start"], x["source_session_key"]))
    cumulative = 0
    for row in ordered:
        cumulative += row["kg_user_count"]
        row["cumulative_kg_user_count"] = cumulative
        row[f"corekg_ge{PRIMARY_CORE_THRESHOLD}_rate_in_cohort"] = fmt_pct(
            row[f"corekg_ge{PRIMARY_CORE_THRESHOLD}_user_count"],
            row["kg_user_count"],
        )
    return ordered


def parse_birthday_guard_events(xml_path: Path):
    root = ET.parse(xml_path).getroot()
    events = []
    for node in root.findall("guard"):
        uid = safe_str(node.attrib.get("uid"))
        if not uid or not uid.isdigit():
            continue
        events.append({
            "uid": uid,
            "uname": safe_str(node.attrib.get("user")),
            "level": safe_int(node.attrib.get("level"), 0),
            "count": safe_int(node.attrib.get("count"), 1),
            "ts": safe_str(node.attrib.get("ts")),
        })
    return events


def classify_birthday_guard(uid, blacklist_uids, universe):
    if uid in blacklist_uids:
        return "黑名单长期观众"
    user = universe.get(uid)
    if user is None:
        return "kg"
    if user["is_510_return"] == 1:
        return "510回旋"
    if user["is_broad_return_exclusive"] == 1:
        return "广义回旋"
    return "kg"


def build_birthday_rows(xml_path, blacklist_uids, universe):
    if not xml_path.exists():
        return [], []

    events = parse_birthday_guard_events(xml_path)
    per_uid = {}
    for event in events:
        uid = event["uid"]
        row = per_uid.setdefault(uid, {
            "uid": uid,
            "uname": event["uname"],
            "guard_event_count": 0,
            "guard_count_sum": 0,
            "max_level": 0,
        })
        row["guard_event_count"] += 1
        row["guard_count_sum"] += event["count"]
        row["max_level"] = max(row["max_level"], event["level"])
        if event["uname"] and not row["uname"]:
            row["uname"] = event["uname"]

    detail_rows = []
    summary = defaultdict(lambda: {"unique_guard_users": 0, "guard_count_sum": 0, "level_1_count": 0, "level_2_count": 0, "level_3_count": 0})
    total_users = len(per_uid)
    total_guard_count = sum(row["guard_count_sum"] for row in per_uid.values())

    for uid, row in sorted(per_uid.items(), key=lambda item: (-item[1]["guard_count_sum"], item[0])):
        category = classify_birthday_guard(uid, blacklist_uids, universe)
        user = universe.get(uid, {})
        detail_row = {
            "uid": uid,
            "uname": row["uname"],
            "category": category,
            "guard_event_count": row["guard_event_count"],
            "guard_count_sum": row["guard_count_sum"],
            "max_level": row["max_level"],
            "is_blacklist": int(uid in blacklist_uids),
            "is_510_return": safe_int(user.get("is_510_return"), 0),
            "is_broad_return_exclusive": safe_int(user.get("is_broad_return_exclusive"), 0),
            "is_window_new": safe_int(user.get("is_window_new"), 0 if uid in blacklist_uids else (1 if uid not in universe else 0)),
            "first_seen_text": user.get("first_seen_text", ""),
            "last_pre_return_text": user.get("last_pre_return_text", ""),
            "as_active_count_after_source": safe_int(user.get("as_active_count_after_source"), 0),
        }
        detail_rows.append(detail_row)
        summary[category]["unique_guard_users"] += 1
        summary[category]["guard_count_sum"] += row["guard_count_sum"]
        if row["max_level"] in (1, 2, 3):
            summary[category][f"level_{row['max_level']}_count"] += 1

    summary_rows = []
    for category in BIRTHDAY_CATEGORY_ORDER:
        item = summary.get(category, {"unique_guard_users": 0, "guard_count_sum": 0, "level_1_count": 0, "level_2_count": 0, "level_3_count": 0})
        summary_rows.append({
            "category": category,
            "unique_guard_users": item["unique_guard_users"],
            "unique_guard_user_rate": fmt_pct(item["unique_guard_users"], total_users),
            "guard_count_sum": item["guard_count_sum"],
            "guard_count_rate": fmt_pct(item["guard_count_sum"], total_guard_count),
            "level_1_count": item["level_1_count"],
            "level_2_count": item["level_2_count"],
            "level_3_count": item["level_3_count"],
        })
    return summary_rows, detail_rows


def build_summary_markdown(definition_rows, core_new_rows, return_rows, birthday_rows, output_path):
    definition_map = {row["item"]: row for row in definition_rows if row.get("item")}
    birthday_map = {row["category"]: row for row in birthday_rows}
    touched_total = safe_int(definition_map["乃琳鸣潮触达用户"]["user_count"], 0)
    kg_total = safe_int(definition_map["kg"]["user_count"], 0)
    core_primary_total = safe_int(definition_map[f"核心kg_ge{PRIMARY_CORE_THRESHOLD}"]["user_count"], 0)
    core_primary_rate = safe_float(definition_map["主承接口径"]["conversion_rate"], 0.0)
    global_510 = next(row for row in return_rows if row["scope"] == "全站" and row["segment"] == "510回旋")
    global_broad = next(row for row in return_rows if row["scope"] == "全站" and row["segment"] == "广义回旋")

    lines = [
        "# 摘要",
        "",
        "## 1. 核心定义",
        f"- `kg` 固定指：`2025-12-09 及之后首次被观测到，且首次互动直播就是 {SOURCE_LIVE_FIXED}` 的用户。",
        f"- `乃琳鸣潮触达用户` 指：至少进入过一次 `{SOURCE_LIVE_FIXED}` 的用户。",
        f"- 主核心口径固定为：`kg 在首次进入乃琳_鸣潮后，累计在 A-SOUL 任意直播有效到场>={PRIMARY_CORE_THRESHOLD}`。",
        "- 这里的有效到场沿用项目统一规则：`单场弹幕>=2 或有礼物金额`。",
        "",
        "## 2. kg 里，真正能被定义成核心观众的有多少",
        f"- 乃琳鸣潮触达用户：`{touched_total:,}`。",
        f"- kg 总量：`{kg_total:,}`。",
        f"- 主口径核心 kg：`{core_primary_total:,}`，占 kg `"
        f"{safe_float_pct(core_primary_rate)}`。",
    ]
    for row in core_new_rows:
        lines.append(
            f"- 敏感度口径 `>= {row['threshold']}`：`{safe_int(row['user_count'], 0):,}` 人，"
            f"占 kg `{safe_float_pct(row['user_rate_in_kg'])}`。"
        )

    lines.extend([
        "",
        "## 3. 510回旋和广义回旋有多少",
        f"- 510 回旋：`{safe_int(global_510['raw_user_count'], 0):,}` 人；其中达到主核心口径的有 `"
        f"{safe_int(global_510[f'core_ge{PRIMARY_CORE_THRESHOLD}_count'], 0):,}` 人。",
        f"- 广义回旋：`{safe_int(global_broad['raw_user_count'], 0):,}` 人；其中达到主核心口径的有 `"
        f"{safe_int(global_broad[f'core_ge{PRIMARY_CORE_THRESHOLD}_count'], 0):,}` 人。",
        "",
        "## 4. 承接率到底在说什么",
        f"- 这次把“承接成功”固定成主核心口径，不再用模糊的来过一次或发过一条弹幕。",
        f"- 承接率 = `kg -> 核心kg_ge{PRIMARY_CORE_THRESHOLD}`。",
        "",
        "## 5. 嘉然生日会舰长画像",
    ])
    if birthday_rows:
        for category in BIRTHDAY_CATEGORY_ORDER:
            row = birthday_map.get(category, {})
            lines.append(
                f"- {category}：`{safe_int(row.get('unique_guard_users'), 0):,}` 人，"
                f"占舰长独立 UID `{safe_float_pct(row.get('unique_guard_user_rate', 0.0))}`；"
                f"对应舰长份数 `{safe_int(row.get('guard_count_sum'), 0):,}`，"
                f"占全部舰长份数 `{safe_float_pct(row.get('guard_count_rate', 0.0))}`。"
            )
    else:
        lines.append("- 未检测到生日会 XML，未生成舰长画像。")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def plot_core_new(core_new_rows, output_path):
    labels = [f">={row['threshold']}" for row in core_new_rows]
    counts = [row["user_count"] for row in core_new_rows]
    rates = [row["user_rate_in_kg"] for row in core_new_rows]
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    colors = ["#A8DADC", "#2A9D8F", "#1D3557"]
    ax.bar(labels, counts, color=colors)
    ax.set_title("核心 kg 人数")
    ax.set_xlabel("A-SOUL 任意直播累计有效到场阈值")
    ax.set_ylabel("人数")
    ymax = max(counts) if counts else 0
    for idx, count in enumerate(counts):
        ax.text(idx, count + max(ymax * 0.015, 20), f"{count:,}\n{safe_float_pct(rates[idx])}", ha="center", va="bottom", fontsize=9)
    ax.set_ylim(0, ymax * 1.18 if ymax else 1)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def plot_return_sets(return_rows, output_path):
    groups = [
        ("全站", "510回旋", "510回旋"),
        ("全站", "广义回旋", "广义回旋"),
    ]
    raw_values = []
    ge3_values = []
    ge5_values = []
    for scope, segment, _ in groups:
        row = next(item for item in return_rows if item["scope"] == scope and item["segment"] == segment)
        raw_values.append(row["raw_user_count"])
        ge3_values.append(row["core_ge3_count"])
        ge5_values.append(row["core_ge5_count"])
    labels = [label for _, _, label in groups]
    x = list(range(len(labels)))
    width = 0.24
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar([i - width for i in x], raw_values, width=width, color="#D9E6F2", label="原始集合")
    ax.bar(x, ge3_values, width=width, color="#4C78A8", label="A-SOUL有效到场>=3")
    ax.bar([i + width for i in x], ge5_values, width=width, color="#E76F51", label="A-SOUL有效到场>=5")
    ax.set_xticks(x, labels)
    ax.set_ylabel("人数")
    ax.set_title("回旋用户集合规模与留存强度")
    ax.legend(frameon=False, ncol=3, loc="upper right")
    ymax = max(raw_values) if raw_values else 0
    for idx, value in enumerate(raw_values):
        ax.text(idx - width, value + max(ymax * 0.01, 10), f"{value:,}", ha="center", va="bottom", fontsize=8.5)
    for idx, value in enumerate(ge3_values):
        ax.text(idx, value + max(ymax * 0.01, 10), f"{value:,}", ha="center", va="bottom", fontsize=8.5)
    for idx, value in enumerate(ge5_values):
        ax.text(idx + width, value + max(ymax * 0.01, 10), f"{value:,}", ha="center", va="bottom", fontsize=8.5)
    ax.set_ylim(0, ymax * 1.22 if ymax else 1)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def plot_host_flow_segments(flow_rows, output_path):
    labels = [row["segment"] for row in flow_rows]
    values = [row["user_count"] for row in flow_rows]
    rates = [row["user_rate"] for row in flow_rows]
    colors = ["#C0CAD4", "#2A9D8F", "#4C78A8", "#E76F51", "#5DA5DA", "#F4A261", "#8E6C8A", "#1D3557"]
    fig, ax = plt.subplots(figsize=(11, 5.6))
    ax.bar(labels, values, color=colors[: len(labels)])
    ax.set_title("kg 团内流动分层")
    ax.set_ylabel("人数")
    ax.set_xticks(range(len(labels)), labels, rotation=20, ha="right")
    ymax = max(values) if values else 0
    for idx, value in enumerate(values):
        ax.text(
            idx,
            value + max(ymax * 0.012, 10),
            f"{value:,}\n{safe_float_pct(rates[idx])}",
            ha="center",
            va="bottom",
            fontsize=8.5,
        )
    ax.set_ylim(0, ymax * 1.22 if ymax else 1)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def plot_kg_trend(trend_rows, output_path):
    labels = [safe_str(row["source_session_start_text"])[5:16] for row in trend_rows]
    cohort_values = [safe_int(row["kg_user_count"], 0) for row in trend_rows]
    cumulative_values = [safe_int(row["cumulative_kg_user_count"], 0) for row in trend_rows]

    fig, ax1 = plt.subplots(figsize=(11.5, 5.8))
    ax1.bar(range(len(labels)), cohort_values, color="#A8DADC", label="单场新增kg")
    ax1.set_ylabel("单场新增kg")
    ax1.set_title("kg 进入时间趋势")
    ax1.set_xticks(range(len(labels)), labels, rotation=35, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(range(len(labels)), cumulative_values, color="#1D3557", marker="o", linewidth=2.2, label="累计kg")
    ax2.set_ylabel("累计kg")

    ymax = max(cohort_values) if cohort_values else 0
    for idx, value in enumerate(cohort_values):
        ax1.text(idx, value + max(ymax * 0.01, 10), f"{value:,}", ha="center", va="bottom", fontsize=7.8, rotation=90)

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def plot_birthday_guard_users(summary_rows, output_path):
    labels = [row["category"] for row in summary_rows]
    values = [row["unique_guard_users"] for row in summary_rows]
    colors = [BIRTHDAY_COLORS[label] for label in labels]
    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    ax.bar(labels, values, color=colors)
    ax.set_title("嘉然生日会舰长构成")
    ax.set_ylabel("独立舰长 UID 数")
    ax.set_xticks(range(len(labels)), labels, rotation=18, ha="right")
    ymax = max(values) if values else 0
    for idx, row in enumerate(summary_rows):
        ax.text(
            idx,
            row["unique_guard_users"] + max(ymax * 0.015, 10),
            f"{row['unique_guard_users']:,}\n{safe_float_pct(row['unique_guard_user_rate'])}",
            ha="center",
            va="bottom",
            fontsize=8.5,
        )
    ax.set_ylim(0, ymax * 1.25 if ymax else 1)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def plot_birthday_guard_counts(summary_rows, output_path):
    labels = [row["category"] for row in summary_rows]
    values = [row["guard_count_sum"] for row in summary_rows]
    colors = [BIRTHDAY_COLORS[label] for label in labels]
    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    ax.bar(labels, values, color=colors)
    ax.set_title("嘉然生日会舰长份数构成")
    ax.set_ylabel("舰长份数")
    ax.set_xticks(range(len(labels)), labels, rotation=18, ha="right")
    ymax = max(values) if values else 0
    for idx, row in enumerate(summary_rows):
        ax.text(
            idx,
            row["guard_count_sum"] + max(ymax * 0.015, 10),
            f"{row['guard_count_sum']:,}\n{safe_float_pct(row['guard_count_rate'])}",
            ha="center",
            va="bottom",
            fontsize=8.5,
        )
    ax.set_ylim(0, ymax * 1.25 if ymax else 1)
    fig.tight_layout()
    fig.savefig(output_path, format="svg")
    plt.close(fig)


def clear_variant_dir(variant_dir: Path):
    if variant_dir.exists():
        shutil.rmtree(variant_dir)
    variant_dir.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="生成精简后的核心分析报告")
    parser.add_argument("--variant-name", default="默认版", help="输出到 分析结果/核心报告/ 下的目录名")
    parser.add_argument(
        "--exclude-source-single-pass-session-key",
        default="",
        help="排除首次 source 即命中该 session_key、且之后再未出现的用户",
    )
    parser.add_argument("--archive-legacy", action="store_true", help="把旧版分析结果移动到 分析结果/legacy/")
    args = parser.parse_args()

    detail_rows = load_detail_rows()
    blacklist_uids = load_blacklist_uids()
    records, invalid_uid_count, blacklisted_count = build_records(detail_rows, blacklist_uids)
    excluded_count = 0
    if args.exclude_source_single_pass_session_key:
        exclude_uids = build_single_pass_exclusion_uids(records, SOURCE_LIVE_FIXED, args.exclude_source_single_pass_session_key)
        excluded_count = len(exclude_uids)
        records = [record for record in records if record["uid"] not in exclude_uids]

    universe, source_users, pure_new_users = build_user_universe(records, SOURCE_LIVE_FIXED)
    definition_rows = build_definition_rows(source_users, pure_new_users)
    core_new_rows = build_core_new_rows(pure_new_users)
    return_rows = build_return_rows(universe, source_users)
    host_flow_rows = build_host_flow_rows(pure_new_users, universe)
    kg_trend_rows = build_kg_trend_rows(pure_new_users)
    birthday_summary_rows, birthday_detail_rows = build_birthday_rows(BIRTHDAY_XML, blacklist_uids, universe)

    core_users = [user for user in pure_new_users if user[f"is_core_ge{PRIMARY_CORE_THRESHOLD}"] == 1]
    source_return_510 = [user for user in source_users if user["is_510_return"] == 1]
    source_return_broad = [user for user in source_users if user["is_broad_return_exclusive"] == 1]

    variant_dir = OUT_ROOT / args.variant_name
    clear_variant_dir(variant_dir)
    tables_dir = variant_dir / "tables"
    plots_dir = variant_dir / "plots"
    tables_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    write_csv(definition_rows, tables_dir / "00_口径定义.csv")
    write_csv(core_new_rows, tables_dir / "01_核心kg分层.csv")
    write_csv(return_rows, tables_dir / "02_回旋集合分层.csv")
    write_csv(host_flow_rows, tables_dir / "03_kg团内流动分层.csv")
    write_csv(kg_trend_rows, tables_dir / "04_kg进入时间趋势.csv")
    write_csv(birthday_summary_rows, tables_dir / "05_生日会舰长概览.csv")
    write_csv(build_uid_list_rows(pure_new_users), tables_dir / "06_kg_UID名单.csv")
    write_csv(build_uid_list_rows(core_users), tables_dir / f"07_核心kg_ge{PRIMARY_CORE_THRESHOLD}_UID名单.csv")
    write_csv(build_uid_list_rows(source_return_510), tables_dir / "08_乃琳鸣潮触达内_510回旋_UID名单.csv")
    write_csv(build_uid_list_rows(source_return_broad), tables_dir / "09_乃琳鸣潮触达内_广义回旋_UID名单.csv")
    write_csv(birthday_detail_rows, tables_dir / "10_生日会舰长明细.csv")

    plot_core_new(core_new_rows, plots_dir / "01_核心kg阈值.svg")
    plot_return_sets(return_rows, plots_dir / "02_回旋集合规模.svg")
    plot_host_flow_segments(host_flow_rows, plots_dir / "03_kg团内流动分层.svg")
    plot_kg_trend(kg_trend_rows, plots_dir / "04_kg进入时间趋势.svg")
    if birthday_summary_rows:
        plot_birthday_guard_users(birthday_summary_rows, plots_dir / "05_生日会舰长构成_按人数.svg")
        plot_birthday_guard_counts(birthday_summary_rows, plots_dir / "06_生日会舰长构成_按份数.svg")

    build_summary_markdown(
        definition_rows,
        core_new_rows,
        return_rows,
        birthday_summary_rows,
        variant_dir / "摘要.md",
    )

    if args.archive_legacy:
        maybe_archive_legacy_outputs()

    print(f"[OK] 原始明细行数: {len(detail_rows)}")
    print(f"[OK] 黑名单过滤行数: {blacklisted_count}")
    print(f"[OK] 非法UID过滤行数: {invalid_uid_count}")
    print(f"[OK] 额外排除 0211 单次且无后续用户数: {excluded_count}")
    print(f"[OK] 乃琳鸣潮触达用户数: {len(source_users)}")
    print(f"[OK] kg 用户数: {len(pure_new_users)}")
    print(f"[OK] 主口径核心kg数: {len(core_users)}")
    print(f"[OK] 生日会舰长独立UID数: {sum(row['unique_guard_users'] for row in birthday_summary_rows)}")
    print(f"[OK] 输出目录: {variant_dir}")


if __name__ == "__main__":
    main()
