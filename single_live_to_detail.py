import csv
import json
import re
from collections import defaultdict
from pathlib import Path

from analysis_settings import CN_TZ


ROOT = Path(__file__).resolve().parent
JSON_ROOT = ROOT / "弹幕JSON"
XML_ROOT = ROOT / "XML弹幕文件"
DETAIL_ROOT = ROOT / "场次明细表"
DETAIL_JSON_ROOT = DETAIL_ROOT / "弹幕JSON"
DETAIL_XML_ROOT = DETAIL_ROOT / "XML弹幕文件"

DETAIL_FIELDS = [
    "uid",
    "uname",
    "host",
    "live_type",
    "sample_folder",
    "live_name",
    "live_id",
    "room_id",
    "channel_name",
    "live_title",
    "start_date",
    "stop_date",
    "danmu_count",
    "gift_count",
    "gift_amount",
    "first_send_date",
    "last_send_date",
    "is_present",
    "is_active",
    "data_source",
    "is_multi_host",
]

DATE_RE = re.compile(r"^(?P<date>\d{4}\.\d{2}\.\d{2})\s+(?P<title>.+)$")
ATTR_RE = re.compile(r'([A-Za-z_]+)="([^"]*)"')
RAW_UID_RE = re.compile(r"(?:\[|,)(\d+)(?:,&quot;|,\")")

MEMBER_NAMES = ["嘉然", "乃琳", "贝拉", "向晚", "珈乐"]
GROUP_HINTS = [
    "A-SOUL夜谈",
    "A-SOUL小剧场",
    "A-SOUL游戏室",
    "A-SOUL团综",
    "A-SOUL元宵团播",
    "A-SOUL",
    "枝江综艺",
    "集光之夜",
    "二创计画",
]
GROUP_TYPE_HINTS = [
    "团播",
    "小剧场",
    "夜谈",
    "游戏室",
    "综艺",
    "纪念直播",
    "生日会",
    "BW",
]
TYPE_RULES = [
    ("鸣潮", "鸣潮"),
    ("终末地", "终末地"),
    ("只狼", "只狼"),
    ("黑神话", "黑神话"),
    ("3D", "3D"),
    ("3d", "3D"),
    ("2D", "2D"),
    ("2d", "2D"),
    ("电台", "电台"),
    ("电影", "电影"),
    ("KTV", "KTV"),
    ("ktv", "KTV"),
    ("团播", "团播"),
    ("小剧场", "团播"),
    ("夜谈", "团播"),
    ("游戏室", "团播"),
    ("综艺", "团播"),
    ("纪念直播", "团播"),
    ("生日会", "团播"),
    ("BW", "团播"),
]
SPECIAL_GROUP_TITLES = {
    "我们一起跨年叭~.xml",
    "愚人节“人生有梦”歌舞会.xml",
    "五一特别企划.xml",
}
SPECIAL_XML_HOST_TYPE = {
    "2025.11.16 A-SOUL团综 第十五期 伪人测试.xml": ("嘉然", "团播"),
    "2025.12.20 【A-SOUL五周年纪念直播】锚定黎明.xml": ("嘉然", "团播"),
}
HOST_BY_USER_NAME = {
    "嘉然今天吃什么": "嘉然",
    "乃琳Queen": "乃琳",
    "贝拉kira": "贝拉",
    "向晚大魔王": "向晚",
    "珈乐Carol": "珈乐",
}


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def recalc_present_active(row):
    danmu_count = safe_int(row.get("danmu_count"), 0)
    gift_count = safe_int(row.get("gift_count"), 0)
    gift_amount = safe_float(row.get("gift_amount"), 0.0)
    is_present = 1 if (danmu_count > 0 or gift_count > 0 or gift_amount > 0) else safe_int(row.get("is_present"), 0)
    is_active = 1 if (danmu_count >= 2 or gift_amount > 0) else 0
    return is_present, is_active


def parse_filename(file_name: str):
    match = DATE_RE.match(file_name)
    if not match:
        return "", "", file_name
    date_text = match.group("date")
    title_text = match.group("title")
    return date_text, date_text[:4], title_text


def date_text_to_ms(date_text: str):
    if not date_text:
        return 0
    try:
        dt = Path(date_text)
        _ = dt
    except Exception:
        pass
    from datetime import datetime

    try:
        parsed = datetime.strptime(date_text, "%Y.%m.%d").replace(tzinfo=CN_TZ)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return 0


OLD_HOST_IGNORE_CUTOFF_MS = date_text_to_ms("2022.05.20")


def guess_host_and_multi(text: str):
    if text in SPECIAL_GROUP_TITLES:
        return "团体/官方", 0

    hit_members = [name for name in MEMBER_NAMES if name in text]
    hit_groups = [hint for hint in GROUP_HINTS if hint in text]
    hit_group_types = [hint for hint in GROUP_TYPE_HINTS if hint in text]

    if len(hit_members) >= 2:
        first_member = min(
            hit_members,
            key=lambda name: (text.find(name), MEMBER_NAMES.index(name)),
        )
        return first_member, 1
    if len(hit_members) == 1:
        return hit_members[0], 0
    if hit_groups or hit_group_types:
        return "团体/官方", 0
    return "未知", 0


def guess_live_type(text: str):
    for keyword, live_type in TYPE_RULES:
        if keyword in text:
            return live_type
    return "其他"


def extract_xml_metadata(xml_path: Path):
    metadata = {}
    in_metadata = False
    with xml_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if line == "<metadata>":
                in_metadata = True
                continue
            if in_metadata and line == "</metadata>":
                break
            if not in_metadata:
                continue

            for key in ("user_name", "room_id", "room_title", "area", "parent_area"):
                start_tag = f"<{key}>"
                end_tag = f"</{key}>"
                if start_tag in line and end_tag in line:
                    value = line.split(start_tag, 1)[1].split(end_tag, 1)[0].strip()
                    metadata[key] = value
    return metadata


def infer_host_from_user_name(user_name: str):
    user_name = safe_str(user_name)
    if user_name in HOST_BY_USER_NAME:
        return HOST_BY_USER_NAME[user_name]
    for display_name, host in HOST_BY_USER_NAME.items():
        if display_name and display_name in user_name:
            return host
    for host in MEMBER_NAMES:
        if host in user_name:
            return host
    return ""


def detect_xml_host_and_type(file_name: str, metadata: dict, fallback_start_date: int):
    _, _, title_text = parse_filename(file_name)
    room_title = safe_str(metadata.get("room_title"))
    user_name = safe_str(metadata.get("user_name"))
    room_id = safe_str(metadata.get("room_id"))

    title_for_rules = title_text
    room_rule_text = f"{user_name} {room_title}".strip()

    special_override = SPECIAL_XML_HOST_TYPE.get(file_name)
    if special_override is not None:
        host, live_type = special_override
        return host, live_type, 0, user_name, room_id, room_title

    if title_text in SPECIAL_GROUP_TITLES:
        return "团体/官方", "团播", 0, user_name, room_id, room_title

    if fallback_start_date and fallback_start_date <= OLD_HOST_IGNORE_CUTOFF_MS:
        if file_name == "2022.05.20 非官方 · 伪 · 珈乐毕业回.xml":
            return "未知", "其他", 0, user_name, room_id, room_title
        return "未知", "其他", 0, user_name, room_id, room_title

    if "A-SOUL" in title_text:
        host = infer_host_from_user_name(user_name) or "团体/官方"
        return host, "团播", 0, user_name, room_id, room_title

    host = infer_host_from_user_name(user_name)
    if not host:
        host, is_multi_host = guess_host_and_multi(title_for_rules)
    else:
        _, is_multi_host = guess_host_and_multi(title_for_rules)

    live_type = guess_live_type(room_rule_text) if room_rule_text else "其他"
    if live_type == "其他":
        live_type = guess_live_type(title_for_rules)

    return host, live_type, is_multi_host, user_name, room_id, room_title


def detect_host_and_type(folder_name: str, file_name: str, channel_name: str = "", live_title: str = ""):
    text = f"{folder_name} {file_name} {channel_name} {live_title}"
    host, is_multi_host = guess_host_and_multi(text)
    live_type = guess_live_type(text)
    return host, live_type, is_multi_host


def make_session_key(row):
    return (
        safe_str(row.get("live_id"))
        or safe_str(row.get("live_name"))
        or safe_str(row.get("start_date"))
    )


def normalize_detail_row(row, source_tag):
    normalized = {field: row.get(field, "") for field in DETAIL_FIELDS}
    normalized["uid"] = safe_str(normalized["uid"])
    normalized["uname"] = safe_str(normalized["uname"])
    normalized["host"] = safe_str(normalized["host"]) or "未知"
    normalized["live_type"] = safe_str(normalized["live_type"]) or "其他"
    normalized["sample_folder"] = safe_str(normalized["sample_folder"])
    normalized["live_name"] = safe_str(normalized["live_name"])
    normalized["live_id"] = safe_str(normalized["live_id"])
    normalized["room_id"] = safe_str(normalized["room_id"])
    normalized["channel_name"] = safe_str(normalized["channel_name"])
    normalized["live_title"] = safe_str(normalized["live_title"])
    normalized["start_date"] = safe_int(normalized["start_date"], 0)
    normalized["stop_date"] = safe_int(normalized["stop_date"], 0)
    normalized["danmu_count"] = safe_int(normalized["danmu_count"], 0)
    normalized["gift_count"] = safe_int(normalized["gift_count"], 0)
    normalized["gift_amount"] = round(safe_float(normalized["gift_amount"], 0.0), 4)
    normalized["first_send_date"] = safe_int(normalized["first_send_date"], 0)
    normalized["last_send_date"] = safe_int(normalized["last_send_date"], 0)
    normalized["data_source"] = safe_str(normalized["data_source"]) or source_tag
    normalized["is_multi_host"] = safe_int(normalized["is_multi_host"], 0)
    is_present, is_active = recalc_present_active(normalized)
    normalized["is_present"] = is_present
    normalized["is_active"] = is_active
    priority_map = {"json": 3, "xml": 2, "supplement": 1}
    normalized["_source_priority"] = priority_map.get(source_tag, 0)
    return normalized


def choose_nonempty_min(*values):
    candidates = [value for value in values if value not in ("", 0, None)]
    return min(candidates) if candidates else 0


def choose_nonempty_max(*values):
    candidates = [value for value in values if value not in ("", 0, None)]
    return max(candidates) if candidates else 0


def merge_duplicate_detail_rows(existing, new_row):
    if new_row["_source_priority"] > existing["_source_priority"]:
        preferred = dict(new_row)
        fallback = existing
    else:
        preferred = dict(existing)
        fallback = new_row

    for field in ("uname", "sample_folder", "live_name", "live_id", "room_id", "channel_name", "live_title", "data_source"):
        if not preferred[field] and fallback[field]:
            preferred[field] = fallback[field]

    preferred["start_date"] = choose_nonempty_min(existing["start_date"], new_row["start_date"])
    preferred["stop_date"] = choose_nonempty_max(existing["stop_date"], new_row["stop_date"])
    preferred["first_send_date"] = choose_nonempty_min(existing["first_send_date"], new_row["first_send_date"])
    preferred["last_send_date"] = choose_nonempty_max(existing["last_send_date"], new_row["last_send_date"])
    preferred["danmu_count"] = max(existing["danmu_count"], new_row["danmu_count"])
    preferred["gift_count"] = max(existing["gift_count"], new_row["gift_count"])
    preferred["gift_amount"] = round(max(existing["gift_amount"], new_row["gift_amount"]), 4)
    preferred["is_present"] = max(existing["is_present"], new_row["is_present"])
    preferred["is_active"] = max(existing["is_active"], new_row["is_active"])
    preferred["is_multi_host"] = max(existing["is_multi_host"], new_row["is_multi_host"])
    return preferred


def merge_detail_rows(json_rows, xml_rows):
    dedup = {}

    for source_tag, rows in (("xml", xml_rows), ("json", json_rows)):
        for row in rows:
            normalized = normalize_detail_row(row, source_tag)
            session_key = make_session_key(normalized)
            if not normalized["uid"] or not session_key:
                continue

            dedup_key = (
                normalized["uid"],
                normalized["host"],
                normalized["live_type"],
                session_key,
            )
            existing = dedup.get(dedup_key)
            if existing is None:
                dedup[dedup_key] = normalized
            else:
                dedup[dedup_key] = merge_duplicate_detail_rows(existing, normalized)

    merged_rows = sorted(
        (
            {field: row[field] for field in DETAIL_FIELDS}
            for row in dedup.values()
        ),
        key=lambda row: (
            safe_int(row.get("start_date"), 0) or safe_int(row.get("first_send_date"), 0),
            safe_str(row.get("host")),
            safe_str(row.get("live_type")),
            safe_str(row.get("live_name")),
            safe_str(row.get("uid")),
        ),
    )
    return merged_rows


def load_json_file(json_path: Path):
    with json_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def aggregate_one_live(json_path: Path):
    raw = load_json_file(json_path)

    if raw.get("code") != 200:
        raise ValueError(f"JSON 返回 code 不是 200: {raw.get('code')}")

    outer_data = raw.get("data", {})
    inner_data = outer_data.get("data", {})

    channel = inner_data.get("channel", {})
    live = inner_data.get("live", {})
    danmakus = inner_data.get("danmakus", [])

    live_name = json_path.stem
    folder_name = json_path.parent.name
    channel_name = channel.get("uName", "")
    live_title = live.get("title", "")

    host, live_type, is_multi_host = detect_host_and_type(folder_name, live_name, channel_name, live_title)

    live_id = live.get("liveId", "")
    room_id = channel.get("roomId", "")
    start_date = live.get("startDate", "")
    stop_date = live.get("stopDate", "")

    user_stats = defaultdict(lambda: {
        "uid": "",
        "uname": "",
        "host": host,
        "live_type": live_type,
        "sample_folder": folder_name,
        "live_name": live_name,
        "live_id": live_id,
        "room_id": room_id,
        "channel_name": channel_name,
        "live_title": live_title,
        "start_date": start_date,
        "stop_date": stop_date,
        "danmu_count": 0,
        "gift_count": 0,
        "gift_amount": 0.0,
        "first_send_date": "",
        "last_send_date": "",
        "is_present": 1,
        "is_active": 0,
        "data_source": "json",
        "is_multi_host": is_multi_host,
    })

    for item in danmakus:
        uid = item.get("uId")
        uname = item.get("uName", "")
        event_type = item.get("type")
        send_date = item.get("sendDate", "")

        if uid in (None, ""):
            continue

        uid = str(uid)
        stat = user_stats[uid]

        stat["uid"] = uid
        if uname and not stat["uname"]:
            stat["uname"] = uname

        if not stat["first_send_date"]:
            stat["first_send_date"] = send_date
        stat["last_send_date"] = send_date

        if event_type == 0:
            stat["danmu_count"] += 1
        elif event_type == 1:
            count = item.get("count", 1) or 1
            price = item.get("price", 0) or 0
            stat["gift_count"] += int(count)
            stat["gift_amount"] += float(price) * int(count)

    rows = []
    for stat in user_stats.values():
        stat["gift_amount"] = round(stat["gift_amount"], 4)
        stat["is_active"] = 1 if (stat["danmu_count"] >= 2 or stat["gift_amount"] > 0) else 0
        rows.append(stat)

    rows.sort(key=lambda x: (-x["danmu_count"], -x["gift_amount"], x["uid"]))
    return rows


def parse_attrs(line: str):
    return dict(ATTR_RE.findall(line))


def extract_uid_from_attrs(attrs):
    uid = safe_str(attrs.get("uid"))
    if uid.isdigit():
        return uid
    raw = safe_str(attrs.get("raw"))
    match = RAW_UID_RE.search(raw)
    if match:
        return match.group(1)
    return ""


def aggregate_one_xml(xml_path: Path):
    date_text, _, title_text = parse_filename(xml_path.name)
    fallback_start_date = date_text_to_ms(date_text)
    metadata = extract_xml_metadata(xml_path)
    host, live_type, is_multi_host, channel_name, room_id, room_title = detect_xml_host_and_type(
        xml_path.name,
        metadata,
        fallback_start_date,
    )
    title_no_ext = Path(title_text).stem
    live_name = Path(room_title or title_text).stem

    user_stats = defaultdict(lambda: {
        "uid": "",
        "uname": "",
        "host": host,
        "live_type": live_type,
        "sample_folder": "XML弹幕文件",
        "live_name": live_name,
        "live_id": f"xml::{xml_path.name}",
        "room_id": room_id,
        "channel_name": channel_name,
        "live_title": room_title or title_no_ext,
        "start_date": 0,
        "stop_date": 0,
        "danmu_count": 0,
        "gift_count": 0,
        "gift_amount": 0.0,
        "first_send_date": 0,
        "last_send_date": 0,
        "is_present": 1,
        "is_active": 0,
        "data_source": "xml",
        "is_multi_host": is_multi_host,
        "_gift_rel_min": None,
        "_gift_rel_max": None,
    })

    anchor_candidates = []
    earliest_abs_ts = 0
    latest_abs_ts = 0

    with xml_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("<d ") or line.startswith("<d\t"):
                attrs = parse_attrs(line)
                uid = extract_uid_from_attrs(attrs)
                if not uid:
                    continue

                stat = user_stats[uid]
                stat["uid"] = uid

                user_name = safe_str(attrs.get("user"))
                if user_name and not stat["uname"]:
                    stat["uname"] = user_name

                p_value = safe_str(attrs.get("p"))
                p_parts = p_value.split(",")
                rel_ts_ms = int(round(safe_float(p_parts[0], 0.0) * 1000)) if p_parts else 0
                abs_ts = safe_int(p_parts[4], 0) if len(p_parts) >= 5 else 0
                if abs_ts > 0:
                    anchor_candidate = abs_ts - rel_ts_ms
                    if anchor_candidate > 0:
                        anchor_candidates.append(anchor_candidate)
                    earliest_abs_ts = abs_ts if earliest_abs_ts == 0 else min(earliest_abs_ts, abs_ts)
                    latest_abs_ts = max(latest_abs_ts, abs_ts)
                    stat["first_send_date"] = abs_ts if stat["first_send_date"] == 0 else min(stat["first_send_date"], abs_ts)
                    stat["last_send_date"] = max(stat["last_send_date"], abs_ts)

                stat["danmu_count"] += 1

            elif line.startswith("<gift ") or line.startswith("<gift\t"):
                attrs = parse_attrs(line)
                uid = extract_uid_from_attrs(attrs)
                if not uid:
                    continue

                stat = user_stats[uid]
                stat["uid"] = uid

                user_name = safe_str(attrs.get("user"))
                if user_name and not stat["uname"]:
                    stat["uname"] = user_name

                gift_count = max(1, safe_int(attrs.get("giftcount"), 1))
                price = safe_float(attrs.get("price"), 0.0)
                rel_ts_ms = int(round(safe_float(attrs.get("ts"), 0.0) * 1000))

                stat["gift_count"] += gift_count
                stat["gift_amount"] += round(price * gift_count, 4)
                if stat["_gift_rel_min"] is None or rel_ts_ms < stat["_gift_rel_min"]:
                    stat["_gift_rel_min"] = rel_ts_ms
                if stat["_gift_rel_max"] is None or rel_ts_ms > stat["_gift_rel_max"]:
                    stat["_gift_rel_max"] = rel_ts_ms

    session_anchor_ms = min(anchor_candidates) if anchor_candidates else fallback_start_date
    session_start_ms = session_anchor_ms or earliest_abs_ts or fallback_start_date

    rows = []
    for stat in user_stats.values():
        if not stat["uid"]:
            continue

        if stat["first_send_date"] == 0 and session_anchor_ms and stat["_gift_rel_min"] is not None:
            stat["first_send_date"] = session_anchor_ms + stat["_gift_rel_min"]
        if stat["last_send_date"] == 0 and session_anchor_ms and stat["_gift_rel_max"] is not None:
            stat["last_send_date"] = session_anchor_ms + stat["_gift_rel_max"]

        stat["start_date"] = session_start_ms or stat["first_send_date"] or fallback_start_date
        stat["stop_date"] = max(
            latest_abs_ts,
            stat["last_send_date"],
            stat["start_date"],
        )
        stat["gift_amount"] = round(stat["gift_amount"], 4)
        stat["is_active"] = 1 if (stat["danmu_count"] >= 2 or stat["gift_amount"] > 0) else 0
        stat.pop("_gift_rel_min", None)
        stat.pop("_gift_rel_max", None)
        rows.append(stat)

    rows.sort(key=lambda x: (-x["danmu_count"], -x["gift_amount"], x["uid"]))
    return rows


def write_detail_csv(rows, output_csv: Path):
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in DETAIL_FIELDS} for row in rows])


def batch_process(json_root: Path, xml_root: Path, json_output_dir: Path, xml_output_dir: Path, merged_output_csv: Path):
    json_output_dir.mkdir(parents=True, exist_ok=True)
    xml_output_dir.mkdir(parents=True, exist_ok=True)

    all_json_rows = []
    all_xml_rows = []

    json_success = 0
    json_fail = 0
    xml_success = 0
    xml_fail = 0

    json_files = sorted(json_root.rglob("*.json"))
    xml_files = sorted(xml_root.glob("*.xml"))

    for json_file in json_files:
        try:
            rows = aggregate_one_live(json_file)
            relative_parent = json_file.parent.relative_to(json_root)
            output_csv = json_output_dir / relative_parent / f"{json_file.stem}_detail.csv"
            write_detail_csv(rows, output_csv)
            all_json_rows.extend(rows)
            json_success += 1
            print(f"[OK] JSON {json_file} -> {output_csv} | 用户数: {len(rows)}")
        except Exception as exc:
            json_fail += 1
            print(f"[FAIL] JSON {json_file} | 原因: {exc}")

    for xml_file in xml_files:
        try:
            rows = aggregate_one_xml(xml_file)
            output_csv = xml_output_dir / f"{xml_file.stem}_detail.csv"
            write_detail_csv(rows, output_csv)
            all_xml_rows.extend(rows)
            xml_success += 1
            print(f"[OK] XML {xml_file} -> {output_csv} | 用户数: {len(rows)}")
        except Exception as exc:
            xml_fail += 1
            print(f"[FAIL] XML {xml_file} | 原因: {exc}")

    merged_rows = merge_detail_rows(all_json_rows, all_xml_rows)
    if merged_rows:
        write_detail_csv(merged_rows, merged_output_csv)
        print(f"[OK] 已生成总汇总表: {merged_output_csv}")

    print("--------------------------------------------------")
    print(f"[DONE] JSON 文件数: {len(json_files)}")
    print(f"[DONE] JSON 成功: {json_success}")
    print(f"[DONE] JSON 失败: {json_fail}")
    print(f"[DONE] XML 文件数: {len(xml_files)}")
    print(f"[DONE] XML 成功: {xml_success}")
    print(f"[DONE] XML 失败: {xml_fail}")
    print(f"[DONE] 合并后总行数: {len(merged_rows)}")
    print(f"[DONE] JSON 明细输出目录: {json_output_dir}")
    print(f"[DONE] XML 明细输出目录: {xml_output_dir}")


if __name__ == "__main__":
    batch_process(
        json_root=JSON_ROOT,
        xml_root=XML_ROOT,
        json_output_dir=DETAIL_JSON_ROOT,
        xml_output_dir=DETAIL_XML_ROOT,
        merged_output_csv=DETAIL_ROOT / "all_live_details.csv",
    )
