import json
import csv
from pathlib import Path
from collections import defaultdict


ROOT = Path(__file__).resolve().parent
JSON_ROOT = ROOT / "弹幕JSON"
DETAIL_ROOT = ROOT / "场次明细表"
DETAIL_JSON_ROOT = DETAIL_ROOT / "弹幕JSON"
SUPPLEMENT_DETAIL_CSV = ROOT / "oldblacklist" / "as_output" / "uid_live_stats.csv"
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
]


def detect_host_and_type(folder_name: str, file_name: str, channel_name: str = "", live_title: str = ""):
    text = f"{folder_name} {file_name} {channel_name} {live_title}"

    host = "未知"
    live_type = "其他"

    if "乃琳" in text:
        host = "乃琳"
    elif "嘉然" in text:
        host = "嘉然"
    elif "贝拉" in text:
        host = "贝拉"

    if "团播" in text:
        live_type = "团播"
    elif "鸣潮" in text:
        live_type = "鸣潮"
    elif "终末地" in text:
        live_type = "终末地"
    elif "3D" in text or "3d" in text:
        live_type = "3D"
    elif "电台" in text:
        live_type = "电台"
    elif "电影" in text:
        live_type = "电影"

    return host, live_type


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
    is_present, is_active = recalc_present_active(normalized)
    normalized["is_present"] = is_present
    normalized["is_active"] = is_active
    normalized["_source_priority"] = 2 if source_tag == "json" else 1
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

    for field in ("uname", "sample_folder", "live_name", "live_id", "room_id", "channel_name", "live_title"):
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
    return preferred


def merge_detail_rows(json_rows, supplement_rows):
    dedup = {}

    for source_tag, rows in (("supplement", supplement_rows), ("json", json_rows)):
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


def load_supplement_rows(csv_path: Path):
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))



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

    host, live_type = detect_host_and_type(folder_name, live_name, channel_name, live_title)

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


def write_detail_csv(rows, output_csv: Path):
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in DETAIL_FIELDS} for row in rows])


def batch_process(root_dir: str, output_dir: str = None, merged_output_csv: str | Path | None = None):
    root_path = Path(root_dir)

    if output_dir:
        out_root = Path(output_dir)
    else:
        out_root = root_path / "detail_output"

    if merged_output_csv:
        merged_csv = Path(merged_output_csv)
    else:
        merged_csv = out_root / "all_live_details.csv"

    out_root.mkdir(parents=True, exist_ok=True)

    all_rows = []
    success_count = 0
    fail_count = 0

    json_files = sorted(root_path.rglob("*.json"))

    if not json_files:
        print("[WARN] 没有找到任何 json 文件")
        return

    for json_file in json_files:
        try:
            rows = aggregate_one_live(json_file)

            relative_parent = json_file.parent.relative_to(root_path)
            target_dir = out_root / relative_parent
            target_dir.mkdir(parents=True, exist_ok=True)

            output_csv = target_dir / f"{json_file.stem}_detail.csv"
            write_detail_csv(rows, output_csv)

            all_rows.extend(rows)
            success_count += 1

            print(f"[OK] {json_file} -> {output_csv} | 用户数: {len(rows)}")

        except Exception as e:
            fail_count += 1
            print(f"[FAIL] {json_file} | 原因: {e}")

    supplement_rows = load_supplement_rows(SUPPLEMENT_DETAIL_CSV)
    if supplement_rows:
        print(f"[INFO] 载入补充明细: {SUPPLEMENT_DETAIL_CSV} | 行数: {len(supplement_rows)}")

    merged_rows = merge_detail_rows(all_rows, supplement_rows)
    if merged_rows:
        write_detail_csv(merged_rows, merged_csv)
        print(f"[OK] 已生成总汇总表: {merged_csv}")

    print("--------------------------------------------------")
    print(f"[DONE] JSON 文件数: {len(json_files)}")
    print(f"[DONE] 成功: {success_count}")
    print(f"[DONE] 失败: {fail_count}")
    print(f"[DONE] 补充明细行数: {len(supplement_rows)}")
    print(f"[DONE] 合并后总行数: {len(merged_rows)}")
    print(f"[DONE] 输出目录: {out_root}")


if __name__ == "__main__":
    root_dir = JSON_ROOT
    output_dir = DETAIL_JSON_ROOT
    merged_output_csv = DETAIL_ROOT / "all_live_details.csv"
    batch_process(root_dir, output_dir, merged_output_csv)
