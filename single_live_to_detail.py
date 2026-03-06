import json
import csv
from pathlib import Path
from collections import defaultdict


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
    fieldnames = [
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

    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def batch_process(root_dir: str, output_dir: str = None):
    root_path = Path(root_dir)

    if output_dir:
        out_root = Path(output_dir)
    else:
        out_root = root_path / "detail_output"

    out_root.mkdir(parents=True, exist_ok=True)

    all_rows = []
    success_count = 0
    fail_count = 0

    json_files = list(root_path.rglob("*.json"))

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

    if all_rows:
        merged_csv = out_root / "all_live_details.csv"
        write_detail_csv(all_rows, merged_csv)
        print(f"[OK] 已生成总汇总表: {merged_csv}")

    print("--------------------------------------------------")
    print(f"[DONE] JSON 文件数: {len(json_files)}")
    print(f"[DONE] 成功: {success_count}")
    print(f"[DONE] 失败: {fail_count}")
    print(f"[DONE] 输出目录: {out_root}")


if __name__ == "__main__":
    root_dir = r"D:\wins\Desktop\as"
    output_dir = r"D:\wins\Desktop\as\场次明细表"
    batch_process(root_dir, output_dir)
