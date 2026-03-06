import csv
from pathlib import Path
from collections import defaultdict


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except:
        return default


def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except:
        return default


def safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def load_detail_csv(detail_csv_path: Path):
    rows = []
    with detail_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_blacklist_uids(blacklist_csv_path: Path):
    uids = set()
    with blacklist_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = safe_str(row.get("uid"))
            if uid:
                uids.add(uid)
    return uids


def build_user_summary(detail_rows, blacklist_uids=None):
    blacklist_uids = blacklist_uids or set()

    user_base = {}
    user_long_map = defaultdict(lambda: {
        "present_count": 0,
        "active_count": 0,
        "danmu_total": 0,
        "gift_count_total": 0,
        "gift_total": 0.0,
        "first_send_date": "",
        "last_send_date": "",
    })

    filtered_detail_count = 0

    for row in detail_rows:
        uid = safe_str(row.get("uid"))
        if not uid:
            continue

        if uid in blacklist_uids:
            filtered_detail_count += 1
            continue

        uname = safe_str(row.get("uname"))
        host = safe_str(row.get("host")) or "未知"
        live_type = safe_str(row.get("live_type")) or "其他"
        live_name = safe_str(row.get("live_name"))
        first_send_date = safe_str(row.get("first_send_date"))
        last_send_date = safe_str(row.get("last_send_date"))

        is_present = safe_int(row.get("is_present"))
        is_active = safe_int(row.get("is_active"))
        danmu_count = safe_int(row.get("danmu_count"))
        gift_count = safe_int(row.get("gift_count"))
        gift_amount = safe_float(row.get("gift_amount"))

        if uid not in user_base:
            user_base[uid] = {
                "uid": uid,
                "uname": uname,
                "total_present_count": 0,
                "total_active_count": 0,
                "total_danmu_count": 0,
                "total_gift_count": 0,
                "total_gift_amount": 0.0,
                "first_seen_send_date": first_send_date,
                "first_seen_live_name": live_name,
                "first_seen_host": host,
                "first_seen_live_type": live_type,
                "last_seen_send_date": last_send_date,
            }
        else:
            if uname and not user_base[uid]["uname"]:
                user_base[uid]["uname"] = uname

            old_first = user_base[uid]["first_seen_send_date"]
            if first_send_date and (not old_first or first_send_date < old_first):
                user_base[uid]["first_seen_send_date"] = first_send_date
                user_base[uid]["first_seen_live_name"] = live_name
                user_base[uid]["first_seen_host"] = host
                user_base[uid]["first_seen_live_type"] = live_type

            old_last = user_base[uid]["last_seen_send_date"]
            if last_send_date and (not old_last or last_send_date > old_last):
                user_base[uid]["last_seen_send_date"] = last_send_date

        user_base[uid]["total_present_count"] += is_present
        user_base[uid]["total_active_count"] += is_active
        user_base[uid]["total_danmu_count"] += danmu_count
        user_base[uid]["total_gift_count"] += gift_count
        user_base[uid]["total_gift_amount"] += gift_amount

        key = (uid, host, live_type)
        stat = user_long_map[key]

        stat["present_count"] += is_present
        stat["active_count"] += is_active
        stat["danmu_total"] += danmu_count
        stat["gift_count_total"] += gift_count
        stat["gift_total"] += gift_amount

        if first_send_date and (not stat["first_send_date"] or first_send_date < stat["first_send_date"]):
            stat["first_send_date"] = first_send_date

        if last_send_date and (not stat["last_send_date"] or last_send_date > stat["last_send_date"]):
            stat["last_send_date"] = last_send_date

    long_rows = []
    combo_keys = set()

    for (uid, host, live_type), stat in user_long_map.items():
        combo_keys.add((host, live_type))
        long_rows.append({
            "uid": uid,
            "uname": user_base[uid]["uname"],
            "host": host,
            "live_type": live_type,
            "present_count": stat["present_count"],
            "active_count": stat["active_count"],
            "danmu_total": stat["danmu_total"],
            "gift_count_total": stat["gift_count_total"],
            "gift_total": round(stat["gift_total"], 4),
            "first_send_date": stat["first_send_date"],
            "last_send_date": stat["last_send_date"],
        })

    long_rows.sort(key=lambda x: (x["uid"], x["host"], x["live_type"]))
    combo_keys = sorted(combo_keys, key=lambda x: (x[0], x[1]))

    wide_rows = []
    for uid, base in sorted(user_base.items(), key=lambda x: x[0]):
        row = {
            "uid": base["uid"],
            "uname": base["uname"],
            "总到场次数": base["total_present_count"],
            "总有效到场次数": base["total_active_count"],
            "总弹幕数": base["total_danmu_count"],
            "总礼物数": base["total_gift_count"],
            "总礼物金额": round(base["total_gift_amount"], 4),
            "首次出现时间": base["first_seen_send_date"],
            "首次出现直播": base["first_seen_live_name"],
            "首次出现主播": base["first_seen_host"],
            "首次出现类型": base["first_seen_live_type"],
            "最后出现时间": base["last_seen_send_date"],
        }

        for host, live_type in combo_keys:
            prefix = f"{host}_{live_type}"
            row[f"{prefix}_到场次数"] = 0
            row[f"{prefix}_有效到场次数"] = 0
            row[f"{prefix}_弹幕总数"] = 0
            row[f"{prefix}_礼物数"] = 0
            row[f"{prefix}_礼物总额"] = 0.0

        wide_rows.append(row)

    wide_index = {row["uid"]: row for row in wide_rows}

    for item in long_rows:
        uid = item["uid"]
        host = item["host"]
        live_type = item["live_type"]
        prefix = f"{host}_{live_type}"

        wide_index[uid][f"{prefix}_到场次数"] = item["present_count"]
        wide_index[uid][f"{prefix}_有效到场次数"] = item["active_count"]
        wide_index[uid][f"{prefix}_弹幕总数"] = item["danmu_total"]
        wide_index[uid][f"{prefix}_礼物数"] = item["gift_count_total"]
        wide_index[uid][f"{prefix}_礼物总额"] = item["gift_total"]

    return long_rows, wide_rows, combo_keys, filtered_detail_count


def write_csv(rows, output_csv: Path, fieldnames=None):
    if not rows:
        print(f"[WARN] 没有可写入数据: {output_csv}")
        return

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    detail_csv = Path(r"D:\wins\Desktop\as\场次明细表\all_live_details.csv")
    blacklist_csv = Path(r"D:\wins\Desktop\as\黑名单\uid_blacklist.csv")
    output_dir = Path(r"D:\wins\Desktop\as\用户总表")

    print(f"[INFO] 读取明细表: {detail_csv}")
    detail_rows = load_detail_csv(detail_csv)

    if not detail_rows:
        print("[WARN] 明细表为空，结束")
        return

    print(f"[INFO] 读取黑名单: {blacklist_csv}")
    blacklist_uids = load_blacklist_uids(blacklist_csv)

    long_rows, wide_rows, combo_keys, filtered_detail_count = build_user_summary(
        detail_rows,
        blacklist_uids=blacklist_uids,
    )

    long_csv = output_dir / "user_summary_long_clean.csv"
    wide_csv = output_dir / "user_summary_wide_clean.csv"

    long_fields = [
        "uid",
        "uname",
        "host",
        "live_type",
        "present_count",
        "active_count",
        "danmu_total",
        "gift_count_total",
        "gift_total",
        "first_send_date",
        "last_send_date",
    ]

    base_fields = [
        "uid",
        "uname",
        "总到场次数",
        "总有效到场次数",
        "总弹幕数",
        "总礼物数",
        "总礼物金额",
        "首次出现时间",
        "首次出现直播",
        "首次出现主播",
        "首次出现类型",
        "最后出现时间",
    ]

    dynamic_fields = []
    for host, live_type in combo_keys:
        prefix = f"{host}_{live_type}"
        dynamic_fields.extend([
            f"{prefix}_到场次数",
            f"{prefix}_有效到场次数",
            f"{prefix}_弹幕总数",
            f"{prefix}_礼物数",
            f"{prefix}_礼物总额",
        ])

    wide_fields = base_fields + dynamic_fields

    write_csv(long_rows, long_csv, long_fields)
    write_csv(wide_rows, wide_csv, wide_fields)

    print(f"[OK] 已生成长表: {long_csv}")
    print(f"[OK] 已生成宽表: {wide_csv}")
    print(f"[OK] 黑名单UID数: {len(blacklist_uids)}")
    print(f"[OK] 被过滤明细行数: {filtered_detail_count}")
    print(f"[OK] 清洗后用户数: {len(wide_rows)}")
    print(f"[OK] 主播-类型组合数: {len(combo_keys)}")


if __name__ == "__main__":
    main()
