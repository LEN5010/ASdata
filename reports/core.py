from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DETAIL_CSV = ROOT / "场次明细表" / "all_live_details.csv"
BLACKLIST_CSV = ROOT / "黑名单" / "uid_blacklist.csv"


def safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_int(value, default=0) -> int:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        return int(float(text))
    except Exception:
        return default


def safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if text == "":
            return default
        return float(text)
    except Exception:
        return default


def fmt_pct(numerator, denominator) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def fmt_date(ts_ms: int) -> str:
    if not ts_ms:
        return ""
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")


def load_csv_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(rows, output_path: Path, fieldnames=None):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None and rows:
        fieldnames = list(rows[0].keys())
    if not fieldnames:
        return
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_detail_rows():
    return load_csv_rows(DETAIL_CSV)


def load_blacklist_uids():
    if not BLACKLIST_CSV.exists():
        return set()
    uids = set()
    for row in load_csv_rows(BLACKLIST_CSV):
        uid = safe_str(row.get("uid"))
        if uid:
            uids.add(uid)
    return uids


def is_valid_uid(uid: str) -> bool:
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

        session_ts = safe_int(row.get("start_date"), 0) or safe_int(row.get("first_send_date"), 0)
        first_send_ts = safe_int(row.get("first_send_date"), 0) or session_ts
        last_send_ts = safe_int(row.get("last_send_date"), 0) or session_ts

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


def build_single_pass_exclusion_uids(records, source_live, session_key):
    if not session_key:
        return set()

    records_by_uid = defaultdict(list)
    for record in records:
        if record["is_present"] >= 1:
            records_by_uid[record["uid"]].append(record)

    exclude_uids = set()
    for uid, user_records in records_by_uid.items():
        source_records = [record for record in user_records if record["target_live"] == source_live]
        if not source_records:
            continue

        source_records.sort(key=lambda x: (x["session_ts"], x["target_live"], x["session_key"]))
        source_first = source_records[0]
        if source_first["session_key"] != session_key:
            continue
        if len(source_records) != 1:
            continue

        source_anchor = (
            source_first["session_ts"],
            source_first["target_live"],
            source_first["session_key"],
        )
        has_later_presence = any(
            (record["session_ts"], record["target_live"], record["session_key"]) > source_anchor
            for record in user_records
        )
        if not has_later_presence:
            exclude_uids.add(uid)

    return exclude_uids
