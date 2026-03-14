from collections import defaultdict

from analysis_settings import (
    RETURN_START_MS,
    SOURCE_LIVE_FIXED,
    SPIN510_HISTORY_END_MS,
    SPIN510_SILENCE_START_MS,
)


def _record_sort_key(record):
    return (
        int(record.get("session_ts", 0) or 0),
        str(record.get("target_live", "")),
        str(record.get("session_key", "")),
    )


def build_user_tag_index(records, source_live=SOURCE_LIVE_FIXED):
    records_by_uid = defaultdict(list)
    for record in records:
        uid = str(record.get("uid", "")).strip()
        session_ts = int(record.get("session_ts", 0) or 0)
        if not uid or session_ts <= 0:
            continue
        records_by_uid[uid].append(record)

    tag_index = {}
    for uid, user_records in records_by_uid.items():
        ordered = sorted(user_records, key=_record_sort_key)
        first_record = ordered[0]
        first_post_return = next(
            (record for record in ordered if int(record.get("session_ts", 0) or 0) >= RETURN_START_MS),
            None,
        )
        pre_return_records = [
            record for record in ordered
            if int(record.get("session_ts", 0) or 0) < RETURN_START_MS
        ]
        last_pre_return = pre_return_records[-1] if pre_return_records else None

        has_pre_510_history = any(
            int(record.get("session_ts", 0) or 0) <= SPIN510_HISTORY_END_MS
            for record in ordered
        )
        has_mid_activity = any(
            SPIN510_SILENCE_START_MS <= int(record.get("session_ts", 0) or 0) < RETURN_START_MS
            for record in ordered
        )

        is_510_return_user = int(
            bool(first_post_return and has_pre_510_history and not has_mid_activity)
        )
        is_broad_return_user = int(bool(first_post_return and last_pre_return))
        is_pure_new_user = int(
            int(first_record.get("session_ts", 0) or 0) >= RETURN_START_MS
            and str(first_record.get("target_live", "")) == source_live
        )

        tag_index[uid] = {
            "uid": uid,
            "first_seen_ts": int(first_record.get("session_ts", 0) or 0),
            "first_seen_target_live": str(first_record.get("target_live", "")),
            "first_post_return_ts": int(first_post_return.get("session_ts", 0) or 0) if first_post_return else 0,
            "first_post_return_target_live": str(first_post_return.get("target_live", "")) if first_post_return else "",
            "last_pre_return_ts": int(last_pre_return.get("session_ts", 0) or 0) if last_pre_return else 0,
            "last_pre_return_target_live": str(last_pre_return.get("target_live", "")) if last_pre_return else "",
            "is_510_return_user": is_510_return_user,
            "is_broad_return_user": is_broad_return_user,
            "is_pure_new_user": is_pure_new_user,
        }

    return tag_index
