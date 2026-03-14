from datetime import datetime, timedelta, timezone


CN_TZ = timezone(timedelta(hours=8))
SOURCE_LIVE_FIXED = "乃琳_鸣潮"


def cn_ts_ms(text: str) -> int:
    dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
    return int(dt.timestamp() * 1000)


BLACKLIST_WINDOW_START_MS = cn_ts_ms("2025-06-09 00:00:00")
BLACKLIST_WINDOW_END_MS = cn_ts_ms("2025-12-08 23:59:59")
RETURN_START_MS = cn_ts_ms("2025-12-09 00:00:00")
SPIN510_HISTORY_END_MS = cn_ts_ms("2022-06-01 23:59:59")
SPIN510_SILENCE_START_MS = cn_ts_ms("2022-06-02 00:00:00")
BROAD_RETURN_GAP_MS = 365 * 24 * 3600 * 1000
