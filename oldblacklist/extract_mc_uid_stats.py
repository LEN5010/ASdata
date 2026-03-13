import argparse
import csv
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool, cpu_count
from pathlib import Path

from extract_uid_blacklist import BiliBiliMidCRC, iter_xml_files


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "as_output"
SOURCE_CONFIGS = {
    "nailin_xml": {
        "host": "乃琳",
        "channel_name": "乃琳Queen",
        "sample_folder": "乃琳样本",
    },
    "jiaran_xml": {
        "host": "嘉然",
        "channel_name": "嘉然今天吃什么",
        "sample_folder": "嘉然样本",
    },
    "beila_xml": {
        "host": "贝拉",
        "channel_name": "贝拉kira",
        "sample_folder": "贝拉样本",
    },
}
DATE_RE = re.compile(r"^(?P<title>.+?)\s+(?P<year>\d{4})年(?P<month>\d{2})月(?P<day>\d{2})日(?P<hour>\d{1,2})点场$")
INVALID_XML_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
CHAT_ID_RE = re.compile(r"<chatid>(.*?)</chatid>")
D_P_RE = re.compile(r'<d\s+p="([^"]+)"')
CN_TZ = timezone(timedelta(hours=8))
DEFAULT_WORKERS = max(1, min(8, cpu_count() or 1))
_CRACKER = None


def init_cracker():
    global _CRACKER
    _CRACKER = BiliBiliMidCRC()


def crack_hash_pair(user_hash):
    return user_hash, _CRACKER.crack(user_hash)


def build_scheduled_start_ms(year, month, day, hour):
    if None in (year, month, day, hour):
        return ""
    return int(datetime(year, month, day, hour, 0, 0, tzinfo=CN_TZ).timestamp() * 1000)


def detect_live_type(text):
    if "团播" in text:
        return "团播"
    if "终末地" in text:
        return "终末地"
    if "鸣潮" in text:
        return "鸣潮"
    if "【3D】" in text or "3D" in text or "3d" in text:
        return "3D"
    if "电台" in text:
        return "电台"
    if "电影" in text:
        return "电影"
    return "其他"


def parse_live_meta(xml_path: Path, source_config):
    stem = xml_path.stem
    match = DATE_RE.match(stem)

    if match:
        raw_title = match.group("title").strip()
        year = int(match.group("year"))
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour = int(match.group("hour"))
    else:
        raw_title = stem
        year = None
        month = None
        day = None
        hour = None

    live_title = raw_title.removeprefix("【直播回放】").strip()
    live_type = detect_live_type(raw_title)
    date_suffix = f"{month:02d}{day:02d}" if month is not None and day is not None else "0000"
    host = source_config["host"]

    return {
        "source_name": stem,
        "host": host,
        "live_type": live_type,
        "sample_folder": source_config["sample_folder"],
        "live_name": f"{host}_{live_type}_{date_suffix}",
        "room_id": "",
        "channel_name": source_config["channel_name"],
        "live_title": live_title,
        "scheduled_start_ms": build_scheduled_start_ms(year, month, day, hour),
    }


def parse_xml_stats(xml_path: Path, source_config):
    live_meta = parse_live_meta(xml_path, source_config)
    chat_id = ""
    live_first_ts = None
    live_last_ts = None
    max_video_offset_ms = 0
    estimated_live_start_ms = None
    hash_stats = defaultdict(lambda: {"danmu_count": 0, "first_send_date": None, "last_send_date": None})

    try:
        xml_text = xml_path.read_text(encoding="utf-8", errors="replace")
        xml_text = INVALID_XML_CHAR_RE.sub("", xml_text)

        chat_id_match = CHAT_ID_RE.search(xml_text)
        if chat_id_match:
            chat_id = chat_id_match.group(1).strip()

        for match in D_P_RE.finditer(xml_text):
            p = match.group(1)
            parts = p.split(",")
            if len(parts) < 7:
                continue

            try:
                video_offset_ms = int(float(parts[0]) * 1000)
            except ValueError:
                video_offset_ms = 0

            if video_offset_ms > max_video_offset_ms:
                max_video_offset_ms = video_offset_ms

            try:
                send_ts_ms = int(parts[4]) * 1000
            except ValueError:
                continue

            live_start_candidate = send_ts_ms - video_offset_ms
            if estimated_live_start_ms is None or live_start_candidate < estimated_live_start_ms:
                estimated_live_start_ms = live_start_candidate

            user_hash = parts[6].strip().lower()
            if not user_hash:
                continue

            stats = hash_stats[user_hash]
            stats["danmu_count"] += 1

            if stats["first_send_date"] is None or send_ts_ms < stats["first_send_date"]:
                stats["first_send_date"] = send_ts_ms
            if stats["last_send_date"] is None or send_ts_ms > stats["last_send_date"]:
                stats["last_send_date"] = send_ts_ms

            if live_first_ts is None or send_ts_ms < live_first_ts:
                live_first_ts = send_ts_ms
            if live_last_ts is None or send_ts_ms > live_last_ts:
                live_last_ts = send_ts_ms
    except Exception as exc:
        raise RuntimeError(f"解析失败: {xml_path} | {exc}") from exc

    live_meta["live_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{live_meta['source_name']}|{chat_id}"))
    live_meta["start_date"] = estimated_live_start_ms or live_meta["scheduled_start_ms"] or live_first_ts or ""
    if live_meta["start_date"] and max_video_offset_ms:
        live_meta["stop_date"] = live_meta["start_date"] + max_video_offset_ms
    else:
        live_meta["stop_date"] = live_last_ts or ""
    return live_meta, hash_stats


def load_hash_cache(paths, include_missing=False):
    cache = {}
    for path in paths:
        csv_path = Path(path)
        if not csv_path.exists():
            continue
        with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            if not reader.fieldnames or "hash" not in reader.fieldnames or "uid" not in reader.fieldnames:
                continue
            for row in reader:
                user_hash = row["hash"].strip().lower()
                uid = row["uid"].strip() or None
                if not include_missing and uid is None:
                    continue
                if user_hash and user_hash not in cache:
                    cache[user_hash] = uid
    return cache


def collect_live_entries(selected_sources=None):
    live_entries = []
    all_hashes = set()
    selected_names = selected_sources or list(SOURCE_CONFIGS.keys())

    for source_name in selected_names:
        source_config = SOURCE_CONFIGS.get(source_name)
        if source_config is None:
            print(f"[WARN] 未知 source 目录，已跳过: {source_name}")
            continue

        source_dir = ROOT / source_name
        if not source_dir.exists():
            print(f"[WARN] source 目录不存在，已跳过: {source_dir}")
            continue

        xml_files = sorted(Path(path) for path in iter_xml_files(str(source_dir)))
        print(f"[INFO] {source_name} XML 文件数: {len(xml_files)}")

        for idx, xml_path in enumerate(xml_files, 1):
            live_meta, hash_stats = parse_xml_stats(xml_path, source_config)
            live_entries.append((live_meta, hash_stats))
            all_hashes.update(hash_stats.keys())

            if idx % 20 == 0 or idx == len(xml_files):
                print(f"[INFO] {source_name} 已处理 {idx}/{len(xml_files)} 个 XML")

    return live_entries, all_hashes


def resolve_hashes(all_hashes, preload_cache=None, crack_missing=True, workers=DEFAULT_WORKERS):
    crack_cache = dict(preload_cache or {})
    pending_hashes = sorted(user_hash for user_hash in all_hashes if user_hash not in crack_cache)

    if not pending_hashes:
        return crack_cache

    print(f"[INFO] 待处理新增 hash 数: {len(pending_hashes)}")

    if not crack_missing:
        for user_hash in pending_hashes:
            crack_cache[user_hash] = None
        return crack_cache

    workers = max(1, int(workers))
    print(f"[INFO] 开始硬解 hash，进程数: {workers}")

    if workers == 1:
        cracker = BiliBiliMidCRC()
        for idx, user_hash in enumerate(pending_hashes, 1):
            crack_cache[user_hash] = cracker.crack(user_hash)
            if idx % 1000 == 0 or idx == len(pending_hashes):
                print(f"[INFO] 已硬解 {idx}/{len(pending_hashes)} 个新增 hash")
        return crack_cache

    with Pool(processes=workers, initializer=init_cracker) as pool:
        for idx, (user_hash, uid) in enumerate(pool.imap_unordered(crack_hash_pair, pending_hashes, chunksize=100), 1):
            crack_cache[user_hash] = uid
            if idx % 1000 == 0 or idx == len(pending_hashes):
                print(f"[INFO] 已硬解 {idx}/{len(pending_hashes)} 个新增 hash")

    return crack_cache


def build_rows(live_entries, crack_cache):
    rows = []
    unresolved = defaultdict(lambda: {"xml_count": 0, "danmu_count": 0})

    for live_meta, hash_stats in live_entries:
        for user_hash, stats in hash_stats.items():
            uid = crack_cache.get(user_hash)
            if uid is None:
                unresolved[user_hash]["xml_count"] += 1
                unresolved[user_hash]["danmu_count"] += stats["danmu_count"]
                continue

            rows.append([
                uid,
                "",
                live_meta["host"],
                live_meta["live_type"],
                live_meta["sample_folder"],
                live_meta["live_name"],
                live_meta["live_id"],
                live_meta["room_id"],
                live_meta["channel_name"],
                live_meta["live_title"],
                live_meta["start_date"],
                live_meta["stop_date"],
                stats["danmu_count"],
                0,
                0.0,
                stats["first_send_date"],
                stats["last_send_date"],
                1,
                0,
            ])

    rows.sort(key=lambda row: (row[10], row[2], row[5], row[0]))
    return rows, unresolved, crack_cache


def write_main_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8-sig") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow([
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
        ])
        writer.writerows(rows)


def write_unresolved_csv(path, unresolved):
    with path.open("w", newline="", encoding="utf-8-sig") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(["hash", "xml_count", "danmu_count"])
        for user_hash, stats in sorted(unresolved.items(), key=lambda item: (-item[1]["xml_count"], -item[1]["danmu_count"], item[0])):
            writer.writerow([user_hash, stats["xml_count"], stats["danmu_count"]])


def write_hash_uid_cache(path, crack_cache, relevant_hashes=None):
    hash_keys = sorted(relevant_hashes if relevant_hashes is not None else crack_cache.keys())
    with path.open("w", newline="", encoding="utf-8-sig") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(["hash", "uid", "status"])
        for user_hash in hash_keys:
            uid = crack_cache.get(user_hash)
            writer.writerow([user_hash, uid or "", "ok" if uid else "missing"])


def main():
    parser = argparse.ArgumentParser(description="统计 oldblacklist 下多目录 XML 中每个 UID 的单场弹幕数据")
    parser.add_argument(
        "--source-dir",
        action="append",
        dest="source_dirs",
        help="指定要处理的 source 目录名，可重复传入；默认处理 nailin_xml、jiaran_xml、beila_xml",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"输出目录，默认 {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument("--skip-crack", action="store_true", help="只使用已有 hash->uid 映射缓存，不反解新增 hash")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"硬解进程数，默认 {DEFAULT_WORKERS}")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    preload_cache = load_hash_cache([
        output_dir / "hash_uid_map.csv",
        ROOT / "output" / "hash_uid_map.csv",
        ROOT / "as_output" / "hash_uid_map.csv",
    ], include_missing=False)
    print(f"[INFO] 载入已有 hash 映射数: {len(preload_cache)}")

    live_entries, all_hashes = collect_live_entries(selected_sources=args.source_dirs)
    print(f"[INFO] 汇总场次数: {len(live_entries)}")
    print(f"[INFO] 汇总唯一 hash 数: {len(all_hashes)}")

    crack_cache = resolve_hashes(
        all_hashes,
        preload_cache=preload_cache,
        crack_missing=not args.skip_crack,
        workers=args.workers,
    )
    rows, unresolved, crack_cache = build_rows(live_entries, crack_cache)

    main_csv_path = output_dir / "uid_live_stats.csv"
    unresolved_csv_path = output_dir / "unresolved_hashes.csv"
    hash_cache_path = output_dir / "hash_uid_map.csv"

    write_main_csv(main_csv_path, rows)
    write_unresolved_csv(unresolved_csv_path, unresolved)
    write_hash_uid_cache(hash_cache_path, crack_cache, relevant_hashes=all_hashes)

    resolved_count = sum(1 for user_hash in all_hashes if crack_cache.get(user_hash) is not None)
    unresolved_count = sum(1 for user_hash in all_hashes if crack_cache.get(user_hash) is None)

    print(f"[DONE] 主结果已输出: {main_csv_path}")
    print(f"[DONE] 未反解 hash 已输出: {unresolved_csv_path}")
    print(f"[DONE] hash 映射缓存已输出: {hash_cache_path}")
    print(f"[INFO] 唯一 hash 反解成功: {resolved_count}")
    print(f"[INFO] 唯一 hash 反解失败: {unresolved_count}")


if __name__ == "__main__":
    main()
