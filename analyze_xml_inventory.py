import argparse
import csv
import os
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parent
XML_ROOT = ROOT / "XML弹幕文件"
OUTPUT_ROOT = ROOT / "分析结果" / "XML全库摸底"

FILE_FIELDS = [
    "file_name",
    "file_path",
    "file_size_mb",
    "date",
    "year",
    "title_rest",
    "host_guess",
    "host_guess_reason",
    "is_multi_host",
    "live_type_guess",
    "live_type_reason",
    "has_uid_attr",
    "has_user_attr",
    "has_raw_attr",
    "has_raw_uid",
    "identity_mode",
    "identity_reason",
    "ingestion_readiness",
    "sample_d_nodes",
    "needs_manual_review",
    "manual_reason",
]

DATE_RE = re.compile(r"^(?P<date>\d{4}\.\d{2}\.\d{2})\s+(?P<title>.+)$")
DM_RE = re.compile(r"<d\b([^>]*)>")
UID_RE = re.compile(r'\suid="([^"]+)"')
USER_RE = re.compile(r'\suser="([^"]+)"')
RAW_RE = re.compile(r'\sraw="([^"]+)"')
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
EXCLUDED_FILES = {
    "2022.05.30 军训工具人.xml",
}
SPECIAL_GROUP_TITLES = {
    "我们一起跨年叭~.xml",
    "愚人节“人生有梦”歌舞会.xml",
    "五一特别企划.xml",
}
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


def parse_args():
    parser = argparse.ArgumentParser(description="扫描 XML 弹幕全库，摸底文件命名、归属和 UID 可提取性")
    parser.add_argument("--xml-root", default=str(XML_ROOT), help="XML 文件目录")
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT), help="输出目录")
    parser.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1), help="并发线程数")
    parser.add_argument("--sample-d-nodes", type=int, default=20, help="每个文件最多采样多少个 <d> 节点")
    parser.add_argument("--chunk-bytes", type=int, default=256 * 1024, help="单次读取字节数")
    parser.add_argument("--max-bytes", type=int, default=2 * 1024 * 1024, help="每个文件最多扫描多少字节")
    return parser.parse_args()


def parse_filename(file_name: str):
    match = DATE_RE.match(file_name)
    if not match:
        return "", "", file_name
    date = match.group("date")
    title = match.group("title")
    return date, date[:4], title


def guess_host(title: str):
    if title in SPECIAL_GROUP_TITLES:
        return "团体/官方", "special_group_file", 0

    hit_members = [name for name in MEMBER_NAMES if name in title]
    hit_groups = [hint for hint in GROUP_HINTS if hint in title]
    hit_group_types = [hint for hint in GROUP_TYPE_HINTS if hint in title]

    if len(hit_members) >= 2:
        first_member = min(
            hit_members,
            key=lambda name: (title.find(name), MEMBER_NAMES.index(name)),
        )
        return first_member, f"first_member_keyword:{first_member}|all:{'|'.join(hit_members)}", 1
    if len(hit_members) == 1:
        return hit_members[0], f"member_keyword:{hit_members[0]}", 0
    if hit_groups:
        return "团体/官方", f"group_keyword:{hit_groups[0]}", 0
    if hit_group_types:
        return "团体/官方", f"group_type_keyword:{hit_group_types[0]}", 0
    return "待定", "no_host_keyword", 0


def guess_live_type(title: str):
    for keyword, live_type in TYPE_RULES:
        if keyword in title:
            return live_type, f"type_keyword:{keyword}"
    return "待定", "no_type_keyword"


def scan_xml_sample(file_path: Path, sample_limit: int, chunk_bytes: int, max_bytes: int):
    sample_count = 0
    has_uid_attr = False
    has_user_attr = False
    has_raw_attr = False
    has_raw_uid = False
    carry = ""
    bytes_read = 0

    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        while bytes_read < max_bytes and sample_count < sample_limit:
            chunk = f.read(chunk_bytes)
            if not chunk:
                break
            bytes_read += len(chunk.encode("utf-8", errors="ignore"))
            text = carry + chunk
            matches = list(DM_RE.finditer(text))
            if not matches:
                carry = text[-4096:]
                continue

            consume_count = 0
            for match in matches:
                attrs = match.group(1)
                sample_count += 1
                consume_count += 1

                if UID_RE.search(attrs):
                    has_uid_attr = True
                if USER_RE.search(attrs):
                    has_user_attr = True

                raw_match = RAW_RE.search(attrs)
                if raw_match:
                    has_raw_attr = True
                    raw_value = raw_match.group(1)
                    if RAW_UID_RE.search(raw_value):
                        has_raw_uid = True

                if sample_count >= sample_limit:
                    break

            if matches:
                carry = text[matches[-1].start():][-4096:]
            else:
                carry = text[-4096:]

    if has_uid_attr:
        identity_mode = "direct_uid"
        identity_reason = "d_node_has_uid_attr"
    elif has_raw_uid:
        identity_mode = "raw_uid"
        identity_reason = "d_node_raw_contains_uid"
    elif has_user_attr:
        identity_mode = "user_only"
        identity_reason = "d_node_has_user_name_only"
    else:
        identity_mode = "anonymous"
        identity_reason = "sampled_d_nodes_have_no_uid_or_user"

    return {
        "has_uid_attr": int(has_uid_attr),
        "has_user_attr": int(has_user_attr),
        "has_raw_attr": int(has_raw_attr),
        "has_raw_uid": int(has_raw_uid),
        "identity_mode": identity_mode,
        "identity_reason": identity_reason,
        "sample_d_nodes": sample_count,
    }


def analyze_one_file(file_path: Path, sample_limit: int, chunk_bytes: int, max_bytes: int):
    date, year, title = parse_filename(file_path.name)
    host_guess, host_reason, is_multi_host = guess_host(title)
    live_type_guess, live_type_reason = guess_live_type(title)
    xml_scan = scan_xml_sample(file_path, sample_limit, chunk_bytes, max_bytes)

    if host_guess in MEMBER_NAMES:
        if xml_scan["identity_mode"] in {"direct_uid", "raw_uid"}:
            ingestion_readiness = "single_uid_ready"
        elif xml_scan["identity_mode"] == "user_only":
            ingestion_readiness = "single_user_only"
        else:
            ingestion_readiness = "single_anonymous"
    elif host_guess == "团体/官方":
        ingestion_readiness = "group_or_official"
    else:
        ingestion_readiness = "host_unknown"

    manual_reasons = []
    if not date:
        manual_reasons.append("filename_date_unparsed")
    if host_guess == "待定":
        manual_reasons.append(f"host:{host_guess}")
    if xml_scan["sample_d_nodes"] == 0:
        manual_reasons.append("no_d_node_sampled")
    if ingestion_readiness == "single_anonymous":
        manual_reasons.append("identity:anonymous")

    return {
        "file_name": file_path.name,
        "file_path": str(file_path.relative_to(ROOT)),
        "file_size_mb": round(file_path.stat().st_size / 1024 / 1024, 3),
        "date": date,
        "year": year,
        "title_rest": title,
        "host_guess": host_guess,
        "host_guess_reason": host_reason,
        "is_multi_host": is_multi_host,
        "live_type_guess": live_type_guess,
        "live_type_reason": live_type_reason,
        **xml_scan,
        "ingestion_readiness": ingestion_readiness,
        "needs_manual_review": int(bool(manual_reasons)),
        "manual_reason": ";".join(manual_reasons),
    }


def write_csv(csv_path: Path, rows, fieldnames):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_year_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["year"] or "未知"].append(row)

    summary_rows = []
    for year in sorted(grouped):
        items = grouped[year]
        identity_counter = Counter(row["identity_mode"] for row in items)
        summary_rows.append({
            "year": year,
            "file_count": len(items),
            "manual_review_count": sum(row["needs_manual_review"] for row in items),
            "single_host_count": sum(row["host_guess"] in MEMBER_NAMES for row in items),
            "multi_host_count": sum(row["is_multi_host"] == 1 for row in items),
            "group_host_count": sum(row["host_guess"] == "团体/官方" for row in items),
            "unknown_host_count": sum(row["host_guess"] == "待定" for row in items),
            "has_uid_attr_count": sum(row["has_uid_attr"] for row in items),
            "has_user_attr_count": sum(row["has_user_attr"] for row in items),
            "has_raw_uid_count": sum(row["has_raw_uid"] for row in items),
            "direct_uid_count": identity_counter["direct_uid"],
            "raw_uid_count": identity_counter["raw_uid"],
            "user_only_count": identity_counter["user_only"],
            "anonymous_count": identity_counter["anonymous"],
        })
    return summary_rows


def build_host_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["host_guess"]].append(row)

    summary_rows = []
    for host in sorted(grouped):
        items = grouped[host]
        identity_counter = Counter(row["identity_mode"] for row in items)
        type_counter = Counter(row["live_type_guess"] for row in items)
        summary_rows.append({
            "host_guess": host,
            "file_count": len(items),
            "manual_review_count": sum(row["needs_manual_review"] for row in items),
            "top_live_type": type_counter.most_common(1)[0][0] if type_counter else "",
            "has_uid_attr_count": sum(row["has_uid_attr"] for row in items),
            "has_user_attr_count": sum(row["has_user_attr"] for row in items),
            "has_raw_uid_count": sum(row["has_raw_uid"] for row in items),
            "direct_uid_count": identity_counter["direct_uid"],
            "raw_uid_count": identity_counter["raw_uid"],
            "user_only_count": identity_counter["user_only"],
            "anonymous_count": identity_counter["anonymous"],
        })
    return summary_rows


def build_identity_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["identity_mode"]].append(row)

    summary_rows = []
    for identity_mode in sorted(grouped):
        items = grouped[identity_mode]
        summary_rows.append({
            "identity_mode": identity_mode,
            "file_count": len(items),
            "manual_review_count": sum(row["needs_manual_review"] for row in items),
            "single_host_count": sum(row["host_guess"] in MEMBER_NAMES for row in items),
            "multi_or_group_count": sum(
                row["is_multi_host"] == 1 or row["host_guess"] == "团体/官方"
                for row in items
            ),
        })
    return summary_rows


def build_readiness_summary(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["ingestion_readiness"]].append(row)

    summary_rows = []
    for readiness in sorted(grouped):
        items = grouped[readiness]
        identity_counter = Counter(row["identity_mode"] for row in items)
        summary_rows.append({
            "ingestion_readiness": readiness,
            "file_count": len(items),
            "manual_review_count": sum(row["needs_manual_review"] for row in items),
            "direct_uid_count": identity_counter["direct_uid"],
            "raw_uid_count": identity_counter["raw_uid"],
            "user_only_count": identity_counter["user_only"],
            "anonymous_count": identity_counter["anonymous"],
        })
    return summary_rows


def analyze_all_files(xml_root: Path, workers: int, sample_limit: int, chunk_bytes: int, max_bytes: int):
    xml_files = sorted(
        path for path in xml_root.glob("*.xml")
        if path.name not in EXCLUDED_FILES
    )
    print(f"[INFO] XML 文件数: {len(xml_files)}")
    if not xml_files:
        return []

    rows = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(analyze_one_file, path, sample_limit, chunk_bytes, max_bytes)
            for path in xml_files
        ]
        for index, future in enumerate(futures, start=1):
            rows.append(future.result())
            if index % 200 == 0 or index == len(futures):
                print(f"[INFO] 已分析 {index}/{len(futures)} 个 XML")
    rows.sort(key=lambda row: (row["date"], row["file_name"]))
    return rows


def main():
    args = parse_args()
    xml_root = Path(args.xml_root)
    output_root = Path(args.output_root)

    rows = analyze_all_files(
        xml_root=xml_root,
        workers=args.workers,
        sample_limit=args.sample_d_nodes,
        chunk_bytes=args.chunk_bytes,
        max_bytes=args.max_bytes,
    )

    file_inventory_path = output_root / "file_inventory.csv"
    summary_year_path = output_root / "summary_by_year.csv"
    summary_host_path = output_root / "summary_by_host_guess.csv"
    summary_identity_path = output_root / "summary_by_identity_mode.csv"
    summary_readiness_path = output_root / "summary_by_readiness.csv"
    manual_review_path = output_root / "manual_review.csv"

    write_csv(file_inventory_path, rows, FILE_FIELDS)
    write_csv(summary_year_path, build_year_summary(rows), [
        "year",
        "file_count",
        "manual_review_count",
        "single_host_count",
        "multi_host_count",
        "group_host_count",
        "unknown_host_count",
        "has_uid_attr_count",
        "has_user_attr_count",
        "has_raw_uid_count",
        "direct_uid_count",
        "raw_uid_count",
        "user_only_count",
        "anonymous_count",
    ])
    write_csv(summary_host_path, build_host_summary(rows), [
        "host_guess",
        "file_count",
        "manual_review_count",
        "top_live_type",
        "has_uid_attr_count",
        "has_user_attr_count",
        "has_raw_uid_count",
        "direct_uid_count",
        "raw_uid_count",
        "user_only_count",
        "anonymous_count",
    ])
    write_csv(summary_identity_path, build_identity_summary(rows), [
        "identity_mode",
        "file_count",
        "manual_review_count",
        "single_host_count",
        "multi_or_group_count",
    ])
    write_csv(summary_readiness_path, build_readiness_summary(rows), [
        "ingestion_readiness",
        "file_count",
        "manual_review_count",
        "direct_uid_count",
        "raw_uid_count",
        "user_only_count",
        "anonymous_count",
    ])
    write_csv(
        manual_review_path,
        [
            row for row in rows
            if row["needs_manual_review"] and row["identity_mode"] in {"direct_uid", "raw_uid"}
        ],
        FILE_FIELDS,
    )

    identity_counter = Counter(row["identity_mode"] for row in rows)
    print(f"[DONE] 输出目录: {output_root}")
    print(f"[DONE] direct_uid={identity_counter['direct_uid']} raw_uid={identity_counter['raw_uid']} user_only={identity_counter['user_only']} anonymous={identity_counter['anonymous']}")


if __name__ == "__main__":
    main()
