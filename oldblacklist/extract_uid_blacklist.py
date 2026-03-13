#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import argparse
import xml.etree.ElementTree as ET
from collections import defaultdict

CRCPOLYNOMIAL = 0xEDB88320


class BiliBiliMidCRC:
    def __init__(self):
        self.crctable = [0] * 256
        self._create_table()

    def _create_table(self):
        for i in range(256):
            crcreg = i
            for _ in range(8):
                if crcreg & 1:
                    crcreg = CRCPOLYNOMIAL ^ (crcreg >> 1)
                else:
                    crcreg >>= 1
            self.crctable[i] = crcreg & 0xFFFFFFFF

    def crc32(self, input_value):
        s = str(input_value)
        crcstart = 0xFFFFFFFF
        for ch in s:
            index = (crcstart ^ ord(ch)) & 0xFF
            crcstart = ((crcstart >> 8) ^ self.crctable[index]) & 0xFFFFFFFF
        return crcstart

    def crc32_last_index(self, input_value):
        s = str(input_value)
        crcstart = 0xFFFFFFFF
        index = 0
        for ch in s:
            index = (crcstart ^ ord(ch)) & 0xFF
            crcstart = ((crcstart >> 8) ^ self.crctable[index]) & 0xFFFFFFFF
        return index

    def get_crc_index(self, t):
        for i in range(256):
            if ((self.crctable[i] >> 24) & 0xFF) == t:
                return i
        return -1

    def deep_check(self, i, index_list):
        hash_value = self.crc32(i)

        tc = (hash_value & 0xFF) ^ index_list[2]
        if not (48 <= tc <= 57):
            return False, None
        suffix = str(tc - 48)

        hash_value = self.crctable[index_list[2]] ^ (hash_value >> 8)
        hash_value &= 0xFFFFFFFF
        tc = (hash_value & 0xFF) ^ index_list[1]
        if not (48 <= tc <= 57):
            return False, None
        suffix += str(tc - 48)

        hash_value = self.crctable[index_list[1]] ^ (hash_value >> 8)
        hash_value &= 0xFFFFFFFF
        tc = (hash_value & 0xFF) ^ index_list[0]
        if not (48 <= tc <= 57):
            return False, None
        suffix += str(tc - 48)

        return True, suffix

    def crack(self, input_hash):
        try:
            ht = int(input_hash, 16) ^ 0xFFFFFFFF
        except ValueError:
            return None

        index_list = [0] * 4

        for i in range(3, -1, -1):
            index_list[3 - i] = self.get_crc_index((ht >> (i * 8)) & 0xFF)
            if index_list[3 - i] < 0:
                return None
            snum = self.crctable[index_list[3 - i]]
            ht ^= (snum >> ((3 - i) * 8))

        for i in range(100000):
            last_index = self.crc32_last_index(i)
            if last_index == index_list[3]:
                ok, suffix = self.deep_check(i, index_list)
                if ok:
                    return str(i) + suffix

        return None


def iter_xml_files(root_dir):
    for base, _, files in os.walk(root_dir):
        for name in files:
            if name.lower().endswith(".xml"):
                yield os.path.join(base, name)


def extract_hashes_from_xml(xml_path):
    hashes = []
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        print(f"[WARN] 解析失败: {xml_path} | {e}")
        return hashes

    for d in root.findall("d"):
        p = d.attrib.get("p", "")
        if not p:
            continue
        parts = p.split(",")
        if len(parts) < 7:
            continue
        user_hash = parts[6].strip().lower()
        if user_hash:
            hashes.append(user_hash)

    return hashes


def write_hash_uid_map(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["hash", "uid", "xml_count", "danmu_count", "status"])
        for row in rows:
            writer.writerow(row)


def write_uid_blacklist(path, uid_stats):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["uid", "xml_count", "danmu_count"])
        for uid, stats in sorted(uid_stats.items(), key=lambda x: (-x[1]["xml_count"], -x[1]["danmu_count"], x[0])):
            writer.writerow([uid, stats["xml_count"], stats["danmu_count"]])


def main():
    parser = argparse.ArgumentParser(description="批量扫描 B 站弹幕 XML，提取 hash 并反解 uid，输出黑名单")
    parser.add_argument("input_dir", help="XML 根目录")
    parser.add_argument("output_dir", help="输出目录")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    all_hash_danmu_count = defaultdict(int)
    all_hash_xml_files = defaultdict(set)

    xml_files = list(iter_xml_files(args.input_dir))
    print(f"[INFO] 找到 XML 文件数: {len(xml_files)}")

    for idx, xml_path in enumerate(xml_files, 1):
        hashes = extract_hashes_from_xml(xml_path)
        for h in hashes:
            all_hash_danmu_count[h] += 1
            all_hash_xml_files[h].add(xml_path)

        if idx % 100 == 0 or idx == len(xml_files):
            print(f"[INFO] 已处理 {idx}/{len(xml_files)} 个 XML")

    unique_hashes = sorted(all_hash_danmu_count.keys())
    print(f"[INFO] 提取到唯一 hash 数: {len(unique_hashes)}")

    cracker = BiliBiliMidCRC()

    hash_uid_rows = []
    uid_stats = defaultdict(lambda: {"xml_count": 0, "danmu_count": 0})

    for idx, h in enumerate(unique_hashes, 1):
        uid = cracker.crack(h)
        xml_count = len(all_hash_xml_files[h])
        danmu_count = all_hash_danmu_count[h]

        if uid is None:
            hash_uid_rows.append([h, "", xml_count, danmu_count, "failed"])
        else:
            hash_uid_rows.append([h, uid, xml_count, danmu_count, "ok"])
            uid_stats[uid]["xml_count"] += xml_count
            uid_stats[uid]["danmu_count"] += danmu_count

        if idx % 1000 == 0 or idx == len(unique_hashes):
            print(f"[INFO] 已反解 {idx}/{len(unique_hashes)} 个 hash")

    hash_uid_map_path = os.path.join(args.output_dir, "hash_uid_map.csv")
    uid_blacklist_path = os.path.join(args.output_dir, "uid_blacklist.csv")

    write_hash_uid_map(hash_uid_map_path, hash_uid_rows)
    write_uid_blacklist(uid_blacklist_path, uid_stats)

    print(f"[DONE] 映射表已输出: {hash_uid_map_path}")
    print(f"[DONE] 黑名单已输出: {uid_blacklist_path}")


if __name__ == "__main__":
    main()
