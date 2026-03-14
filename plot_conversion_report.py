import argparse
from html import escape
from pathlib import Path
import csv


ROOT = Path(__file__).resolve().parent
SOURCE_LIVE = "乃琳_鸣潮"
DEFAULT_ANALYSIS_DIR_NAME = f"{SOURCE_LIVE}_后续承接分析"
FONT_FAMILY = "'Microsoft YaHei','PingFang SC','Noto Sans CJK SC',sans-serif"


def load_csv(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def fmt_pct(value):
    return f"{value:.1%}"


def fmt_int(value):
    return f"{safe_int(value):,}"


def compact_date_label(text):
    text = str(text or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[5:10]
    return text


def sort_summary_rows(rows):
    return sorted(
        rows,
        key=lambda x: (
            -safe_float(x.get("post_ge1_rate"), 0.0),
            -safe_float(x.get("first_target_rate"), 0.0),
            x.get("target_live", ""),
        ),
    )


def svg_start(width, height):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="white"/>',
    ]


def svg_end(lines, output_path: Path):
    lines.append("</svg>")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def add_text(lines, x, y, text, size=12, anchor="start", weight="normal", fill="#1f2328"):
    lines.append(
        f'<text x="{x}" y="{y}" font-family="{FONT_FAMILY}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" fill="{fill}">{escape(str(text))}</text>'
    )


def add_rotated_text(lines, x, y, text, angle, size=12, anchor="start", weight="normal", fill="#1f2328"):
    lines.append(
        f'<text x="{x}" y="{y}" font-family="{FONT_FAMILY}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" fill="{fill}" '
        f'transform="rotate({angle} {x} {y})">{escape(str(text))}</text>'
    )


def add_rect(lines, x, y, width, height, fill, stroke="none", rx=0):
    lines.append(
        f'<rect x="{x}" y="{y}" width="{max(width, 0)}" height="{height}" fill="{fill}" stroke="{stroke}" rx="{rx}"/>'
    )


def add_circle(lines, cx, cy, r, fill, stroke="none", stroke_width=1):
    lines.append(
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"/>'
    )


def add_line(lines, x1, y1, x2, y2, stroke="#d0d7de", stroke_width=1, dash=False):
    extra = ' stroke-dasharray="4 4"' if dash else ""
    lines.append(
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{stroke_width}"{extra}/>'
    )


def add_polyline(lines, points, color, stroke_width=3):
    if not points:
        return
    point_str = " ".join(f"{x},{y}" for x, y in points)
    lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="{stroke_width}" points="{point_str}"/>')
    for x, y in points:
        lines.append(f'<circle cx="{x}" cy="{y}" r="3.5" fill="{color}"/>')


def scale_linear(value, max_value, span):
    if max_value <= 0:
        return 0
    return value / max_value * span


def draw_legend(lines, items, x, y):
    cursor_x = x
    for color, label in items:
        add_rect(lines, cursor_x, y - 10, 14, 14, color, rx=2)
        add_text(lines, cursor_x + 20, y + 2, label, size=12)
        cursor_x += 20 + len(label) * 14


def draw_grouped_hbar(labels, series, title, output_path: Path):
    left = 250
    right = 120
    top = 95
    bottom = 40
    bar_h = 14
    inner_gap = 5
    group_gap = 16
    row_h = len(series) * bar_h + (len(series) - 1) * inner_gap + group_gap
    width = 1200
    height = top + bottom + len(labels) * row_h
    chart_w = width - left - right
    max_value = max((max(item["values"]) for item in series if item["values"]), default=1)
    max_value = max_value * 1.15 if max_value > 0 else 1

    lines = svg_start(width, height)
    add_text(lines, width / 2, 38, title, size=24, anchor="middle", weight="bold")
    draw_legend(lines, [(item["color"], item["label"]) for item in series], left, 66)

    ticks = 5
    for i in range(ticks + 1):
        value = max_value * i / ticks
        x = left + chart_w * i / ticks
        add_line(lines, x, top - 10, x, height - bottom, dash=True)
        add_text(lines, x, height - 12, fmt_pct(value), size=11, anchor="middle", fill="#57606a")

    for idx, label in enumerate(labels):
        y_group = top + idx * row_h
        label_y = y_group + (len(series) * bar_h + (len(series) - 1) * inner_gap) / 2 + 4
        add_text(lines, left - 12, label_y, label, size=13, anchor="end")

        for s_idx, item in enumerate(series):
            value = item["values"][idx]
            y = y_group + s_idx * (bar_h + inner_gap)
            bar_w = scale_linear(value, max_value, chart_w)
            add_rect(lines, left, y, bar_w, bar_h, item["color"], rx=3)
            add_text(lines, left + bar_w + 8, y + 12, fmt_pct(value), size=11, fill="#57606a")

    svg_end(lines, output_path)


def draw_vertical_bar(labels, values, colors, title, output_path: Path):
    left = 70
    right = 40
    top = 80
    bottom = 120
    width = max(900, len(labels) * 120)
    height = 600
    chart_w = width - left - right
    chart_h = height - top - bottom
    max_value = max(values) if values else 1
    max_value = max_value * 1.18 if max_value > 0 else 1

    lines = svg_start(width, height)
    add_text(lines, width / 2, 36, title, size=24, anchor="middle", weight="bold")

    ticks = 5
    for i in range(ticks + 1):
        value = max_value * i / ticks
        y = top + chart_h - chart_h * i / ticks
        add_line(lines, left, y, width - right, y, dash=True)
        add_text(lines, left - 10, y + 4, fmt_pct(value), size=11, anchor="end", fill="#57606a")

    bar_slot = chart_w / max(len(labels), 1)
    bar_w = min(64, bar_slot * 0.62)
    for idx, (label, value) in enumerate(zip(labels, values)):
        x = left + idx * bar_slot + (bar_slot - bar_w) / 2
        bar_h = scale_linear(value, max_value, chart_h)
        y = top + chart_h - bar_h
        color = colors[idx % len(colors)]
        add_rect(lines, x, y, bar_w, bar_h, color, rx=4)
        add_text(lines, x + bar_w / 2, y - 8, fmt_pct(value), size=11, anchor="middle", fill="#57606a")
        add_text(lines, x + bar_w / 2, height - 56, label, size=12, anchor="middle")

    svg_end(lines, output_path)


def draw_population_composition(segments, title, subtitle, output_path: Path):
    width = 1280
    height = 720
    left = 120
    right = 120
    top = 130
    bar_top = 210
    bar_h = 72
    bottom = 110
    chart_w = width - left - right
    card_y = 360
    card_w = 300
    card_h = 150
    card_gap = 28

    lines = svg_start(width, height)
    add_text(lines, width / 2, 42, title, size=26, anchor="middle", weight="bold")
    add_text(lines, width / 2, 72, subtitle, size=13, anchor="middle", fill="#57606a")

    total_rate = sum(segment["rate"] for segment in segments)
    total_rate = total_rate if total_rate > 0 else 1.0
    total_count = sum(segment["count"] for segment in segments)

    add_text(lines, left, bar_top - 22, f"总人数：{fmt_int(total_count)}", size=16, weight="bold")
    draw_legend(lines, [(segment["color"], segment["label"]) for segment in segments], left, 102)
    cursor_x = left
    for segment in segments:
        seg_w = chart_w * (segment["rate"] / total_rate)
        add_rect(lines, cursor_x, bar_top, seg_w, bar_h, segment["color"], rx=10)
        center_x = cursor_x + seg_w / 2
        if seg_w >= 220:
            add_text(lines, center_x, bar_top + 31, segment["label"], size=18, anchor="middle", weight="bold", fill=segment.get("text_fill", "white"))
            add_text(lines, center_x, bar_top + 54, fmt_pct(segment["rate"]), size=15, anchor="middle", fill=segment.get("text_fill", "white"))
        cursor_x += seg_w

    cursor_x = left
    for idx, segment in enumerate(segments):
        seg_w = chart_w * (segment["rate"] / total_rate)
        if idx > 0:
            add_line(lines, cursor_x, bar_top - 12, cursor_x, bar_top + bar_h + 12, stroke="white", stroke_width=3)
        cursor_x += seg_w

    total_card_w = len(segments) * card_w + max(len(segments) - 1, 0) * card_gap
    card_left = (width - total_card_w) / 2
    for idx, segment in enumerate(segments):
        x = card_left + idx * (card_w + card_gap)
        add_rect(lines, x, card_y, card_w, card_h, segment["card_bg"], stroke="#d0d7de", rx=18)
        add_text(lines, x + 20, card_y + 34, segment["label"], size=16, weight="bold", fill=segment["card_fg"])
        add_text(lines, x + 20, card_y + 82, fmt_int(segment["count"]), size=32, weight="bold", fill="#1f2328")
        add_text(lines, x + 20, card_y + 112, f"占 source 人群 {fmt_pct(segment['rate'])}", size=13, fill="#57606a")
        if segment.get("sub"):
            add_text(lines, x + 20, card_y + 136, segment["sub"], size=12, fill="#57606a")

    svg_end(lines, output_path)


def interpolate_color(ratio, low=(239, 248, 255), high=(8, 81, 156)):
    ratio = max(0.0, min(1.0, ratio))
    rgb = tuple(int(low[i] + (high[i] - low[i]) * ratio) for i in range(3))
    return f"rgb({rgb[0]},{rgb[1]},{rgb[2]})"


def draw_heatmap(row_labels, col_labels, matrix, title, output_path: Path):
    cell_w = 140
    cell_h = 34
    left = 240
    top = 110
    right = 40
    bottom = 40
    width = left + len(col_labels) * cell_w + right
    height = top + len(row_labels) * cell_h + bottom
    max_value = max((max(row) for row in matrix), default=1)
    max_value = max(max_value, 0.0001)

    lines = svg_start(width, height)
    add_text(lines, width / 2, 38, title, size=24, anchor="middle", weight="bold")

    for col_idx, label in enumerate(col_labels):
        x = left + col_idx * cell_w + cell_w / 2
        add_text(lines, x, top - 18, label, size=12, anchor="middle")

    for row_idx, label in enumerate(row_labels):
        y = top + row_idx * cell_h + cell_h / 2 + 5
        add_text(lines, left - 12, y, label, size=12, anchor="end")

    for row_idx, row in enumerate(matrix):
        for col_idx, value in enumerate(row):
            x = left + col_idx * cell_w
            y = top + row_idx * cell_h
            fill = interpolate_color(value / max_value)
            add_rect(lines, x, y, cell_w - 2, cell_h - 2, fill)
            text_fill = "white" if value / max_value > 0.55 else "#0b1f33"
            add_text(lines, x + cell_w / 2, y + cell_h / 2 + 4, fmt_pct(value), size=11, anchor="middle", fill=text_fill)

    svg_end(lines, output_path)


def draw_metric_cards(lines, cards, left, top, card_w, card_h, cols=3, gap_x=24, gap_y=22):
    for idx, card in enumerate(cards):
        row = idx // cols
        col = idx % cols
        x = left + col * (card_w + gap_x)
        y = top + row * (card_h + gap_y)
        add_rect(lines, x, y, card_w, card_h, card["bg"], stroke="#d0d7de", rx=14)
        add_text(lines, x + 18, y + 28, card["label"], size=13, fill="#57606a")
        add_text(lines, x + 18, y + 72, card["value"], size=28, weight="bold", fill=card["fg"])
        if card.get("sub"):
            add_text(lines, x + 18, y + 102, card["sub"], size=12, fill="#57606a")


def plot_source_profile(profile_rows, output_path: Path, source_name: str):
    profile = {row["metric"]: row["value"] for row in profile_rows}
    width = 1280
    height = 760
    lines = svg_start(width, height)
    add_text(lines, width / 2, 40, f"{source_name} 人群：source 画像概况", size=26, anchor="middle", weight="bold")
    add_text(
        lines,
        width / 2,
        68,
        f"分析窗口：{profile.get('analysis_start', '')} ~ {profile.get('analysis_end', '')}",
        size=13,
        anchor="middle",
        fill="#57606a",
    )

    bar_left = 120
    bar_top = 120
    bar_w = 1040
    bar_h = 44
    new_rate = safe_float(profile.get("window_new_user_rate"), 0.0)
    old_rate = safe_float(profile.get("window_old_user_rate"), 0.0)
    new_w = bar_w * new_rate
    old_w = bar_w * old_rate
    add_text(lines, bar_left, bar_top - 18, "source 人群里，窗口内先被 source 捕获的比例", size=16, weight="bold")
    add_rect(lines, bar_left, bar_top, new_w, bar_h, "#61DDAA", rx=8)
    add_rect(lines, bar_left + new_w, bar_top, old_w, bar_h, "#5B8FF9", rx=8)
    add_text(lines, bar_left + 16, bar_top + 29, f"窗口内新用户 {fmt_pct(new_rate)}", size=16, weight="bold", fill="#103d2e")
    add_text(lines, bar_left + new_w + 16, bar_top + 29, f"窗口内旧用户 {fmt_pct(old_rate)}", size=16, weight="bold", fill="#11385f")

    add_circle(lines, 180, 260, 68, fill="#f6f8fa", stroke="#d0d7de", stroke_width=2)
    add_text(lines, 180, 248, "source", size=16, anchor="middle", fill="#57606a")
    add_text(lines, 180, 282, fmt_int(profile.get("source_user_count")), size=30, anchor="middle", weight="bold")

    cards = [
        {"label": "source 有效到场", "value": fmt_pct(safe_float(profile.get("source_active_user_rate"))), "sub": f"{fmt_int(profile.get('source_active_user_count'))} 人", "bg": "#f5fbf8", "fg": "#1a7f4b"},
        {"label": "source 到场≥2 场", "value": fmt_pct(safe_float(profile.get("source_ge2_user_rate"))), "sub": f"{fmt_int(profile.get('source_ge2_user_count'))} 人", "bg": "#f7fbff", "fg": "#0969da"},
        {"label": "source 到场≥3 场", "value": fmt_pct(safe_float(profile.get("source_ge3_user_rate"))), "sub": f"{fmt_int(profile.get('source_ge3_user_count'))} 人", "bg": "#fdf8ff", "fg": "#8250df"},
        {"label": "去过任意非 source", "value": fmt_pct(safe_float(profile.get("post_any_non_source_user_rate"))), "sub": f"{fmt_int(profile.get('post_any_non_source_user_count'))} 人", "bg": "#fff8f2", "fg": "#bc4c00"},
        {"label": "去过嘉然直播", "value": fmt_pct(safe_float(profile.get("post_jiaran_user_rate"))), "sub": f"{fmt_int(profile.get('post_jiaran_user_count'))} 人", "bg": "#f7f8ff", "fg": "#3b5bdb"},
        {"label": "去过贝拉直播", "value": fmt_pct(safe_float(profile.get("post_bella_user_rate"))), "sub": f"{fmt_int(profile.get('post_bella_user_count'))} 人", "bg": "#faf7ff", "fg": "#7a3dd8"},
    ]
    draw_metric_cards(lines, cards, 320, 220, 250, 124, cols=3)
    add_text(lines, 120, 682, "适合视频开场：先讲 source 有多大，再讲其中有多少是在窗口内先被乃琳鸣潮捕获。", size=14, fill="#57606a")
    svg_end(lines, output_path)


def plot_overlap_vs_post(rows, output_path: Path, source_name: str):
    rows = sort_summary_rows(rows)
    labels = [row["target_live"] for row in rows]
    series = [
        {"label": "重合率 overlap>=1", "color": "#5B8FF9", "values": [safe_float(row["overlap_ge1_rate"], 0.0) for row in rows]},
        {"label": "后续承接率 post>=1", "color": "#61DDAA", "values": [safe_float(row["post_ge1_rate"], 0.0) for row in rows]},
        {"label": "后续有效承接率", "color": "#F6BD16", "values": [safe_float(row["post_active_ge1_rate"], 0.0) for row in rows]},
    ]
    draw_grouped_hbar(labels, series, f"{source_name} 人群：重合率 vs 后续承接率", output_path)


def plot_source_timeline(rows, output_path: Path, source_name: str):
    rows = sorted(rows, key=lambda x: safe_int(x.get("source_session_start"), 0))
    cohort_counts = [safe_int(row.get("cohort_user_count"), 0) for row in rows]
    cumulative_counts = []
    total = 0
    for count in cohort_counts:
        total += count
        cumulative_counts.append(total)

    width = max(1200, len(rows) * 56)
    height = 680
    left = 90
    right = 90
    top = 110
    bottom = 170
    chart_w = width - left - right
    chart_h = height - top - bottom
    max_count = max(max(cohort_counts, default=0), max(cumulative_counts, default=0), 1)
    max_count = int(max_count * 1.1)

    lines = svg_start(width, height)
    add_text(lines, width / 2, 38, f"{source_name} 人群：进入时间趋势", size=24, anchor="middle", weight="bold")
    add_text(lines, width / 2, 64, "蓝线=单场首次进入 source 的 cohort 用户数；绿线=累计 source 用户数", size=13, anchor="middle", fill="#57606a")
    add_text(lines, width / 2, 84, "横轴仅保留稀疏日期刻度，避免场次名堆叠", size=12, anchor="middle", fill="#8c959f")
    draw_legend(lines, [("#5B8FF9", "单场 cohort 用户数"), ("#61DDAA", "累计 source 用户数")], left, 88)

    ticks = 5
    for i in range(ticks + 1):
        value = max_count * i / ticks
        y = top + chart_h - chart_h * i / ticks
        add_line(lines, left, y, width - right, y, dash=True)
        add_text(lines, left - 12, y + 4, fmt_int(round(value)), size=11, anchor="end", fill="#57606a")

    x_points = []
    for idx in range(len(rows)):
        x = left + chart_w * idx / max(len(rows) - 1, 1)
        x_points.append(x)
        add_line(lines, x, top, x, top + chart_h, stroke="#eef2f6")

    cohort_points = []
    cumulative_points = []
    for idx, x in enumerate(x_points):
        cohort_y = top + chart_h - scale_linear(cohort_counts[idx], max_count, chart_h)
        cumulative_y = top + chart_h - scale_linear(cumulative_counts[idx], max_count, chart_h)
        cohort_points.append((x, cohort_y))
        cumulative_points.append((x, cumulative_y))

    add_polyline(lines, cohort_points, "#5B8FF9", stroke_width=3)
    add_polyline(lines, cumulative_points, "#61DDAA", stroke_width=3)

    tick_step = max(1, len(rows) // 10)
    for idx, row in enumerate(rows):
        if idx % tick_step != 0 and idx != len(rows) - 1:
            continue
        x = x_points[idx]
        tick_label = compact_date_label(row.get("source_session_start_text", ""))
        add_rotated_text(lines, x, height - 42, tick_label, -40, size=11, anchor="end", fill="#57606a")

    if cohort_points:
        add_text(lines, cohort_points[-1][0] + 12, cohort_points[-1][1] + 4, fmt_int(cohort_counts[-1]), size=12, fill="#5B8FF9")
    if cumulative_points:
        add_text(lines, cumulative_points[-1][0] + 12, cumulative_points[-1][1] + 4, fmt_int(cumulative_counts[-1]), size=12, fill="#1a7f4b")

    svg_end(lines, output_path)


def plot_first_target(rows, output_path: Path, source_name: str):
    renamed = []
    for row in rows:
        copied = dict(row)
        if copied.get("first_target") == "无后续非source目标":
            copied["first_target"] = "留在 源直播间"
        renamed.append(copied)
    renamed = sorted(renamed, key=lambda x: (-safe_float(x.get("user_rate"), 0.0), x.get("first_target", "")))
    labels = [row["first_target"] for row in renamed]
    series = [{"label": "首次后续去向占比", "color": "#5D7092", "values": [safe_float(row["user_rate"], 0.0) for row in renamed]}]
    draw_grouped_hbar(labels, series, f"{source_name} 人群：首次后续去向（含无后续去向）", output_path)


def plot_host_segments(rows, output_path: Path, source_name: str):
    labels = [row["segment"] for row in rows]
    values = [safe_float(row["user_rate"], 0.0) for row in rows]
    colors = ["#8D8D8D", "#61DDAA", "#5B8FF9", "#9270CA", "#78D3F8", "#F6BD16", "#E8684A", "#6DC8EC"]
    draw_vertical_bar(labels, values, colors, f"{source_name} 人群：团内流动分层", output_path)


def plot_host_segments_without_no_follow(rows, output_path: Path, source_name: str):
    filtered_rows = [row for row in rows if row.get("segment") != "无后续到场"]
    retained_total = sum(safe_int(row.get("user_count"), 0) for row in filtered_rows)
    labels = [row["segment"] for row in filtered_rows]
    if retained_total > 0:
        values = [safe_int(row.get("user_count"), 0) / retained_total for row in filtered_rows]
    else:
        values = [0.0 for _ in filtered_rows]
    colors = ["#61DDAA", "#5B8FF9", "#9270CA", "#78D3F8", "#F6BD16", "#E8684A", "#6DC8EC"]
    title = f"{source_name} 人群：团内流动分层（不含无后续，剩余 {fmt_int(retained_total)} 人）"
    draw_vertical_bar(labels, values, colors, title, output_path)


def plot_return_layers(profile_rows, output_path: Path, source_name: str):
    profile = {row["metric"]: row["value"] for row in profile_rows}
    total_count = safe_int(profile.get("source_user_count"), 0)
    spin510_count = safe_int(profile.get("spin510_user_count"), 0)
    broad_return_count = safe_int(profile.get("broad_return_user_count"), 0)
    broad_only_count = max(0, broad_return_count - spin510_count)
    other_count = max(0, total_count - broad_return_count)

    spin510_rate = safe_float(profile.get("spin510_user_rate"), 0.0)
    broad_return_rate = safe_float(profile.get("broad_return_user_rate"), 0.0)
    broad_only_rate = max(0.0, broad_return_rate - spin510_rate)
    other_rate = max(0.0, 1.0 - broad_return_rate)

    segments = [
        {
            "label": "510回旋",
            "count": spin510_count,
            "rate": spin510_rate,
            "color": "#E8684A",
            "text_fill": "white",
            "card_bg": "#fff3ef",
            "card_fg": "#bc4c00",
            "sub": "2022-06-02 到 2025-12-08 消失，2025-12-09 后回归",
        },
        {
            "label": "广义回旋但非510",
            "count": broad_only_count,
            "rate": broad_only_rate,
            "color": "#F6BD16",
            "text_fill": "#4a3400",
            "card_bg": "#fff9e8",
            "card_fg": "#9a6700",
            "sub": "出现前至少一年未参与弹幕，但不满足 510 回旋口径",
        },
        {
            "label": "其他观众",
            "count": other_count,
            "rate": other_rate,
            "color": "#5B8FF9",
            "text_fill": "white",
            "card_bg": "#f4f8ff",
            "card_fg": "#0969da",
            "sub": "不属于广义回旋",
        },
    ]
    subtitle = "把 510 回旋从广义回旋里单独拆出来，避免两者并列时误读为互斥关系"
    draw_population_composition(segments, f"{source_name} 人群：广义回旋 / 510回旋构成", subtitle, output_path)


def plot_true_new_vs_old(profile_rows, output_path: Path, source_name: str):
    profile = {row["metric"]: row["value"] for row in profile_rows}
    segments = [
        {
            "label": "真实纯新观众",
            "count": safe_int(profile.get("pure_new_user_count"), 0),
            "rate": safe_float(profile.get("pure_new_user_rate"), 0.0),
            "color": "#61DDAA",
            "text_fill": "#103d2e",
            "card_bg": "#f3fcf7",
            "card_fg": "#1a7f4b",
            "sub": "首次出现在 2025-12-09 之后，且首次直播即 source",
        },
        {
            "label": "出现过的老观众",
            "count": safe_int(profile.get("window_old_user_count"), 0),
            "rate": safe_float(profile.get("window_old_user_rate"), 0.0),
            "color": "#5B8FF9",
            "text_fill": "white",
            "card_bg": "#f4f8ff",
            "card_fg": "#0969da",
            "sub": "在进入 source 之前，已在当前分析窗口别处出现过",
        },
    ]
    subtitle = "这张图只回答一个问题：乃琳鸣潮里到底有多少是真纯新，有多少是窗口内已经出现过的老观众"
    draw_population_composition(segments, f"{source_name} 人群：真实纯新 vs 出现过的老观众", subtitle, output_path)


def build_index_html(files, page_title):
    lines = [f"<html><head><meta charset='utf-8'><title>{escape(page_title)}</title></head><body>", f"<h1>{escape(page_title)}</h1>", "<ul>"]
    for file in files:
        lines.append(f"<li><a href='{escape(file.name)}'>{escape(file.name)}</a></li>")
    lines.append("</ul></body></html>")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="生成乃琳鸣潮后续承接 SVG 图表")
    parser.add_argument(
        "--analysis-dir-name",
        default=DEFAULT_ANALYSIS_DIR_NAME,
        help="读取 分析结果/ 下的目录名",
    )
    parser.add_argument(
        "--page-title",
        default="",
        help="索引页标题，留空则使用目录名",
    )
    args = parser.parse_args()

    analysis_dir = ROOT / "分析结果" / args.analysis_dir_name
    plots_dir = analysis_dir / "plots"
    cohort_overview_csv = analysis_dir / "00_source_cohorts.csv"
    profile_csv = analysis_dir / "05_source_profile.csv"
    summary_csv = analysis_dir / "01_summary_by_target.csv"
    first_target_csv = analysis_dir / "02_first_target.csv"
    host_segment_csv = analysis_dir / "04_host_flow_segments.csv"
    index_html = plots_dir / "index.html"

    plots_dir.mkdir(parents=True, exist_ok=True)
    cohort_overview_rows = load_csv(cohort_overview_csv)
    profile_rows = load_csv(profile_csv)
    summary_rows = load_csv(summary_csv)
    first_target_rows = load_csv(first_target_csv)
    host_segment_rows = load_csv(host_segment_csv)
    if not summary_rows:
        print("[WARN] summary CSV 为空，结束")
        return
    source_name = summary_rows[0].get("source_live", SOURCE_LIVE)
    output_files = [
        plots_dir / "00_source画像概况.svg",
        plots_dir / "01_重合率_vs_后续承接率.svg",
        plots_dir / "02_source用户进入时间趋势.svg",
        plots_dir / "03_首次后续去向.svg",
        plots_dir / "04_团内流动分层.svg",
        plots_dir / "05_团内流动分层_不含无后续.svg",
        plots_dir / "06_广义回旋_vs_510回旋.svg",
        plots_dir / "07_真实纯新_vs_出现过的老观众.svg",
    ]
    plot_source_profile(profile_rows, output_files[0], source_name)
    plot_overlap_vs_post(summary_rows, output_files[1], source_name)
    plot_source_timeline(cohort_overview_rows, output_files[2], source_name)
    plot_first_target(first_target_rows, output_files[3], source_name)
    plot_host_segments(host_segment_rows, output_files[4], source_name)
    plot_host_segments_without_no_follow(host_segment_rows, output_files[5], source_name)
    plot_return_layers(profile_rows, output_files[6], source_name)
    plot_true_new_vs_old(profile_rows, output_files[7], source_name)
    page_title = args.page_title or f"{args.analysis_dir_name} 图表"
    index_html.write_text(build_index_html(output_files, page_title), encoding="utf-8")
    print(f"[OK] SVG 图表已输出到: {plots_dir}")
    print(f"[OK] 索引页: {index_html}")


if __name__ == "__main__":
    main()
