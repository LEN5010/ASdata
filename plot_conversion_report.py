from html import escape
from pathlib import Path
import csv


ROOT = Path(__file__).resolve().parent
SOURCE_LIVE = "乃琳_鸣潮"
ANALYSIS_DIR = ROOT / "分析结果" / f"{SOURCE_LIVE}_后续承接分析"
PLOTS_DIR = ANALYSIS_DIR / "plots"
SUMMARY_CSV = ANALYSIS_DIR / "01_summary_by_target.csv"
FIRST_TARGET_CSV = ANALYSIS_DIR / "02_first_target.csv"
HOST_SEGMENT_CSV = ANALYSIS_DIR / "04_host_flow_segments.csv"
INDEX_HTML = PLOTS_DIR / "index.html"
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


def add_rect(lines, x, y, width, height, fill, stroke="none", rx=0):
    lines.append(
        f'<rect x="{x}" y="{y}" width="{max(width, 0)}" height="{height}" fill="{fill}" stroke="{stroke}" rx="{rx}"/>'
    )


def add_line(lines, x1, y1, x2, y2, stroke="#d0d7de", stroke_width=1, dash=False):
    extra = ' stroke-dasharray="4 4"' if dash else ""
    lines.append(
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{stroke_width}"{extra}/>'
    )


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


def plot_overlap_vs_post(rows, output_path: Path, source_name: str):
    rows = sort_summary_rows(rows)
    labels = [row["target_live"] for row in rows]
    series = [
        {"label": "重合率 overlap>=1", "color": "#5B8FF9", "values": [safe_float(row["overlap_ge1_rate"], 0.0) for row in rows]},
        {"label": "后续承接率 post>=1", "color": "#61DDAA", "values": [safe_float(row["post_ge1_rate"], 0.0) for row in rows]},
        {"label": "后续有效承接率", "color": "#F6BD16", "values": [safe_float(row["post_active_ge1_rate"], 0.0) for row in rows]},
    ]
    draw_grouped_hbar(labels, series, f"{source_name} 人群：重合率 vs 后续承接率", output_path)


def plot_first_target(rows, output_path: Path, source_name: str):
    filtered = [row for row in rows if row.get("first_target") != "无后续非source目标"]
    filtered = sorted(filtered, key=lambda x: (-safe_float(x.get("user_rate"), 0.0), x.get("first_target", "")))
    labels = [row["first_target"] for row in filtered]
    series = [{"label": "首次后续去向占比", "color": "#5D7092", "values": [safe_float(row["user_rate"], 0.0) for row in filtered]}]
    draw_grouped_hbar(labels, series, f"{source_name} 人群：首次后续去向（first target）", output_path)


def plot_host_segments(rows, output_path: Path, source_name: str):
    labels = [row["segment"] for row in rows]
    values = [safe_float(row["user_rate"], 0.0) for row in rows]
    colors = ["#8D8D8D", "#61DDAA", "#5B8FF9", "#9270CA", "#78D3F8", "#F6BD16", "#E8684A", "#6DC8EC"]
    draw_vertical_bar(labels, values, colors, f"{source_name} 人群：团内流动分层", output_path)


def build_index_html(files):
    lines = ["<html><head><meta charset='utf-8'><title>乃琳鸣潮后续承接图表</title></head><body>", f"<h1>{escape(SOURCE_LIVE)} 后续承接图表</h1>", "<ul>"]
    for file in files:
        lines.append(f"<li><a href='{escape(file.name)}'>{escape(file.name)}</a></li>")
    lines.append("</ul></body></html>")
    return "\n".join(lines)


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows = load_csv(SUMMARY_CSV)
    first_target_rows = load_csv(FIRST_TARGET_CSV)
    host_segment_rows = load_csv(HOST_SEGMENT_CSV)
    if not summary_rows:
        print("[WARN] summary CSV 为空，结束")
        return
    source_name = summary_rows[0].get("source_live", SOURCE_LIVE)
    output_files = [
        PLOTS_DIR / "01_重合率_vs_后续承接率.svg",
        PLOTS_DIR / "03_首次后续去向.svg",
        PLOTS_DIR / "04_团内流动分层.svg",
    ]
    plot_overlap_vs_post(summary_rows, output_files[0], source_name)
    plot_first_target(first_target_rows, output_files[1], source_name)
    plot_host_segments(host_segment_rows, output_files[2], source_name)
    INDEX_HTML.write_text(build_index_html(output_files), encoding="utf-8")
    print(f"[OK] SVG 图表已输出到: {PLOTS_DIR}")
    print(f"[OK] 索引页: {INDEX_HTML}")


if __name__ == "__main__":
    main()
