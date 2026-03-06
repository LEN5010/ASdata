import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


def safe_float(value, default=np.nan):
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


def load_csv(csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda x: (
            -safe_float(x.get("target_ge1_rate"), 0),
            x.get("target_live", ""),
        )
    )


def add_bar_labels(ax, bars, values, session_counts=None, fmt="{:.1%}", dx=0.005):
    for i, (bar, value) in enumerate(zip(bars, values)):
        if np.isnan(value):
            label = "N/A"
            xpos = 0.01
        else:
            label = fmt.format(value)
            xpos = bar.get_width() + dx

        if session_counts is not None:
            label = f"{label} (样本{session_counts[i]}场)"

        ax.text(
            xpos,
            bar.get_y() + bar.get_height() / 2,
            label,
            va="center",
            ha="left",
            fontsize=9,
        )


def plot_reach_chart(rows, output_path: Path, source_name: str):
    labels = [row["target_live"] for row in rows]
    values = [safe_float(row["target_ge1_rate"], 0.0) for row in rows]
    sessions = [safe_int(row.get("target_total_sessions"), 0) for row in rows]

    fig_height = max(6, len(labels) * 0.55)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    y = np.arange(len(labels))
    bars = ax.barh(y, values, color="#5B8FF9")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values) * 1.25 if values and max(values) > 0 else 1)
    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群目标直播覆盖率（到场>=1）", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)

    add_bar_labels(ax, bars, values, session_counts=sessions)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_retention_chart(rows, output_path: Path, source_name: str):
    labels = [row["target_live"] for row in rows]
    sessions = [safe_int(row.get("target_total_sessions"), 0) for row in rows]

    v1 = [safe_float(row["target_ge1_rate"], 0.0) for row in rows]
    v2 = [
        safe_float(row["target_ge2_rate"], np.nan)
        if safe_int(row.get("target_total_sessions"), 0) >= 2 else np.nan
        for row in rows
    ]
    v3 = [
        safe_float(row["target_ge3_rate"], np.nan)
        if safe_int(row.get("target_total_sessions"), 0) >= 3 else np.nan
        for row in rows
    ]

    fig_height = max(6, len(labels) * 0.6)
    fig, ax = plt.subplots(figsize=(11, fig_height))

    y = np.arange(len(labels))
    h = 0.22

    v2_plot = np.nan_to_num(v2, nan=0.0)
    v3_plot = np.nan_to_num(v3, nan=0.0)

    bars1 = ax.barh(y - h, v1, height=h, color="#91CC75", label="到场>=1")
    bars2 = ax.barh(y, v2_plot, height=h, color="#FAC858", label="到场>=2（仅目标总场次>=2）")
    bars3 = ax.barh(y + h, v3_plot, height=h, color="#EE6666", label="到场>=3（仅目标总场次>=3）")

    ax.set_yticks(y)
    ax.set_yticklabels([f"{label}（{sess}场）" for label, sess in zip(labels, sessions)], fontsize=10)
    ax.invert_yaxis()

    max_candidates = v1 + [x for x in v2 if not np.isnan(x)] + [x for x in v3 if not np.isnan(x)]
    max_value = max(max_candidates) if max_candidates else 1
    ax.set_xlim(0, max_value * 1.22 if max_value > 0 else 1)

    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群多阈值沉淀率（按目标总场次动态展示）", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend()

    add_bar_labels(ax, bars1, v1)
    add_bar_labels(ax, bars2, v2)
    add_bar_labels(ax, bars3, v3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_active_chart(rows, output_path: Path, source_name: str):
    if not rows or "target_active_ge1_rate" not in rows[0]:
        print("[INFO] CSV中无 target_active_ge1_rate，跳过有效承接图")
        return

    labels = [row["target_live"] for row in rows]
    sessions = [safe_int(row.get("target_total_sessions"), 0) for row in rows]
    reach_values = [safe_float(row["target_ge1_rate"], 0.0) for row in rows]
    active_values = [safe_float(row.get("target_active_ge1_rate"), 0.0) for row in rows]

    fig_height = max(6, len(labels) * 0.6)
    fig, ax = plt.subplots(figsize=(11, fig_height))

    y = np.arange(len(labels))
    h = 0.35

    bars1 = ax.barh(y - h / 2, reach_values, height=h, color="#73C0DE", label="到场>=1")
    bars2 = ax.barh(y + h / 2, active_values, height=h, color="#3BA272", label="有效到场>=1")

    ax.set_yticks(y)
    ax.set_yticklabels([f"{label}（{sess}场）" for label, sess in zip(labels, sessions)], fontsize=10)
    ax.invert_yaxis()

    max_value = max(reach_values + active_values) if rows else 1
    ax.set_xlim(0, max_value * 1.22 if max_value > 0 else 1)
    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群覆盖率 vs 有效承接", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend()

    add_bar_labels(ax, bars1, reach_values)
    add_bar_labels(ax, bars2, active_values)

    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def main():
    input_csv = Path(r"D:\wins\Desktop\as\分析结果\conversion_report_乃琳鸣潮.csv")
    output_dir = Path(r"D:\wins\Desktop\as\分析结果\plots_conversion")
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_csv(input_csv)
    if not rows:
        print("[WARN] 输入CSV为空，结束")
        return

    rows = sort_rows(rows)
    source_name = rows[0].get("source_live", "乃琳_鸣潮")

    plot_reach_chart(rows, output_dir / "01_乃琳鸣潮_覆盖率_到场ge1.png", source_name)
    plot_retention_chart(rows, output_dir / "02_乃琳鸣潮_多阈值沉淀率.png", source_name)
    plot_active_chart(rows, output_dir / "03_乃琳鸣潮_覆盖率_vs_有效承接.png", source_name)

    print(f"[OK] 图表已输出到: {output_dir}")


if __name__ == "__main__":
    main()
