import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".mplconfig"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".cache"))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


SOURCE_LIVE = "乃琳_鸣潮"
ANALYSIS_DIR = ROOT / "分析结果" / f"{SOURCE_LIVE}_后续承接分析"
PLOTS_DIR = ANALYSIS_DIR / "plots"
SUMMARY_CSV = ANALYSIS_DIR / "01_summary_by_target.csv"
FIRST_TARGET_CSV = ANALYSIS_DIR / "02_first_target.csv"
COHORT_CSV = ANALYSIS_DIR / "03_cohort_by_target.csv"
HOST_SEGMENT_CSV = ANALYSIS_DIR / "04_host_flow_segments.csv"
COHORT_OVERVIEW_CSV = ANALYSIS_DIR / "00_source_cohorts.csv"
INDEX_HTML = PLOTS_DIR / "index.html"


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def sort_summary(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=["post_ge1_rate", "first_target_rate", "target_live"],
        ascending=[False, False, True],
    ).reset_index(drop=True)


def add_barh_labels(ax, bars, values, fmt="{:.1%}", offset=0.005):
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_width() + offset,
            bar.get_y() + bar.get_height() / 2,
            fmt.format(float(value)),
            va="center",
            ha="left",
            fontsize=9,
        )


def plot_overlap_vs_post(df: pd.DataFrame, output_path: Path, source_name: str):
    df = sort_summary(df)
    labels = df["target_live"].tolist()
    overlap = df["overlap_ge1_rate"].to_numpy()
    post = df["post_ge1_rate"].to_numpy()
    active = df["post_active_ge1_rate"].to_numpy()

    y = np.arange(len(labels))
    h = 0.23
    fig_h = max(6, len(labels) * 0.62)
    fig, ax = plt.subplots(figsize=(11, fig_h))

    bars1 = ax.barh(y - h, overlap, height=h, color="#5B8FF9", label="重合率 overlap>=1")
    bars2 = ax.barh(y, post, height=h, color="#61DDAA", label="后续承接率 post>=1")
    bars3 = ax.barh(y + h, active, height=h, color="#F6BD16", label="后续有效承接率")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    max_value = max(np.max(overlap), np.max(post), np.max(active)) if len(labels) else 1
    ax.set_xlim(0, max_value * 1.22 if max_value > 0 else 1)
    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群：重合率 vs 后续承接率", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend()

    add_barh_labels(ax, bars1, overlap)
    add_barh_labels(ax, bars2, post)
    add_barh_labels(ax, bars3, active)

    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_window_chart(df: pd.DataFrame, output_path: Path, source_name: str):
    df = sort_summary(df)
    labels = df["target_live"].tolist()
    d7 = df["post_d7_ge1_rate"].to_numpy()
    d14 = df["post_d14_ge1_rate"].to_numpy()
    d30 = df["post_d30_ge1_rate"].to_numpy()

    y = np.arange(len(labels))
    h = 0.23
    fig_h = max(6, len(labels) * 0.62)
    fig, ax = plt.subplots(figsize=(11, fig_h))

    bars1 = ax.barh(y - h, d7, height=h, color="#91CC75", label="D7")
    bars2 = ax.barh(y, d14, height=h, color="#FAC858", label="D14")
    bars3 = ax.barh(y + h, d30, height=h, color="#EE6666", label="D30")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    max_value = max(np.max(d7), np.max(d14), np.max(d30)) if len(labels) else 1
    ax.set_xlim(0, max_value * 1.22 if max_value > 0 else 1)
    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群：后续承接时间窗（D7 / D14 / D30）", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.legend()

    add_barh_labels(ax, bars1, d7)
    add_barh_labels(ax, bars2, d14)
    add_barh_labels(ax, bars3, d30)

    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_first_target(df: pd.DataFrame, output_path: Path, source_name: str):
    df = df[df["first_target"] != "无后续非source目标"].sort_values(
        by=["user_rate", "first_target"], ascending=[False, True]
    )
    labels = df["first_target"].tolist()
    values = df["user_rate"].to_numpy()

    fig_h = max(5, len(labels) * 0.58)
    fig, ax = plt.subplots(figsize=(10, fig_h))
    y = np.arange(len(labels))
    bars = ax.barh(y, values, color="#5D7092")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    max_value = np.max(values) if len(labels) else 1
    ax.set_xlim(0, max_value * 1.25 if max_value > 0 else 1)
    ax.set_xlabel("比例")
    ax.set_title(f"{source_name} 人群：首次后续去向（first target）", fontsize=14, pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)

    add_barh_labels(ax, bars, values)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_host_segments(df: pd.DataFrame, output_path: Path, source_name: str):
    labels = df["segment"].tolist()
    values = df["user_rate"].to_numpy()
    colors = ["#8D8D8D", "#61DDAA", "#5B8FF9", "#9270CA", "#78D3F8", "#F6BD16", "#E8684A", "#6DC8EC"]

    fig, ax = plt.subplots(figsize=(11, 6))
    x = np.arange(len(labels))
    bars = ax.bar(x, values, color=colors[: len(labels)])

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("比例")
    ax.set_title(f"{source_name} 人群：团内流动分层", fontsize=14, pad=12)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    max_value = np.max(values) if len(labels) else 1
    ax.set_ylim(0, max_value * 1.25 if max_value > 0 else 1)

    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, value, f"{value:.1%}", ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def plot_cohort_heatmap(cohort_df: pd.DataFrame, cohort_overview_df: pd.DataFrame, output_path: Path, source_name: str):
    cohort_order = cohort_overview_df.sort_values(by=["source_session_start", "source_session_name"])
    cohort_labels = cohort_order["source_session_name"].tolist()

    target_order = (
        cohort_df.groupby("target_live", as_index=False)["post_d14_ge1_rate"]
        .max()
        .sort_values(by=["post_d14_ge1_rate", "target_live"], ascending=[False, True])
    )
    target_labels = target_order["target_live"].tolist()

    pivot = cohort_df.pivot_table(
        index="target_live",
        columns="source_session_name",
        values="post_d14_ge1_rate",
        fill_value=0.0,
    )
    pivot = pivot.reindex(index=target_labels, columns=cohort_labels, fill_value=0.0)

    fig_w = max(8, len(cohort_labels) * 1.8)
    fig_h = max(6, len(target_labels) * 0.55)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    im = ax.imshow(pivot.to_numpy(), aspect="auto", cmap="YlGnBu")

    ax.set_xticks(np.arange(len(cohort_labels)))
    ax.set_xticklabels(cohort_labels, rotation=20, ha="right", fontsize=10)
    ax.set_yticks(np.arange(len(target_labels)))
    ax.set_yticklabels(target_labels, fontsize=10)
    ax.set_title(f"{source_name} 各 source cohort 的 D14 后续承接热力图", fontsize=14, pad=12)

    data = pivot.to_numpy()
    vmax = data.max() if data.size else 1
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            value = data[i, j]
            color = "white" if vmax and value / vmax > 0.55 else "#0b1f33"
            ax.text(j, i, f"{value:.1%}", ha="center", va="center", fontsize=8, color=color)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("D14 后续承接率")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close()


def build_index_html(files):
    lines = [
        "<html><head><meta charset='utf-8'><title>乃琳鸣潮后续承接图表</title></head><body>",
        f"<h1>{SOURCE_LIVE} 后续承接图表</h1>",
        "<ul>",
    ]
    for file in files:
        lines.append(f"<li><a href='{file.name}'>{file.name}</a></li>")
    lines.append("</ul></body></html>")
    return "\n".join(lines)


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    summary_df = load_csv(SUMMARY_CSV)
    first_target_df = load_csv(FIRST_TARGET_CSV)
    cohort_df = load_csv(COHORT_CSV)
    host_segment_df = load_csv(HOST_SEGMENT_CSV)
    cohort_overview_df = load_csv(COHORT_OVERVIEW_CSV)

    if summary_df.empty:
        print("[WARN] summary CSV 为空，结束")
        return

    source_name = summary_df.iloc[0].get("source_live", SOURCE_LIVE)
    output_files = [
        PLOTS_DIR / "01_重合率_vs_后续承接率.png",
        PLOTS_DIR / "02_D7_D14_D30后续承接率.png",
        PLOTS_DIR / "03_首次后续去向.png",
        PLOTS_DIR / "04_团内流动分层.png",
        PLOTS_DIR / "05_cohort_D14热力图.png",
    ]

    plot_overlap_vs_post(summary_df, output_files[0], source_name)
    plot_window_chart(summary_df, output_files[1], source_name)
    plot_first_target(first_target_df, output_files[2], source_name)
    plot_host_segments(host_segment_df, output_files[3], source_name)
    plot_cohort_heatmap(cohort_df, cohort_overview_df, output_files[4], source_name)

    INDEX_HTML.write_text(build_index_html(output_files), encoding="utf-8")
    print(f"[OK] 图表已输出到: {PLOTS_DIR}")
    print(f"[OK] 索引页: {INDEX_HTML}")


if __name__ == "__main__":
    main()
