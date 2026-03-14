# ASOUL 直播弹幕分析

这个项目用于整理 ASOUL 直播弹幕数据，并生成用于分析“乃琳鸣潮人群后续承接 / 团内流动”的明细表、汇总表、结果表和 SVG 图表。

当前数据来源分为两部分：

- `弹幕JSON/` 中的正式直播 JSON
- `XML弹幕文件/` 中的历史 XML 弹幕回放数据

## 目录

- `弹幕JSON/`
  原始直播弹幕 JSON，按成员 / 类型分文件夹存放。

- `XML弹幕文件/`
  历史直播 XML 弹幕文件，用于补齐早期缺失场次，并统一接入总明细。

- `场次明细表/`
  单场直播聚合后的明细表。
  - `场次明细表/弹幕JSON/`：JSON 单场 `_detail.csv`
  - `场次明细表/XML弹幕文件/`：XML 单场 `_detail.csv`
  - `场次明细表/all_live_details.csv`：JSON + XML 合并后的统一总表

- `用户总表/`
  由场次明细继续汇总得到的用户长表 / 宽表。

- `黑名单/`
  黑名单 UID，用于过滤旧用户。
  当前规则：`2025-06-09 00:00:00` 至 `2025-12-08 23:59:59` 期间发过言的 UID。

- `分析结果/`
  承接分析结果、三人阈值结果和 SVG 图表。
  - `分析结果/乃琳_鸣潮_后续承接分析/`：默认版本
  - `分析结果/乃琳_鸣潮_后续承接分析_去0211单次且无后续/`：排除 `乃琳_鸣潮_0211` 中“只来这一次且之后不再出现”的用户后的版本

## 脚本

- `single_live_to_detail.py`
  读取 `弹幕JSON/` 与 `XML弹幕文件/`，生成单场明细，并合并为 `场次明细表/all_live_details.csv`。

- `build_uid_blacklist.py`
  从 `all_live_details.csv` 中提取 `2025-06-09` 至 `2025-12-08` 发过言的 UID，生成新的 `黑名单/uid_blacklist.csv`。

- `build_user_summary.py`
  根据 `场次明细表/all_live_details.csv` 生成用户长表、宽表，并应用黑名单清洗。

- `build_conversion_report.py`
  以 `乃琳_鸣潮` 为 source，生成后续承接 / 团内流动分析结果和 cohort 表。
  支持输出到自定义分析目录，也支持排除某一场“首次命中 source 且之后再未出现”的一次性用户。

- `plot_conversion_report.py`
  根据承接分析结果生成 SVG 图表。支持读取自定义分析目录。

- `fans_plot.py`
  生成三人维度的解释性阈值摘要表、非零长表和 SVG 图。

- `build_all_reports.py`
  一键串联总明细、黑名单、用户总表、默认承接分析、`0211` 过滤版承接分析和图表产出。

## 推荐流程

推荐直接运行：

```bash
python3 build_all_reports.py
```

这会依次产出：

- `场次明细表/all_live_details.csv`
- `黑名单/uid_blacklist.csv`
- `用户总表/user_summary_wide_clean.csv`
- 默认分析目录 `分析结果/乃琳_鸣潮_后续承接分析/`
- 过滤版目录 `分析结果/乃琳_鸣潮_后续承接分析_去0211单次且无后续/`

如需分步调试，再依次运行：

```bash
python3 single_live_to_detail.py
python3 build_uid_blacklist.py
python3 build_user_summary.py
python3 build_conversion_report.py
python3 plot_conversion_report.py
python3 fans_plot.py
```

如果要生成 `0211` 过滤版，可单独运行：

```bash
python3 build_conversion_report.py \
  --analysis-dir-name '乃琳_鸣潮_后续承接分析_去0211单次且无后续' \
  --exclude-source-single-pass-session-key '32d14305-7ef3-4b90-b261-8d68e754b6b3' \
  --no-legacy-output

python3 plot_conversion_report.py \
  --analysis-dir-name '乃琳_鸣潮_后续承接分析_去0211单次且无后续' \
  --page-title '乃琳_鸣潮_后续承接分析_去0211单次且无后续'
```

## 主要输出

- `场次明细表/all_live_details.csv`
- `黑名单/uid_blacklist.csv`
- `用户总表/user_summary_wide_clean.csv`
- `分析结果/乃琳_鸣潮_后续承接分析/`
- `分析结果/乃琳_鸣潮_后续承接分析_去0211单次且无后续/`
- `分析结果/fans_conversion_三人解释性阈值_摘要.csv`

核心图表位于 `分析结果/乃琳_鸣潮_后续承接分析/plots/`，包括：

- `00_source画像概况.svg`
- `01_重合率_vs_后续承接率.svg`
- `02_source用户进入时间趋势.svg`
- `03_首次后续去向.svg`
- `04_团内流动分层.svg`
- `05_团内流动分层_不含无后续.svg`
- `06_广义回旋_vs_510回旋.svg`
- `07_真实纯新_vs_出现过的老观众.svg`

## 说明

- 当前核心分析口径是：`乃琳_鸣潮` 人群在首次进入 source 之后的后续承接。
- 默认承接分析窗口从 `2025-12-09 00:00:00` 开始；更早数据主要用于黑名单和回旋/纯新判定。
- 过滤版会排除 `乃琳_鸣潮_0211` 里“只来这一场且之后不再出现”的一次性观众，用于观察去掉超大场路过流量后的结构。
- `all_live_details.csv` 已经是统一口径；后续脚本不再单独读取旧版 `KG补充名单/`。
- 图表默认输出为 SVG。
