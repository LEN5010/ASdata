# ASOUL 直播弹幕分析

这个项目用于整理 ASOUL 直播弹幕数据，并生成用于分析“乃琳鸣潮人群后续承接 / 团内流动”的明细表、汇总表、结果表和 SVG 图表。

当前数据来源分为两部分：

- `弹幕JSON/` 中的正式直播 JSON
- `oldblacklist/` 中用于补齐早期缺失场次的 XML 回放数据

## 目录

- `弹幕JSON/`
  原始直播弹幕 JSON，按成员 / 类型分文件夹存放。

- `场次明细表/`
  单场直播聚合后的明细表。
  - `场次明细表/弹幕JSON/`：每场直播对应的 `_detail.csv`
  - `场次明细表/all_live_details.csv`：JSON 明细与 XML 补充明细合并后的统一总表

- `用户总表/`
  由场次明细继续汇总得到的用户长表 / 宽表。

- `黑名单/`
  黑名单 UID，用于过滤老粉或异常用户。

- `oldblacklist/`
  早期 XML 回放及补充明细产物。
  - `oldblacklist/nailin_xml/`：乃琳早期缺失 XML
  - `oldblacklist/jiaran_xml/`：嘉然早期缺失 XML
  - `oldblacklist/beila_xml/`：贝拉早期缺失 XML
  - `oldblacklist/as_output/uid_live_stats.csv`：三目录统一产出的补充明细

- `分析结果/`
  承接分析结果、三人阈值结果和 SVG 图表。

## 脚本

- `oldblacklist/extract_mc_uid_stats.py`
  扫描 `oldblacklist/nailin_xml`、`oldblacklist/jiaran_xml`、`oldblacklist/beila_xml`，输出统一补充明细到 `oldblacklist/as_output/uid_live_stats.csv`。

- `single_live_to_detail.py`
  读取 `弹幕JSON/`，并合并 `oldblacklist/as_output/uid_live_stats.csv` 补充明细，生成单场明细和总汇总表。

- `build_user_summary.py`
  根据 `场次明细表/all_live_details.csv` 生成用户长表、宽表，并应用黑名单清洗。

- `build_conversion_report.py`
  以 `乃琳_鸣潮` 为 source，生成后续承接 / 团内流动分析结果和 cohort 表。

- `plot_conversion_report.py`
  根据承接分析结果生成 SVG 图表。

- `fans_plot.py`
  生成三人维度的解释性阈值摘要表、非零长表和 SVG 图。

- `build_all_reports.py`
  一键串联补充明细、总明细、用户总表、承接分析和图表产出。

## 推荐流程

推荐直接运行：

```bash
python3 build_all_reports.py
```

如需分步调试，再依次运行：

```bash
python3 oldblacklist/extract_mc_uid_stats.py
python3 single_live_to_detail.py
python3 build_user_summary.py
python3 build_conversion_report.py
python3 plot_conversion_report.py
python3 fans_plot.py
```

如果补充 XML 只想使用已有 hash 映射、不反解新增 hash，可以运行：

```bash
python3 oldblacklist/extract_mc_uid_stats.py --skip-crack
```

## 主要输出

- `场次明细表/all_live_details.csv`
- `用户总表/user_summary_wide_clean.csv`
- `分析结果/乃琳_鸣潮_后续承接分析/`
- `分析结果/fans_conversion_三人解释性阈值_摘要.csv`

核心图表位于 `分析结果/乃琳_鸣潮_后续承接分析/plots/`，包括：

- `00_source画像概况.svg`
- `01_重合率_vs_后续承接率.svg`
- `03_首次后续去向.svg`
- `04_团内流动分层.svg`
- `05_团内流动分层_不含无后续.svg`

## 说明

- 当前核心分析口径是：`乃琳_鸣潮` 人群在首次进入 source 之后的后续承接。
- `all_live_details.csv` 已经是统一口径；后续脚本不再单独读取旧版 `KG补充名单/`。
- 图表默认输出为 SVG。
