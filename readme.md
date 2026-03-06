# ASOUL 直播弹幕分析

这个项目用于整理 ASOUL 直播弹幕 JSON，并生成用于分析“乃琳鸣潮人群后续承接 / 团内流动”的中间表、结果表和图表。

## 目录

- `弹幕JSON/`
  原始直播弹幕 JSON，按成员 / 类型分文件夹存放。

- `场次明细表/`
  单场直播聚合后的明细表。
  - `场次明细表/弹幕JSON/`：每场直播对应的 `_detail.csv`
  - `场次明细表/all_live_details.csv`：全部场次汇总表

- `用户总表/`
  由场次明细继续汇总得到的用户长表 / 宽表。

- `黑名单/`
  黑名单 UID，用于过滤老粉或异常用户。

- `分析结果/`
  承接分析结果、三人阈值结果、SVG 图表和报告素材。

## 脚本

- `single_live_to_detail.py`
  读取 `弹幕JSON/`，生成单场明细和总汇总表。

- `build_user_summary.py`
  根据 `场次明细表/all_live_details.csv` 生成用户长表、宽表，并应用黑名单清洗。

- `build_conversion_report.py`
  以 `乃琳_鸣潮` 为 source，生成后续承接 / 团内流动分析结果、cohort 表和报告素材。

- `plot_conversion_report.py`
  根据承接分析结果生成 SVG 图表。

- `fans_plot.py`
  生成三人维度的解释性阈值摘要表、非零长表和 SVG 图。

## 推荐流程

1. 运行 `single_live_to_detail.py`
2. 运行 `build_user_summary.py`
3. 运行 `build_conversion_report.py`
4. 运行 `plot_conversion_report.py`
5. 如需三人阈值对比，再运行 `fans_plot.py`

## 主要输出

- `场次明细表/all_live_details.csv`
- `用户总表/user_summary_wide_clean.csv`
- `分析结果/乃琳_鸣潮_后续承接分析/`
- `分析结果/fans_conversion_三人解释性阈值_摘要.csv`

## 说明

- 当前核心分析口径是：`乃琳_鸣潮` 人群在首次进入 source 之后的后续承接。
- 图表默认输出为 SVG。
