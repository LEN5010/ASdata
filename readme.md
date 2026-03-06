# 直播用户分析脚本

用于从单场直播数据汇总用户总表，并基于用户宽表做流向、转化和交叉分析。

## 文件说明

- `single_live_to_detail.py`  
  单场直播原始数据整理为场次明细表。

- `build_user_summary.py`  
  根据明细表汇总生成用户总表，使用 `uid_blacklist.csv` 过滤黑名单用户，生成干净版宽表。

## 推荐流程

1. 先运行 `single_live_to_detail.py`
2. 再运行 `build_user_summary.py`
3. 分析使用 `user_summary_wide_clean.csv`

## 说明

默认分析核心字段是各列 `xxx_到场次数`。  
如果要做严格“先看A后看B”的时间顺序分析，需要回到明细表继续处理。
