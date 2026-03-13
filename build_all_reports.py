import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run_step(title, command):
    print(f"[STEP] {title}")
    print(f"[CMD] {' '.join(command)}")
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description="一键生成补充明细、总明细、统计报表和图表")
    parser.add_argument("--skip-supplement", action="store_true", help="跳过 oldblacklist XML -> 补充明细")
    parser.add_argument("--skip-fans", action="store_true", help="跳过 fans_plot.py")
    parser.add_argument("--skip-user-summary", action="store_true", help="跳过 build_user_summary.py")
    parser.add_argument("--workers", type=int, default=None, help="传给 oldblacklist/extract_mc_uid_stats.py 的 workers")
    parser.add_argument("--skip-crack", action="store_true", help="补充明细阶段只使用已有 hash->uid 映射，不反解新增 hash")
    args = parser.parse_args()

    python_exe = sys.executable or "python3"

    if not args.skip_supplement:
        supplement_cmd = [python_exe, "oldblacklist/extract_mc_uid_stats.py"]
        if args.workers is not None:
            supplement_cmd.extend(["--workers", str(args.workers)])
        if args.skip_crack:
            supplement_cmd.append("--skip-crack")
        run_step("生成 oldblacklist 补充明细", supplement_cmd)

    run_step("生成统一场次明细", [python_exe, "single_live_to_detail.py"])

    if not args.skip_user_summary:
        run_step("生成用户总表", [python_exe, "build_user_summary.py"])

    run_step("生成承接分析报表", [python_exe, "build_conversion_report.py"])
    run_step("生成承接分析图表", [python_exe, "plot_conversion_report.py"])

    if not args.skip_fans:
        run_step("生成三人阈值图表", [python_exe, "fans_plot.py"])

    print("[DONE] 全流程已完成")


if __name__ == "__main__":
    main()
