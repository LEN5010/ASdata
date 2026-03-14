import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
FILTERED_ANALYSIS_DIR_NAME = "乃琳_鸣潮_后续承接分析_去0211单次且无后续"
FILTERED_SESSION_KEY = "32d14305-7ef3-4b90-b261-8d68e754b6b3"


def run_step(title, command):
    print(f"[STEP] {title}")
    print(f"[CMD] {' '.join(command)}")
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description="一键生成统一明细、黑名单、用户总表、承接报表和图表")
    parser.add_argument("--skip-blacklist", action="store_true", help="跳过黑名单重算")
    parser.add_argument("--skip-user-summary", action="store_true", help="跳过 build_user_summary.py")
    parser.add_argument("--skip-fans", action="store_true", help="跳过 fans_plot.py")
    args = parser.parse_args()

    python_exe = sys.executable or "python3"

    run_step("生成统一场次明细", [python_exe, "single_live_to_detail.py"])

    if not args.skip_blacklist:
        run_step("重算黑名单", [python_exe, "build_uid_blacklist.py"])

    if not args.skip_user_summary:
        run_step("生成用户总表", [python_exe, "build_user_summary.py"])

    run_step("生成承接分析报表", [python_exe, "build_conversion_report.py"])
    run_step("生成承接分析图表", [python_exe, "plot_conversion_report.py"])
    run_step(
        "生成承接分析报表（去0211单次且无后续）",
        [
            python_exe,
            "build_conversion_report.py",
            "--analysis-dir-name",
            FILTERED_ANALYSIS_DIR_NAME,
            "--exclude-source-single-pass-session-key",
            FILTERED_SESSION_KEY,
            "--no-legacy-output",
        ],
    )
    run_step(
        "生成承接分析图表（去0211单次且无后续）",
        [
            python_exe,
            "plot_conversion_report.py",
            "--analysis-dir-name",
            FILTERED_ANALYSIS_DIR_NAME,
            "--page-title",
            FILTERED_ANALYSIS_DIR_NAME,
        ],
    )

    if not args.skip_fans:
        run_step("生成三人阈值图表", [python_exe, "fans_plot.py"])

    print("[DONE] 全流程已完成")


if __name__ == "__main__":
    main()
