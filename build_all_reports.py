import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
FILTERED_VARIANT = "去0211单次且无后续"
FILTERED_SESSION_KEY = "32d14305-7ef3-4b90-b261-8d68e754b6b3"


def run_step(title, command):
    print(f"[STEP] {title}")
    print(f"[CMD] {' '.join(command)}")
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description="一键生成统一明细、黑名单、用户总表和精简后的核心分析报告")
    parser.add_argument("--skip-blacklist", action="store_true", help="跳过 build_uid_blacklist.py")
    parser.add_argument("--skip-user-summary", action="store_true", help="跳过 build_user_summary.py")
    parser.add_argument("--archive-legacy", action="store_true", help="把旧版分析结果移动到 分析结果/legacy/")
    args = parser.parse_args()

    python_exe = sys.executable or "python3"

    run_step("生成统一场次明细", [python_exe, "single_live_to_detail.py"])

    if not args.skip_blacklist:
        run_step("重算黑名单", [python_exe, "build_uid_blacklist.py"])

    if not args.skip_user_summary:
        run_step("生成用户总表", [python_exe, "build_user_summary.py"])

    report_cmd = [python_exe, "-m", "reports.build_report", "--variant-name", "默认版"]
    if args.archive_legacy:
        report_cmd.append("--archive-legacy")
    run_step("生成核心报告（默认版）", report_cmd)

    run_step(
        "生成核心报告（去0211单次且无后续）",
        [
            python_exe,
            "-m",
            "reports.build_report",
            "--variant-name",
            FILTERED_VARIANT,
            "--exclude-source-single-pass-session-key",
            FILTERED_SESSION_KEY,
        ],
    )

    print("[DONE] 全流程已完成")


if __name__ == "__main__":
    main()
