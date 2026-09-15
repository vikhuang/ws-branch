"""相容薄殼(舊 CLI → ws_branch.tables.runner)。

背景建表迴圈仍以 `python factory.py build --year Y` 呼叫,介面凍結;
新程式請直接用 `python -m ws_branch build/verify`。全史建完後可刪本檔。
"""

from __future__ import annotations

import argparse

from ws_branch.tables import runner


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--year", type=int)
    b.add_argument("--force", action="store_true")
    v = sub.add_parser("verify")
    v.add_argument("--n", type=int, default=60)
    args = ap.parse_args()
    if args.cmd == "build":
        runner.build("t1_broker_daily", args.year, args.force)
    else:
        runner.verify("t1_broker_daily", args.n)


if __name__ == "__main__":
    main()
