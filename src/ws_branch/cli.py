"""薄殼 CLI:uv run python -m ws_branch <cmd>。"""

from __future__ import annotations

import argparse

from ws_branch.tables import runner


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="ws_branch")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="建表(缺哪年建哪年)")
    b.add_argument("--table", default="t1_broker_daily")
    b.add_argument("--year", type=int)
    b.add_argument("--force", action="store_true")
    v = sub.add_parser("verify", help="對帳")
    v.add_argument("--table", default="t1_broker_daily")
    v.add_argument("--n", type=int, default=60)
    args = ap.parse_args(argv)
    if args.cmd == "build":
        runner.build(args.table, args.year, args.force)
    else:
        runner.verify(args.table, args.n)


if __name__ == "__main__":
    main()
