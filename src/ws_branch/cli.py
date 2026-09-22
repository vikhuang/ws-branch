"""薄殼 CLI:uv run python -m ws_branch <cmd>。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ws_branch.contracts import contract_document, dataset_contract
from ws_branch.tables import runner
from ws_branch.tables.exchange import export_dataset


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
    c = sub.add_parser("contract", help="輸出機器可讀的資料契約")
    c.add_argument("--dataset")
    e = sub.add_parser("export", help="依契約匯出有界資料與 receipt")
    e.add_argument("--dataset", required=True)
    e.add_argument("--start", required=True)
    e.add_argument("--end", required=True)
    e.add_argument("--symbol", action="append", default=[])
    e.add_argument("--broker", action="append", default=[])
    e.add_argument("--output", type=Path, required=True)
    e.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)
    if args.cmd == "build":
        runner.build(args.table, args.year, args.force)
    elif args.cmd == "verify":
        runner.verify(args.table, args.n)
    elif args.cmd == "contract":
        print(json.dumps(dataset_contract(args.dataset) if args.dataset else contract_document(),
                         ensure_ascii=False, indent=2))
    else:
        output, receipt = export_dataset(
            args.dataset, start=args.start, end=args.end, output=args.output,
            symbols=tuple(args.symbol), brokers=tuple(args.broker), force=args.force,
        )
        print(f"{output}\n{receipt}")


if __name__ == "__main__":
    main()
