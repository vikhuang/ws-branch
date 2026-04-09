"""Smoke test: 全鏈驗證 — GCS broker_tx → 3324 分點前 5 → Telegram。

模組合約：
  - ws-core: 路徑解析（WS_DATA_ROOT）
  - ws-bot-core: run_bot() 生命週期 + Telegram 推播
  - ws-branch: business logic（這裡極簡化）

收到 Telegram = Cloud Run consumer 能讀 GCS + 推播，全鏈通。
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load env cascade（和 run.sh 一致）
_SHARED_ENV = Path.home() / "r20" / "wp" / ".ws-env"
if _SHARED_ENV.exists():
    load_dotenv(_SHARED_ENV, override=False)
load_dotenv(override=False)

# ws-core: 路徑解析（觸發 WS_DATA_ROOT + ADC 自動偵測）
from ws_core.paths import data_dir, is_gcs  # noqa: E402

import gcsfs  # noqa: E402
import polars as pl  # noqa: E402
from ws_bot_core import BotResult, Message, run_bot  # noqa: E402


SYMBOL = "3324"
SYMBOL_NAME = "雙鴻"
TOP_N = 5


def smoke_test() -> BotResult:
    """讀 broker_tx → 篩 3324 → 分價量前 5 → 回傳 Telegram 訊息。"""
    root = data_dir()

    # 找最新的 broker_tx 檔案
    broker_dir = f"{root}/fugle/broker_tx"
    latest_file = _find_latest(broker_dir)
    if not latest_file:
        return BotResult.fail(f"找不到 broker_tx 檔案: {broker_dir}")

    # 讀取
    df = pl.read_parquet(latest_file)
    filtered = df.filter(pl.col("symbol_id") == SYMBOL)

    if filtered.is_empty():
        return BotResult.fail(f"{SYMBOL} 無資料: {latest_file}")

    # 分價量前 5
    top = (
        filtered
        .with_columns((pl.col("buy") + pl.col("sell")).alias("volume"))
        .sort("volume", descending=True)
        .head(TOP_N)
    )

    # 格式化
    date_str = latest_file.split("_")[-1].replace(".parquet", "")
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    lines = [f"<b>Smoke Test — {SYMBOL} {SYMBOL_NAME} {date_fmt}</b>", ""]
    for row in top.iter_rows(named=True):
        net = row["buy"] - row["sell"]
        sign = "+" if net >= 0 else ""
        lines.append(
            f"{row['broker_name']}  ${row['price']}  "
            f"B:{row['buy']:,}  S:{row['sell']:,}  net:{sign}{net:,}"
        )
    lines.append("")
    lines.append(f"source: {root}")

    return BotResult.ok(
        [Message(text="\n".join(lines), chat_key="ADMIN")],
        symbol=SYMBOL,
        rows=len(filtered),
    )


def _find_latest(broker_dir: str) -> str | None:
    """Find the latest broker_tx parquet file."""
    if broker_dir.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        try:
            files = [f for f in fs.ls(broker_dir.replace("gs://", ""))
                     if f.endswith(".parquet")]
        except FileNotFoundError:
            return None
        if not files:
            return None
        files.sort()
        return f"gs://{files[-1]}"
    else:
        d = Path(broker_dir)
        files = sorted(d.glob("*.parquet"))
        return str(files[-1]) if files else None


if __name__ == "__main__":
    run_bot(smoke_test, "ws-branch-smoke", notify=True)
