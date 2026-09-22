from __future__ import annotations

import datetime
import json
from pathlib import Path

import polars as pl
import pytest

from ws_branch.contracts import CONTRACT_VERSION, contract_document, dataset_contract
from ws_branch.tables import exchange, io


def test_contract_covers_public_datasets() -> None:
    document = contract_document()
    assert document["contract_version"] == CONTRACT_VERSION
    assert set(document["datasets"]) == {
        "t1_broker_daily", "t2_broker_pricelevel", "t3_official_daily",
        "t3b_accounting_bounds", "t4_broker_measure",
    }
    assert dataset_contract("t2_broker_pricelevel")["columns"]["buy"].endswith("(股)")


def test_export_writes_bounded_data_and_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = pl.DataFrame({
        "symbol_id": ["3450", "2330"], "date": [datetime.date(2026, 9, 14)] * 2,
        "broker": ["8440", "8440"], "broker_name": ["摩根大通"] * 2,
        "buy_sh": [10, 20], "sell_sh": [5, 8], "buy_dollar": [1000.0, 2000.0],
        "sell_dollar": [500.0, 800.0], "has_dash": [False, False],
    })
    monkeypatch.setattr(io, "scan", lambda *args, **kwargs: source.lazy())
    monkeypatch.setattr(exchange, "_git_commit", lambda: "abc1234")
    output = tmp_path / "sample.parquet"
    _, receipt_path = exchange.export_dataset(
        "t1_broker_daily", start="2026-09-14", end="2026-09-14", output=output,
        symbols=("3450",),
    )
    assert pl.read_parquet(output)["symbol_id"].to_list() == ["3450"]
    receipt = json.loads(receipt_path.read_text())
    assert receipt["contract_version"] == CONTRACT_VERSION
    assert receipt["provider_git_commit"] == "abc1234"
    assert receipt["result"]["rows"] == 1
    assert len(receipt["result"]["sha256"]) == 64
    assert len(receipt["contract_sha256"]) == 64


def test_export_refuses_wrong_dimension(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(io, "scan", lambda *args, **kwargs: pl.DataFrame({"date": []}).lazy())
    with pytest.raises(ValueError, match="no broker dimension"):
        exchange.export_dataset(
            "t3_official_daily", start="2026-09-14", end="2026-09-14",
            output=tmp_path / "bad.parquet", brokers=("8440",),
        )
