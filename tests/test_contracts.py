from __future__ import annotations

import datetime
import json
from pathlib import Path

import polars as pl
import pytest
from ws_branch.contracts import (
    CONTRACT_VERSION,
    contract_document,
    dataset_contract,
)
from ws_branch.tables import exchange, io


def test_contract_covers_public_datasets() -> None:
    document = contract_document()
    assert document["contract_version"] == CONTRACT_VERSION
    assert set(document["datasets"]) == {
        "t1_broker_daily", "t2_broker_pricelevel", "t3_official_daily",
        "t3b_accounting_bounds", "t4_broker_measure", "salience_pair",
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
    assert receipt["contract"]["name"] == "t1_broker_daily"
    assert receipt["source_coverage"]["tables"]["t1_broker_daily"]["missing_years"] == []


@pytest.mark.parametrize("problem", ["dtype", "duplicate"])
def test_export_rejects_schema_and_grain_violations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str,
) -> None:
    buy_sh: list[int] | list[str] = [10, 20] if problem == "duplicate" else ["bad", "bad"]
    brokers = ["8440", "8440"] if problem == "duplicate" else ["8440", "8441"]
    source = pl.DataFrame({
        "symbol_id": ["3450", "3450"], "date": [datetime.date(2026, 9, 14)] * 2,
        "broker": brokers, "broker_name": ["A", "B"], "buy_sh": buy_sh,
        "sell_sh": [5, 8], "buy_dollar": [1000.0, 2000.0],
        "sell_dollar": [500.0, 800.0], "has_dash": [False, False],
    })
    monkeypatch.setattr(io, "scan", lambda *args, **kwargs: source.lazy())
    match = "wrong dtypes" if problem == "dtype" else "duplicate grain"
    with pytest.raises(ValueError, match=match):
        exchange.export_dataset(
            "t1_broker_daily", start="2026-09-14", end="2026-09-14",
            output=tmp_path / f"{problem}.parquet",
        )


def test_export_rejects_missing_requested_source_year(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(io, "existing_years", lambda _name: [2025])
    with pytest.raises(ValueError, match="missing requested years: \\[2024\\]"):
        exchange.export_dataset(
            "t1_broker_daily", start="2024-12-31", end="2025-01-02",
            output=tmp_path / "partial.parquet",
        )


def test_export_rejects_missing_trading_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = pl.DataFrame({"date": [datetime.date(2025, 1, 2)]})
    monkeypatch.setattr(io, "existing_years", lambda _name: [2025])
    monkeypatch.setattr(io, "scan", lambda *args, **kwargs: source.lazy())
    monkeypatch.setattr(
        exchange, "_expected_trading_dates",
        lambda _start, _end: [datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)],
    )
    with pytest.raises(ValueError, match="missing trading dates"):
        exchange.export_dataset(
            "t1_broker_daily", start="2025-01-02", end="2025-01-03",
            output=tmp_path / "gap.parquet",
        )


def test_export_refuses_wrong_dimension(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(io, "scan", lambda *args, **kwargs: pl.DataFrame({"date": []}).lazy())
    monkeypatch.setattr(exchange, "_source_coverage", lambda *args, **kwargs: {})
    with pytest.raises(ValueError, match="no broker dimension"):
        exchange.export_dataset(
            "t3_official_daily", start="2026-09-14", end="2026-09-14",
            output=tmp_path / "bad.parquet", brokers=("8440",),
        )
