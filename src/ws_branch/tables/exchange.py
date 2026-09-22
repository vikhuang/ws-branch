"""Versioned file-exchange boundary for external research consumers."""

from __future__ import annotations

import datetime
import hashlib
import json
import subprocess
from pathlib import Path

import polars as pl

from ws_branch.contracts import CONTRACT_VERSION, dataset_contract
from ws_branch.tables import io


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
            cwd=Path(__file__).resolve().parents[3], check=False,
        ).stdout.strip() or None
    except OSError:
        return None


def _source(start: str, end: str, contract: dict) -> pl.LazyFrame:
    if contract["source"] == "materialized":
        return io.scan(contract["physical_name"], start=start, end=end)
    if contract["name"] == "t2_broker_pricelevel":
        from ws_core import broker_tx_pricelevel_scan
        return broker_tx_pricelevel_scan(start=start, end=end)
    raise ValueError(f"unsupported exchange source: {contract['name']}")


def _validate_columns(schema: pl.Schema, contract: dict) -> None:
    missing = sorted(set(contract["columns"]) - set(schema.names()))
    if missing:
        raise ValueError(f"{contract['name']} violates {CONTRACT_VERSION}; missing columns: {missing}")


def export_dataset(
    name: str, *, start: str, end: str, output: Path,
    symbols: tuple[str, ...] = (), brokers: tuple[str, ...] = (), force: bool = False,
) -> tuple[Path, Path]:
    """Export one bounded query and a receipt without changing dataset semantics."""
    contract = dataset_contract(name)
    if datetime.date.fromisoformat(start) > datetime.date.fromisoformat(end):
        raise ValueError(f"start {start} is after end {end}")
    if output.exists() and not force:
        raise FileExistsError(f"output exists: {output}; pass --force to replace")
    frame = _source(start, end, contract)
    if symbols:
        column = contract["symbol_column"]
        if column is None:
            raise ValueError(f"{name} has no symbol dimension")
        frame = frame.filter(pl.col(column).is_in(symbols))
    if brokers:
        column = contract["broker_column"]
        if column is None:
            raise ValueError(f"{name} has no broker dimension")
        frame = frame.filter(pl.col(column).is_in(brokers))
    _validate_columns(frame.collect_schema(), contract)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.sink_parquet(output, compression="zstd")
    summary = pl.scan_parquet(output).select(
        pl.len().alias("rows"),
        pl.col(contract["date_column"]).min().alias("date_min"),
        pl.col(contract["date_column"]).max().alias("date_max"),
    ).collect().row(0, named=True)
    digest = _sha256_file(output)
    contract_digest = hashlib.sha256(
        json.dumps(contract, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    receipt = {
        "contract_version": CONTRACT_VERSION,
        "contract_sha256": contract_digest,
        "dataset": name,
        "provider_git_commit": _git_commit(),
        "query": {"start": start, "end": end, "symbols": list(symbols), "brokers": list(brokers)},
        "result": {**summary, "path": str(output.resolve()), "sha256": digest},
        "availability": contract["availability"],
        "universe": contract["universe"],
        "missingness": contract["missingness"],
        "created_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    receipt_path = output.with_suffix(output.suffix + ".receipt.json")
    receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return output, receipt_path
