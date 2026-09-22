"""Versioned file-exchange boundary for external research consumers."""

from __future__ import annotations

import datetime
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

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


def _expected_dtype(spec: str) -> pl.DataType:
    name = spec.split(":", 1)[0]
    simple = {
        "String": pl.String, "Date": pl.Date, "Boolean": pl.Boolean,
        "Int32": pl.Int32, "Int64": pl.Int64, "UInt32": pl.UInt32, "Float64": pl.Float64,
    }
    if name in simple:
        return simple[name]
    if name.startswith("Datetime[") and name.endswith("]"):
        return pl.Datetime(time_zone=name[9:-1])
    raise ValueError(f"unsupported contract dtype: {name}")


def _validate_columns(schema: pl.Schema, contract: dict) -> None:
    missing = sorted(set(contract["columns"]) - set(schema.names()))
    if missing:
        raise ValueError(f"{contract['name']} violates {CONTRACT_VERSION}; missing columns: {missing}")
    wrong = {
        name: {"expected": str(_expected_dtype(spec)), "actual": str(schema[name])}
        for name, spec in contract["columns"].items()
        if schema[name] != _expected_dtype(spec)
    }
    if wrong:
        raise ValueError(f"{contract['name']} violates {CONTRACT_VERSION}; wrong dtypes: {wrong}")


def _validate_values(output: Path, contract: dict) -> None:
    frame = pl.scan_parquet(output)
    grain = contract["grain"]
    null_grain = frame.select(
        pl.any_horizontal(*(pl.col(c).is_null() for c in grain)).any()
    ).collect().item()
    if null_grain:
        raise ValueError(f"{contract['name']} violates {CONTRACT_VERSION}; null in grain {grain}")
    duplicate = frame.group_by(grain).len().filter(pl.col("len") > 1).limit(1).collect()
    if duplicate.height:
        raise ValueError(
            f"{contract['name']} violates {CONTRACT_VERSION}; duplicate grain {grain}: "
            f"{duplicate.row(0, named=True)}"
        )


def _effective_start(contract: dict, start: str) -> datetime.date:
    return datetime.date.fromisoformat(start) - datetime.timedelta(days=contract.get("lookback_days", 0))


def _requested_years(contract: dict, start: str, end: str) -> list[int]:
    return list(range(_effective_start(contract, start).year,
                      datetime.date.fromisoformat(end).year + 1))


def _source_tables(contract: dict) -> list[str]:
    if contract.get("source_tables"):
        return list(contract["source_tables"])
    return [contract["physical_name"]] if contract["source"] == "materialized" else []


def _expected_trading_dates(start: str, end: str) -> list[datetime.date]:
    from ws_core import trading_days

    return trading_days(start=start, end=end)["zdate"].to_list()


def _source_coverage(contract: dict, start: str, end: str) -> dict[str, Any]:
    requested = _requested_years(contract, start, end)
    coverage: dict[str, Any] = {
        "query_start": start, "effective_start": str(_effective_start(contract, start)),
        "requested_years": requested, "calendar": "ws_core.trading_days", "tables": {},
    }
    expected_dates = _expected_trading_dates(str(_effective_start(contract, start)), end)
    for table in _source_tables(contract):
        available = io.existing_years(table)
        missing = sorted(set(requested) - set(available))
        if missing:
            raise ValueError(f"{contract['name']} source {table} missing requested years: {missing}")
        observed = (io.scan(table, start=str(_effective_start(contract, start)), end=end)
                    .select("date").unique().sort("date").collect()["date"].to_list())
        missing_dates = sorted(set(expected_dates) - set(observed))
        coverage["tables"][table] = {
            "available_years": available, "missing_years": missing,
            "observed_date_count": len(observed),
            "missing_trading_dates": [str(d) for d in missing_dates],
        }
        if missing_dates:
            preview = [str(d) for d in missing_dates[:5]]
            raise ValueError(
                f"{contract['name']} source {table} missing trading dates: {preview}"
                f"{'...' if len(missing_dates) > 5 else ''}"
            )
    return coverage


def _source_manifests(contract: dict, start: str, end: str) -> list[dict[str, Any]]:
    records = []
    for table in _source_tables(contract):
        for year in _requested_years(contract, start, end):
            path = io.table_dir(table) / f"year={year}.manifest.json"
            if not path.exists():
                continue
            raw = json.loads(path.read_text(encoding="utf-8"))
            records.append({
                "table": table, "year": year, "path": str(path.resolve()),
                "sha256": _sha256_file(path), "schema_version": raw.get("schema_version"),
                "measurement_version": raw.get("measurement_version"),
                "source_snapshot": raw.get("source_snapshot"), "as_of": raw.get("as_of"),
            })
    return records


def _validate_view_coverage(
    frame: pl.LazyFrame, contract: dict, start: str, end: str, coverage: dict[str, Any],
) -> None:
    if _source_tables(contract):
        return
    observed = (frame.select(contract["date_column"]).unique().sort(contract["date_column"])
                .collect()[contract["date_column"]].to_list())
    missing = sorted(set(_expected_trading_dates(start, end)) - set(observed))
    coverage["view"] = {
        "observed_date_count": len(observed),
        "missing_trading_dates": [str(d) for d in missing],
    }
    if missing:
        raise ValueError(
            f"{contract['name']} source view missing trading dates: "
            f"{[str(d) for d in missing[:5]]}{'...' if len(missing) > 5 else ''}"
        )


def _salience_source(start: str, end: str, symbols: tuple[str, ...], contract: dict) -> pl.LazyFrame:
    if not symbols:
        raise ValueError("salience_pair requires at least one --symbol")
    from ws_branch.measure import salience
    from ws_branch.products.loaders import LOOKBACK_DAYS, T1_COLS, universe_days

    lo = datetime.date.fromisoformat(start) - datetime.timedelta(days=LOOKBACK_DAYS)
    hi = datetime.date.fromisoformat(end)
    universe = universe_days(lo, hi)
    branch_day = (io.scan("t4_broker_measure", start=str(lo), end=end)
                  .select("broker", "date", "gross_amt").collect())
    t1 = (io.scan("t1_broker_daily", start=str(lo), end=end)
          .filter(pl.col("symbol_id").is_in(symbols)).select(T1_COLS).collect())
    gated, _ = salience.gate_numerator(t1, universe)
    result = salience.pair_pipeline(
        gated, branch_day, symbols=list(symbols), universe_days=universe,
    ).filter(pl.col("date").is_between(datetime.date.fromisoformat(start), hi))
    return result.select(contract["columns"].keys()).lazy()


def export_dataset(
    name: str, *, start: str, end: str, output: Path,
    symbols: tuple[str, ...] = (), brokers: tuple[str, ...] = (), force: bool = False,
) -> tuple[Path, Path]:
    """Export one bounded query, validate its contract, and write an auditable receipt."""
    contract = dataset_contract(name)
    if datetime.date.fromisoformat(start) > datetime.date.fromisoformat(end):
        raise ValueError(f"start {start} is after end {end}")
    if output.exists() and not force:
        raise FileExistsError(f"output exists: {output}; pass --force to replace")
    coverage = _source_coverage(contract, start, end)
    frame = (_salience_source(start, end, symbols, contract)
             if contract["source"] == "computed_operation" else _source(start, end, contract))
    _validate_view_coverage(frame, contract, start, end, coverage)
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
    with tempfile.NamedTemporaryFile(
        prefix=f".{output.name}.", suffix=".partial", dir=output.parent, delete=False,
    ) as staging_file:
        staging = Path(staging_file.name)
    try:
        frame.sink_parquet(staging, compression="zstd")
        _validate_values(staging, contract)
        staging.replace(output)
    finally:
        staging.unlink(missing_ok=True)
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
        "contract": contract,
        "dataset": name,
        "provider_git_commit": _git_commit(),
        "source_coverage": coverage,
        "source_manifests": _source_manifests(contract, start, end),
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
