"""Machine-readable public dataset contracts."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

CONTRACT_VERSION = "ws-branch.datasets.v1"


def contract_document() -> dict[str, Any]:
    """Return the versioned public dataset contract."""
    resource = files(__package__).joinpath("datasets.v1.json")
    document = json.loads(resource.read_text(encoding="utf-8"))
    if document.get("contract_version") != CONTRACT_VERSION:
        raise RuntimeError("bundled ws-branch contract version mismatch")
    return document


def dataset_contract(name: str) -> dict[str, Any]:
    """Return one dataset contract, failing with the available names."""
    datasets = contract_document()["datasets"]
    if name not in datasets:
        raise KeyError(f"unknown dataset {name!r}; available: {sorted(datasets)}")
    return {"contract_version": CONTRACT_VERSION, "name": name, **datasets[name]}

