"""分層執法(三色分層,REDESIGN §3):measure/ 與 products/(除 loaders)零 IO;registry 只宣告。"""

from __future__ import annotations

import ast
import pathlib

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "ws_branch"
IO_MODULES = {"ws_core", "ws_branch.tables", "ws_branch.tables.io", "ws_branch.products.loaders"}


def _imports(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
    return out


def _violates(mods: set[str]) -> set[str]:
    return {m for m in mods if any(m == io or m.startswith(io + ".") for io in IO_MODULES)}


def test_measure_is_pure_zero_io() -> None:
    for f in (SRC / "measure").glob("*.py"):
        assert not _violates(_imports(f)), f"{f.name} 匯入了 IO 層:{_violates(_imports(f))}"


def test_products_renderers_are_pure_except_loaders() -> None:
    for f in (SRC / "products").glob("*.py"):
        if f.name == "loaders.py":
            continue
        assert not _violates(_imports(f)), f"{f.name} 匯入了 IO 層:{_violates(_imports(f))}"


def test_registry_only_declares() -> None:
    tree = ast.parse((SRC / "tables" / "registry.py").read_text())
    fns = [n.name for n in tree.body if isinstance(n, ast.FunctionDef)]
    assert fns == [], f"registry 不得定義函數(verify 歸 tables/verify.py):{fns}"


def test_runner_knows_no_table_internals() -> None:
    src = (SRC / "tables" / "runner.py").read_text()
    for token in ("_parts_", "t1_broker_daily", "t4_broker", "t3b"):
        assert token not in src, f"runner 不得認識特定表的內部:{token}"


def test_cli_scripts_are_thin_shells() -> None:
    for name in ("v3_readbook.py", "v3_broker_profile.py"):
        src = (SRC.parents[1] / "scripts" / "observatory" / name).read_text()
        assert "io.scan" not in src and "stock_attr" not in src, f"{name} 不得自己載資料(歸 products/loaders)"
        assert len(src.splitlines()) < 40, f"{name} 應為薄殼"
