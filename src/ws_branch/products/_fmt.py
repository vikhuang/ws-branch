"""讀本 / profile 共用的文字格式化(純函數)。「—」= 無值,不留空白、不印 None。"""

from __future__ import annotations


def pct(x: float | None, digits: int = 2) -> str:
    return "—" if x is None else f"{x * 100:.{digits}f}%"


def num(x: float | None, fmt: str = "+.1f") -> str:
    return "—" if x is None else format(x, fmt)


def lots(sh: float | None, signed: bool = False) -> str:
    if sh is None:
        return "—"
    return f"{sh / 1000:+,.0f}" if signed else f"{sh / 1000:,.0f}"
