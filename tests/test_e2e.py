"""e2e:合成小宇宙走完整鏈(ws-core 讀取器 → CLI 建表 → verify → 讀回)。

範圍=鏈路語意正確性;效能/記憶體不在此(條文+真實 verify 把守)。
小宇宙埋三個真實地雷:千分位逗號價格、dash 彙總分點、零股尾數(vol 取整張)。
負向測試竄改資料後 verify 必須 FAIL——警報器本身也要被測。
"""

from __future__ import annotations

import datetime
import os
import shutil
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

REPO = Path(__file__).resolve().parents[1]
D1, D2 = datetime.date(2026, 1, 5), datetime.date(2026, 1, 6)


def _utc_midnight_taipei(d: datetime.date):
    tz8 = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime(d.year, d.month, d.day, tzinfo=tz8).astimezone(
        datetime.timezone.utc)


def _write_broker_shard(root: Path, d: datetime.date, rows: list[dict]) -> None:
    (root / "fugle" / "broker_tx").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema={
        "symbol_id": pl.String, "date": pl.Datetime("ms", "UTC"),
        "broker": pl.String, "broker_name": pl.String,
        "price": pl.String, "buy": pl.Int64, "sell": pl.Int64,
    }).write_parquet(root / "fugle" / "broker_tx"
                     / f"broker_tx_{d.strftime('%Y%m%d')}.parquet")


def _mk_universe(root: Path) -> None:
    """2 股 × 2 日 × 3 分點;每股日 買總=賣總=TEJ vol(整張)+零股尾數。"""
    dt1, dt2 = _utc_midnight_taipei(D1), _utc_midnight_taipei(D2)

    def R(sym, dt, broker, name, price, buy, sell):
        return dict(symbol_id=sym, date=dt, broker=broker, broker_name=name,
                    price=price, buy=buy, sell=sell)

    _write_broker_shard(root, D1, [
        # 2330:買總=賣總=1,001,000 股 → vol=1001 張(整除)
        R("2330", dt1, "A1", "元大-台北", "100.00", 600_000, 200_000),
        R("2330", dt1, "8440", "摩根大通", "1,085.00", 400_000, 800_000),  # 逗號價/外資 cohort
        R("2330", dt1, "P1", "國泰-自營", "-", 1_000, 1_000),           # dash-only
        # 1531:21,501 股 → vol=21 張(零股尾數,考 1 張容差)
        R("1531", dt1, "A1", "元大-台北", "35.50", 21_501, 21_501),
    ])
    _write_broker_shard(root, D2, [
        R("2330", dt2, "A1", "元大-台北", "101.00", 50_000, 50_000),
    ])

    (root / "tej").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "coid": ["2330", "1531", "2330"],
        "mdate": [D1, D1, D2],
        "vol": [1001, 21, 50],
    }).write_parquet(root / "tej" / "prices.parquet")

    base = {k: 0.0 for k in [
        "qfii_buy", "qfii_sell", "qfii_ex", "fund_buy", "fund_sell", "fund_ex",
        "dlrp_buy", "dlrp_sell", "dlrp_ex", "dlrh_buy", "dlrh_sell", "dlrh_ex",
        "qfii_bamt", "qfii_samt", "fund_bamt", "fund_samt", "vol_dt", "vol_dtp",
        # T3 v2:自營金額 + 三大法人合計
        "dlrp_bamt", "dlrp_samt", "dlrh_bamt", "dlrh_samt",
        "tot_buy", "tot_sell", "tot_bamt", "tot_samt"]}
    rows = [
        # 2330@D1:官方外資買 400 張、賣 800 張;全市場 1,001 張(vol)
        # → 買方 cohort(F1=摩根大通,代號 8440)400 張,界限可手算
        {"coid": "2330", "mdate": D1, **base,
         "qfii_buy": 400.0, "qfii_sell": 800.0, "qfii_ex": -400.0,
         "qfii_bamt": 434_000.0, "qfii_samt": 868_000.0,
         "tot_buy": 400.0, "tot_sell": 800.0,
         "vol_dt": 100.0, "vol_dtp": 9.99},
        {"coid": "1531", "mdate": D1, **base, "fund_buy": 21.0, "fund_ex": 21.0,
         "tot_buy": 21.0},
        {"coid": "2330", "mdate": D2, **base},
    ]
    pl.DataFrame(rows).write_parquet(root / "tej" / "shareholding.parquet")

    # stock_attr:universe gate 的來源(逐日證券類型)。2330/1531 為普通股,
    # 另埋一檔 ETF 與一列指數,驗證 gate 會擋掉它們。
    pl.DataFrame({
        "coid": ["2330", "1531", "0050", "IX0001"] * 2,
        "mdate": [D1] * 4 + [D2] * 4,
        "stktp_c": ["普通股", "普通股", "ETF", "指數"] * 2,
    }).write_parquet(root / "tej" / "stock_attr.parquet")

    # t4 v2(sector_hhi/top_sector)需要 ws-core tickers 讀取器;WS_DATA_ROOT
    # 覆寫後這個讀取器也指到合成小宇宙,故此處補一份最小 tickers fixture。
    (root / "tickers").mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "ticker_local": ["2330", "1531"],
        "industry_local": ["半導體業", "其他電機"],
    }).write_parquet(root / "tickers" / "tw.parquet")


def _run(args: list[str], env: dict, expect_ok: bool = True) -> str:
    r = subprocess.run([sys.executable, "-m", "ws_branch", *args],
                       capture_output=True, text=True, cwd=REPO,
                       env={**os.environ, **env}, timeout=300)
    if expect_ok:
        assert r.returncode == 0, f"{args} failed:\n{r.stdout}\n{r.stderr}"
    return r.stdout + r.stderr


@pytest.fixture(scope="module")
def universe(tmp_path_factory: pytest.TempPathFactory) -> dict:
    src = tmp_path_factory.mktemp("mini_src")
    tables = tmp_path_factory.mktemp("mini_tables")
    _mk_universe(src)
    env = {"WS_DATA_ROOT": str(src), "WS_BRANCH_DATA_DIR": str(tables)}
    for t in ["t1_broker_daily", "t3_official_daily", "t3b_accounting_bounds",
              "t4_broker_features"]:
        _run(["build", "--table", t, "--year", "2026"], env)
    return {"env": env, "tables": tables}


def test_e2e_t1_hand_checked(universe: dict) -> None:
    df = pl.read_parquet(universe["tables"] / "t1_broker_daily" / "year=2026.parquet")
    assert df["date"].dtype == pl.Date and set(df["date"].to_list()) == {D1, D2}
    f1 = df.filter((pl.col("broker") == "8440") & (pl.col("date") == D1))
    assert f1[0, "buy_dollar"] == pytest.approx(400_000 * 1085.0)  # 逗號價正確
    p1 = df.filter(pl.col("broker") == "P1")
    assert p1[0, "buy_sh"] == 1_000 and p1[0, "has_dash"]           # dash coalesce


def test_e2e_t3_units(universe: dict) -> None:
    df = pl.read_parquet(universe["tables"] / "t3_official_daily" / "year=2026.parquet")
    r = df.filter((pl.col("symbol_id") == "2330") & (pl.col("date") == D1))
    assert r[0, "foreign_buy_sh"] == pytest.approx(400_000)         # 千股→股
    assert r[0, "foreign_sell_amt"] == pytest.approx(868_000_000)   # 千元→元
    assert r[0, "day_trade_pct"] == pytest.approx(9.99)


def test_e2e_t4_hand_checked(universe: dict) -> None:
    df = pl.read_parquet(universe["tables"] / "t4_broker_features" / "year=2026.parquet")
    a1 = df.filter((pl.col("broker") == "A1") & (pl.col("date") == D1))
    # A1@D1:2330 gross=600k*100+200k*100=8000 萬;1531 gross=(21501+21501)*35.5
    g2330 = 600_000 * 100.0 + 200_000 * 100.0
    g1531 = (21_501 + 21_501) * 35.5
    assert a1[0, "n_symbols"] == 2
    assert a1[0, "gross_amt"] == pytest.approx(g2330 + g1531)
    assert a1[0, "top1_share"] == pytest.approx(g2330 / (g2330 + g1531))
    exp_ratio = abs((600_000 * 100 + 21_501 * 35.5) - (200_000 * 100 + 21_501 * 35.5)) \
        / (g2330 + g1531)
    assert a1[0, "directional_ratio"] == pytest.approx(exp_ratio)


def test_e2e_t4_v2_hand_checked(universe: dict) -> None:
    import math

    df = pl.read_parquet(universe["tables"] / "t4_broker_features" / "year=2026.parquet")
    g2330_d1 = 600_000 * 100.0 + 200_000 * 100.0
    g1531_d1 = (21_501 + 21_501) * 35.5
    g2330_d2 = 50_000 * 101.0 + 50_000 * 101.0

    # sector_hhi/top_sector:A1@D1 碰兩個產業(2330=半導體業,1531=其他電機)
    a1_d1 = df.filter((pl.col("broker") == "A1") & (pl.col("date") == D1)).row(0, named=True)
    total = g2330_d1 + g1531_d1
    exp_hhi = (g2330_d1 / total) ** 2 + (g1531_d1 / total) ** 2
    assert a1_d1["sector_hhi"] == pytest.approx(exp_hhi)
    assert a1_d1["top_sector"] == "半導體業"

    # basket_self_sim:A1 D2(僅 2330)vs D1(2330+1531)cosine
    a1_d2 = df.filter((pl.col("broker") == "A1") & (pl.col("date") == D2)).row(0, named=True)
    norm_d1 = math.sqrt(g2330_d1**2 + g1531_d1**2)
    norm_d2 = g2330_d2
    exp_sim = (g2330_d1 * g2330_d2) / (norm_d1 * norm_d2)
    assert a1_d2["basket_self_sim"] == pytest.approx(exp_sim)
    # A1 首個活躍日(D1)無「昨天」可比 → null,不得補 0
    assert a1_d1["basket_self_sim"] is None

    # multiplicity 對全表都要落在 [0,1](不變量,verify 也會查同一件事)
    finite = df.filter(pl.col("multiplicity").is_not_null())
    assert finite.height > 0
    assert finite["multiplicity"].min() >= -1e-9
    assert finite["multiplicity"].max() <= 1 + 1e-9

    # 小宇宙每個分點日購物籃都 < _MIN_SUPPORT(5 檔),foreign_sim/fund_sim
    # 一律 null——這是「資料太小不硬湊統計量」的設計行為,不是遺漏欄位
    assert df["foreign_sim_buy"].drop_nulls().len() == 0
    assert set(df.columns) >= {
        "foreign_sim_buy_20d", "foreign_sim_buy_60d",
        "fund_sim_sell_20d", "fund_sim_sell_60d",
        "daytrade_assoc", "foreign_sim_confidence", "fund_sim_confidence",
    }


def test_e2e_t3b_bounds_hand_checked(universe: dict) -> None:
    df = pl.read_parquet(
        universe["tables"] / "t3b_accounting_bounds" / "year=2026.parquet")
    # universe gate:只有普通股 2330/1531 進表,ETF 0050 與指數 IX0001 被擋掉
    assert set(df["symbol_id"].unique()) == {"2330", "1531"}
    assert set(df["side"].unique()) == {"buy", "sell"}

    r = df.filter((pl.col("symbol_id") == "2330") & (pl.col("date") == D1)
                  & (pl.col("side") == "buy")).row(0, named=True)
    # V=1,001 張=1,001,000 股;T1 觀測=600k+400k+1k(dash)=1,001,000
    assert r["market_total_sh"] == pytest.approx(1_001_000)
    assert r["observed_total_sh"] == pytest.approx(1_001_000)
    assert r["unobserved_sh"] == pytest.approx(0.0) and r["unobserved_sh_ok"]
    # cohort(8440 摩根大通)買 400,000 股;官方外資買 400,000 股
    assert r["cohort_sh"] == pytest.approx(400_000)
    assert r["official_foreign_sh"] == pytest.approx(400_000)
    # 手算界限:L=max(0, 400k+400k−1,001k)=0;U=min(400k,400k)=400k
    assert r["foreign_x_lo"] == pytest.approx(0.0)
    assert r["foreign_x_hi"] == pytest.approx(400_000)
    assert r["foreign_cov_hi"] == pytest.approx(1.0)  # 上界剛好 100%
    assert r["foreign_y_lo"] == pytest.approx(0.0)    # 不必有外資在 cohort 外
    # 餘額 = V − 四桶 = 1,001k − 400k = 601k(未識別種類,不得叫散戶)
    assert r["other_actor_sh"] == pytest.approx(601_000) and r["other_actor_sh_ok"]
    assert r["universe_version"] == "stock_v1"

    # 1531@D1 賣方:零股尾數 21,501 股 vs vol 21 張 → 未觀測 −501 股,容差內
    r2 = df.filter((pl.col("symbol_id") == "1531") & (pl.col("side") == "sell")
                   ).row(0, named=True)
    assert r2["unobserved_sh"] == pytest.approx(21_000 - 21_501)
    assert r2["unobserved_sh_ok"]


def test_e2e_verifies_pass(universe: dict) -> None:
    env = universe["env"]
    out1 = _run(["verify", "--table", "t1_broker_daily", "--n", "10"], env)
    assert "PASS" in out1 and "零超出" in out1
    out3 = _run(["verify", "--table", "t3_official_daily", "--n", "10"], env)
    assert "PASS" in out3
    out4 = _run(["verify", "--table", "t4_broker_features", "--n", "10"], env)
    assert "PASS" in out4


def _tampered_copy(universe: dict, tmp_path: Path, table: str,
                   mutate) -> dict:
    tables2 = tmp_path / "tampered"
    shutil.copytree(universe["tables"], tables2)
    p = tables2 / table / "year=2026.parquet"
    mutate(pl.read_parquet(p)).write_parquet(p)
    return {**universe["env"], "WS_BRANCH_DATA_DIR": str(tables2)}


def test_e2e_alarm_fires_on_t1_overshoot(universe: dict, tmp_path: Path) -> None:
    env = _tampered_copy(
        universe, tmp_path, "t1_broker_daily",
        lambda df: df.with_columns(
            pl.when((pl.col("broker") == "A1") & (pl.col("date") == D1)
                    & (pl.col("symbol_id") == "2330"))
            .then(pl.col("buy_sh") + 500_000).otherwise(pl.col("buy_sh"))
            .alias("buy_sh")))
    out = _run(["verify", "--table", "t1_broker_daily", "--n", "10"],
               env, expect_ok=False)
    assert "FAIL" in out and "超過" in out


def test_e2e_alarm_fires_on_t3_drift(universe: dict, tmp_path: Path) -> None:
    env = _tampered_copy(
        universe, tmp_path, "t3_official_daily",
        lambda df: df.with_columns(
            (pl.col("foreign_buy_sh") * 2).alias("foreign_buy_sh")))
    out = _run(["verify", "--table", "t3_official_daily", "--n", "10"],
               env, expect_ok=False)
    assert "FAIL" in out