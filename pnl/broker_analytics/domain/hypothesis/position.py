"""Position derivation from pnl_daily + daily_summary + close_prices.

Plan A: Mathematically exact (verified diff=0 vs FIFO ground truth).
Formula: net_shares = cumsum(buy_shares - sell_shares)
         avg_cost = close_price - unrealized_pnl / net_shares (when net_shares != 0)

Used by strategies 3 (conviction), 4 (exodus), 8 (concentration).
"""

import polars as pl


def derive_positions(
    trade_df: pl.DataFrame,
    pnl_daily_df: pl.DataFrame,
    prices: pl.DataFrame,
    symbol: str,
) -> pl.DataFrame:
    """Derive daily position info per broker.

    Args:
        trade_df: daily_summary schema [broker, date, buy_shares, sell_shares, ...]
        pnl_daily_df: [broker, date, realized_pnl, unrealized_pnl]
        prices: close_prices long format [symbol_id, date, close_price]
        symbol: stock symbol for price lookup

    Returns:
        DataFrame[broker, date, net_shares, unrealized_pnl, avg_cost]
        Only rows where net_shares != 0 (broker has a position).
    """
    # Step 1: cumulative net shares per broker
    net = (
        trade_df
        .sort("date")
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("daily_net"),
        )
        .with_columns(
            pl.col("daily_net")
            .cum_sum()
            .over("broker")
            .alias("net_shares")
        )
        .select("broker", "date", "net_shares")
    )

    # Step 2: join with pnl_daily for unrealized_pnl
    pos = net.join(
        pnl_daily_df.select(
            pl.col("broker").cast(pl.Utf8),
            "date",
            "unrealized_pnl",
        ),
        on=["broker", "date"],
        how="inner",
    )

    # Step 3: join with prices for this symbol
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .select("date", pl.col("close_price").alias("price"))
    )
    pos = pos.join(sym_prices, on="date", how="inner")

    # Step 4: derive avg_cost where net_shares != 0
    pos = (
        pos
        .filter(pl.col("net_shares") != 0)
        .with_columns(
            (pl.col("price") - pl.col("unrealized_pnl") / pl.col("net_shares"))
            .alias("avg_cost")
        )
        .select("broker", "date", "net_shares", "unrealized_pnl", "avg_cost")
    )

    return pos
