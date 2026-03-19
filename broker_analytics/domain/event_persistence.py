"""Event persistence: measure how sustained a signal is over time.

For each event, count how many events (same symbol) occurred in a
trailing window. Sustained conviction (3+ days in a row) may carry
more information than isolated single-day events.

Pure functions — input/output are polars DataFrames.
"""

import polars as pl


def compute_event_persistence(
    events: pl.DataFrame,
    window: int = 5,
) -> pl.DataFrame:
    """Count trailing events within a calendar-day window.

    For each event on date D, persistence = number of events in
    [D - window*1.5, D] (inclusive, using calendar days with weekend
    buffer). Includes the event itself, so minimum = 1.

    Args:
        events: DataFrame with at least [date] column. Must be for
            a SINGLE symbol (no symbol column needed).
        window: Trailing window in trading days (calendar ≈ window × 1.5).

    Returns:
        Same DataFrame with added `persistence` column (Int32).
    """
    if len(events) == 0 or "date" not in events.columns:
        return events.with_columns(pl.lit(0, dtype=pl.Int32).alias("persistence"))

    cal_window = int(window * 1.5)
    dates = events.sort("date")["date"].to_list()

    # For each event, count events in trailing window
    counts = []
    date_set = sorted(set(dates))

    for d in dates:
        from datetime import timedelta
        start = d - timedelta(days=cal_window)
        n = sum(1 for dd in date_set if start <= dd <= d)
        counts.append(n)

    return events.with_columns(
        pl.Series("persistence", counts, dtype=pl.Int32)
    )
