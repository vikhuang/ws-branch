"""Event deduplication: remove overlapping events within hold periods.

When events for the same symbol fire more frequently than the hold period,
downstream backtests create overlapping positions that inflate Sharpe.
This module suppresses events that fall within the hold period of a
preceding event.

Pure functions — input/output are polars DataFrames.
"""

import polars as pl


def dedup_overlapping_events(
    events: pl.DataFrame,
    hold_days: int,
) -> pl.DataFrame:
    """Remove events that overlap with a preceding event's hold period.

    For each (symbol, direction) group, if event B fires within `hold_days`
    trading days of event A, event B is suppressed. Only the first event
    in each hold window survives.

    Uses calendar days as approximation (hold_days × 1.5 for weekends).

    Args:
        events: DataFrame with columns [symbol, date, direction].
        hold_days: Hold period in trading days.

    Returns:
        Filtered DataFrame with overlapping events removed.
    """
    if len(events) == 0 or hold_days <= 0:
        return events

    # Calendar day gap ≈ trading days × 1.5 (conservative, keeps slightly more)
    calendar_gap = int(hold_days * 1.5)

    sorted_events = events.sort("symbol", "date")
    keep_mask = _compute_keep_mask(sorted_events, calendar_gap)
    return sorted_events.filter(keep_mask)


def _compute_keep_mask(
    events: pl.DataFrame, calendar_gap: int,
) -> list[bool]:
    """Compute boolean mask: True = keep, False = suppress.

    Greedy forward scan per (symbol, direction) group.
    """
    symbols = events["symbol"].to_list()
    dates = events["date"].to_list()
    directions = events["direction"].to_list()

    n = len(events)
    keep = [False] * n

    # Track last kept event date per (symbol, direction)
    last_kept: dict[tuple[str, int], object] = {}

    for i in range(n):
        key = (symbols[i], directions[i])
        prev_date = last_kept.get(key)

        if prev_date is None or (dates[i] - prev_date).days >= calendar_gap:
            keep[i] = True
            last_kept[key] = dates[i]

    return keep
