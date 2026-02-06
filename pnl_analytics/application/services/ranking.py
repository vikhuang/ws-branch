"""Ranking Service: Generate broker ranking reports.

Orchestrates the full ranking report generation:
1. Load all required data via repositories
2. Analyze each broker using BrokerAnalyzer
3. Generate sorted ranking DataFrame
4. Export to various formats (CSV, Parquet, Excel)

This service coordinates repositories and domain logic
to produce the final ranking report.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl

from pnl_analytics.infrastructure import (
    DataPaths,
    DEFAULT_PATHS,
    TradeRepository,
    ClosedTradeRepository,
    PriceRepository,
    BrokerRepository,
    IndexMapRepository,
    PnlRepository,
)
from pnl_analytics.domain.returns import calculate_daily_returns
from pnl_analytics.domain.metrics import add_alpha_columns
from pnl_analytics.application.services.broker_analysis import (
    BrokerAnalyzer,
    BrokerAnalysisResult,
)


# =============================================================================
# Report Configuration
# =============================================================================

@dataclass(frozen=True)
class RankingReportConfig:
    """Configuration for ranking report generation.

    Attributes:
        min_trading_days: Minimum trading days for timing analysis
        permutation_count: Number of permutations for p-value
        output_dir: Directory for output files
        output_formats: List of formats ("csv", "parquet", "xlsx")
    """
    min_trading_days: int = 20
    permutation_count: int = 200
    output_dir: Path = Path(".")
    output_formats: tuple[str, ...] = ("csv", "parquet")


# =============================================================================
# Ranking Service
# =============================================================================

class RankingService:
    """Service for generating broker ranking reports.

    Coordinates data loading, analysis, and output generation.

    Example:
        >>> service = RankingService()
        >>> df = service.generate_report()
        >>> service.save_report(df, "ranking_report")
    """

    # Column order for output
    REPORT_COLUMNS = [
        "rank",
        "broker",
        "name",
        "direction",
        "total_pnl",
        "realized_pnl",
        "unrealized_pnl",
        "exec_alpha",
        "timing_alpha",
        "p_value",
        "timing_significance",
        "lead_corr",
        "lag_corr",
        "style",
        "trading_days",
        "total_volume",
        "total_amount",
        "cumulative_net",
        "trade_count",
    ]

    def __init__(
        self,
        paths: DataPaths = DEFAULT_PATHS,
        config: RankingReportConfig | None = None,
    ):
        """Initialize the service.

        Args:
            paths: Data paths configuration
            config: Report configuration (uses defaults if not provided)
        """
        self._paths = paths
        self._config = config or RankingReportConfig()

        # Initialize repositories
        self._trade_repo = TradeRepository(paths)
        self._closed_repo = ClosedTradeRepository(paths)
        self._price_repo = PriceRepository(paths)
        self._broker_repo = BrokerRepository(paths)
        self._index_repo = IndexMapRepository(paths)
        self._pnl_repo = PnlRepository(paths)

    def generate_report(
        self,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """Generate the full ranking report.

        Args:
            progress_callback: Optional callback(current, total) for progress

        Returns:
            DataFrame with all broker rankings and metrics
        """
        # Load all data
        trade_df = self._trade_repo.get_all()
        closed_trades = self._closed_repo.get_all()
        price_df = self._price_repo.get_all()
        broker_names = self._broker_repo.get_all()
        index_maps = self._index_repo.get_all()
        realized, unrealized = self._pnl_repo.get_all()

        # Calculate returns and prepare data
        daily_returns = calculate_daily_returns(price_df)
        price_dict = self._price_repo.get_price_dict()
        all_dates = sorted(index_maps["dates"].keys())

        # Add alpha columns to closed trades
        closed_with_alpha = add_alpha_columns(closed_trades, price_dict)

        # Create analyzer
        analyzer = BrokerAnalyzer(
            trade_df=trade_df,
            closed_trades_with_alpha=closed_with_alpha,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            broker_index_map=index_maps["brokers"],
            daily_returns=daily_returns,
            all_dates=all_dates,
            min_trading_days=self._config.min_trading_days,
            permutation_count=self._config.permutation_count,
        )

        # Analyze all brokers
        brokers = list(index_maps["brokers"].keys())
        total = len(brokers)
        results = []

        for i, broker in enumerate(brokers):
            if progress_callback:
                progress_callback(i, total)

            name = broker_names.get(broker, "")
            result = analyzer.analyze(broker, name=name)
            if result:
                results.append(result.to_dict())

        # Create DataFrame
        df = pl.DataFrame(results)

        # Sort by total_pnl descending and add rank
        df = df.sort("total_pnl", descending=True)
        df = df.with_row_index("rank", offset=1)

        # Reorder columns
        available_cols = [c for c in self.REPORT_COLUMNS if c in df.columns]
        df = df.select(available_cols)

        return df

    def save_report(
        self,
        df: pl.DataFrame,
        base_name: str = "ranking_report",
        formats: tuple[str, ...] | None = None,
    ) -> list[Path]:
        """Save report to specified formats.

        Args:
            df: Report DataFrame
            base_name: Base filename without extension
            formats: Output formats (uses config if not provided)

        Returns:
            List of saved file paths
        """
        formats = formats or self._config.output_formats
        output_dir = self._config.output_dir
        saved = []

        for fmt in formats:
            path = output_dir / f"{base_name}.{fmt}"

            if fmt == "csv":
                df.write_csv(path)
            elif fmt == "parquet":
                df.write_parquet(path)
            elif fmt == "xlsx":
                self._save_excel(df, path)
            else:
                raise ValueError(f"Unknown format: {fmt}")

            saved.append(path)

        return saved

    def _save_excel(self, df: pl.DataFrame, path: Path) -> None:
        """Save report to Excel with formatted sheets.

        Creates two sheets:
        1. 摘要 (Summary) - Key columns for quick review
        2. 完整報告 (Full Report) - All columns
        """
        import xlsxwriter

        workbook = xlsxwriter.Workbook(str(path))

        # Sheet 1: Summary
        ws1 = workbook.add_worksheet("摘要")
        summary_cols = [
            "rank", "broker", "name", "total_pnl",
            "realized_pnl", "unrealized_pnl"
        ]
        self._write_sheet(workbook, ws1, df, summary_cols)

        # Sheet 2: Full Report
        ws2 = workbook.add_worksheet("完整報告")
        self._write_sheet(workbook, ws2, df, df.columns)

        workbook.close()

    def _write_sheet(
        self,
        workbook,
        worksheet,
        df: pl.DataFrame,
        columns: list[str],
    ) -> None:
        """Write DataFrame columns to Excel worksheet."""
        # Header format
        header_fmt = workbook.add_format({
            "bold": True,
            "bg_color": "#4472C4",
            "font_color": "white",
            "border": 1,
        })

        # Number format for PNL (億元)
        pnl_fmt = workbook.add_format({"num_format": "#,##0.00"})
        pct_fmt = workbook.add_format({"num_format": "0.0000%"})

        # Write headers
        for col_idx, col_name in enumerate(columns):
            worksheet.write(0, col_idx, col_name, header_fmt)

        # Write data
        for row_idx, row in enumerate(df.select(columns).iter_rows(named=True), 1):
            for col_idx, col_name in enumerate(columns):
                value = row[col_name]

                # Format based on column type
                if col_name in ("total_pnl", "realized_pnl", "unrealized_pnl"):
                    # Convert to 億元
                    if value is not None:
                        worksheet.write(row_idx, col_idx, value / 1e8, pnl_fmt)
                    else:
                        worksheet.write(row_idx, col_idx, "")
                elif col_name in ("exec_alpha", "lead_corr", "lag_corr"):
                    if value is not None:
                        worksheet.write(row_idx, col_idx, value, pct_fmt)
                    else:
                        worksheet.write(row_idx, col_idx, "")
                else:
                    if value is not None:
                        worksheet.write(row_idx, col_idx, value)
                    else:
                        worksheet.write(row_idx, col_idx, "")

        # Adjust column widths
        for col_idx, col_name in enumerate(columns):
            width = max(len(col_name), 10)
            worksheet.set_column(col_idx, col_idx, width)

    def get_market_stats(self) -> dict:
        """Get market-level statistics.

        Returns:
            Dict with market return, date range, etc.
        """
        price_df = self._price_repo.get_all()
        dates = self._index_repo.get_dates()

        first_price = self._price_repo.get_first_price()
        last_price = self._price_repo.get_last_price()
        market_return = (last_price - first_price) / first_price

        return {
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
            "first_price": first_price,
            "last_price": last_price,
            "market_return": market_return,
            "trading_days": len(dates),
        }

    def analyze_single_broker(self, broker: str) -> BrokerAnalysisResult | None:
        """Analyze a single broker (convenience method).

        Args:
            broker: Broker code

        Returns:
            BrokerAnalysisResult or None if not found
        """
        # Load required data
        trade_df = self._trade_repo.get_all()
        closed_trades = self._closed_repo.get_all()
        price_dict = self._price_repo.get_price_dict()
        broker_names = self._broker_repo.get_all()
        index_maps = self._index_repo.get_all()
        realized, unrealized = self._pnl_repo.get_all()
        daily_returns = calculate_daily_returns(self._price_repo.get_all())
        all_dates = sorted(index_maps["dates"].keys())

        closed_with_alpha = add_alpha_columns(closed_trades, price_dict)

        analyzer = BrokerAnalyzer(
            trade_df=trade_df,
            closed_trades_with_alpha=closed_with_alpha,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            broker_index_map=index_maps["brokers"],
            daily_returns=daily_returns,
            all_dates=all_dates,
            min_trading_days=self._config.min_trading_days,
            permutation_count=self._config.permutation_count,
        )

        return analyzer.analyze(broker, name=broker_names.get(broker, ""))
