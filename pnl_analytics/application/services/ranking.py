"""Ranking Service: Generate broker ranking reports.

Reads pre-aggregated broker_ranking.parquet and adds broker names.
Exports to various formats (CSV, Parquet, Excel).
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import (
    RankingRepository,
    BrokerRepository,
    RepositoryError,
)


@dataclass(frozen=True)
class RankingReportConfig:
    """Configuration for ranking report output.

    Attributes:
        output_dir: Directory for output files
        output_formats: Formats to export ("csv", "parquet", "xlsx")
    """
    output_dir: Path = Path(".")
    output_formats: tuple[str, ...] = ("csv", "parquet")


class RankingService:
    """Service for generating broker ranking reports.

    Example:
        >>> service = RankingService()
        >>> df = service.get_ranking()
        >>> service.save_report(df, "ranking_report")
    """

    def __init__(
        self,
        paths: DataPaths = DEFAULT_PATHS,
        config: RankingReportConfig | None = None,
    ):
        self._paths = paths
        self._config = config or RankingReportConfig()
        self._ranking_repo = RankingRepository(paths)
        self._broker_repo = BrokerRepository(paths)

    def get_ranking(self, with_names: bool = True) -> pl.DataFrame:
        """Get broker ranking with optional names.

        Args:
            with_names: Add broker names column

        Returns:
            DataFrame with broker rankings
        """
        df = self._ranking_repo.get_all()

        if with_names:
            try:
                broker_names = self._broker_repo.get_all()
                df = df.with_columns(
                    pl.col("broker")
                    .map_elements(lambda b: broker_names.get(b, ""), return_dtype=pl.Utf8)
                    .alias("name")
                )
                # Move name after broker
                cols = df.columns
                if "name" in cols:
                    name_idx = cols.index("name")
                    broker_idx = cols.index("broker")
                    if name_idx > broker_idx + 1:
                        cols.pop(name_idx)
                        cols.insert(broker_idx + 1, "name")
                        df = df.select(cols)
            except RepositoryError:
                pass

        return df

    def get_top(self, n: int = 10) -> pl.DataFrame:
        """Get top N brokers by PNL."""
        return self.get_ranking().head(n)

    def get_bottom(self, n: int = 10) -> pl.DataFrame:
        """Get bottom N brokers by PNL."""
        return self.get_ranking().tail(n)

    def get_broker(self, broker: str) -> pl.DataFrame | None:
        """Get ranking for a specific broker."""
        df = self.get_ranking().filter(pl.col("broker") == broker)
        return df if len(df) > 0 else None

    def save_report(
        self,
        df: pl.DataFrame | None = None,
        base_name: str = "ranking_report",
        formats: tuple[str, ...] | None = None,
    ) -> list[Path]:
        """Save report to specified formats.

        Args:
            df: DataFrame to save (defaults to full ranking)
            base_name: Base filename without extension
            formats: Output formats (uses config if not provided)

        Returns:
            List of saved file paths
        """
        if df is None:
            df = self.get_ranking()

        formats = formats or self._config.output_formats
        output_dir = self._config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
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
        """Save report to Excel with formatted columns."""
        import xlsxwriter

        workbook = xlsxwriter.Workbook(str(path))
        worksheet = workbook.add_worksheet("排名")

        # Formats
        header_fmt = workbook.add_format({
            "bold": True,
            "bg_color": "#4472C4",
            "font_color": "white",
            "border": 1,
        })
        pnl_fmt = workbook.add_format({"num_format": "#,##0"})
        pct_fmt = workbook.add_format({"num_format": "0.00%"})

        # Write headers
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name, header_fmt)

        # Write data
        for row_idx, row in enumerate(df.iter_rows(named=True), 1):
            for col_idx, col_name in enumerate(df.columns):
                value = row[col_name]
                if value is None:
                    worksheet.write(row_idx, col_idx, "")
                elif col_name in ("total_pnl", "realized_pnl", "unrealized_pnl",
                                  "total_buy_amount", "total_sell_amount", "total_amount"):
                    worksheet.write(row_idx, col_idx, value, pnl_fmt)
                elif col_name == "win_rate":
                    worksheet.write(row_idx, col_idx, value, pct_fmt)
                else:
                    worksheet.write(row_idx, col_idx, value)

        # Adjust column widths
        for col_idx, col_name in enumerate(df.columns):
            worksheet.set_column(col_idx, col_idx, max(len(col_name), 12))

        workbook.close()

    def get_summary(self) -> dict:
        """Get market-level summary statistics."""
        df = self._ranking_repo.get_all()

        return {
            "broker_count": len(df),
            "total_pnl": df["total_pnl"].sum(),
            "total_realized": df["realized_pnl"].sum(),
            "total_unrealized": df["unrealized_pnl"].sum(),
            "total_trades": df["trade_count"].sum(),
            "avg_win_rate": df["win_rate"].mean(),
            "top_pnl": df["total_pnl"].max(),
            "bottom_pnl": df["total_pnl"].min(),
        }
