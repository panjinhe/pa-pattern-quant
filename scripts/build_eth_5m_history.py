from __future__ import annotations

import argparse
import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import polars as pl


LOCAL_SOURCE_GLOB = (
    r"E:\Skygarden\data\binance_um_perp\parquet\dataset=klines\symbol=ETHUSDT"
    r"\interval=5m\year=*\month=*\data.parquet"
)
REMOTE_BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines/ETHUSDT/5m"
DEFAULT_OUTPUT = Path(
    r"E:\pa-pattern-quant\data\binance_um_perp\ETHUSDT\5m\ETHUSDT-5m-history.parquet"
)
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> "YearMonth":
        year_str, month_str = value.split("-", maxsplit=1)
        return cls(year=int(year_str), month=int(month_str))

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def next(self) -> "YearMonth":
        if self.month == 12:
            return YearMonth(self.year + 1, 1)
        return YearMonth(self.year, self.month + 1)

    def previous(self) -> "YearMonth":
        if self.month == 1:
            return YearMonth(self.year - 1, 12)
        return YearMonth(self.year, self.month - 1)

    @property
    def month_start(self) -> str:
        return f"{self.year:04d}-{self.month:02d}-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a consolidated ETHUSDT 5m history file from Binance monthly archives and local Skygarden parquet."
    )
    parser.add_argument(
        "--start-month",
        default="2022-01",
        help="Earliest month to include, in YYYY-MM format.",
    )
    parser.add_argument(
        "--source-glob",
        default=LOCAL_SOURCE_GLOB,
        help="Glob path for local Skygarden parquet partitions.",
    )
    parser.add_argument(
        "--remote-base-url",
        default=REMOTE_BASE_URL,
        help="Base URL for Binance monthly archive downloads.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output parquet path.",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        help="Optional CSV output path. Use only when downstream tooling requires CSV.",
    )
    return parser.parse_args()


def month_range(start: YearMonth, end: YearMonth) -> list[YearMonth]:
    months: list[YearMonth] = []
    current = start
    while current <= end:
        months.append(current)
        current = current.next()
    return months


def normalize_remote_month(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select(KLINE_COLUMNS)
        .with_columns(
            pl.from_epoch("open_time", time_unit="ms").alias("open_time"),
            pl.from_epoch("close_time", time_unit="ms").alias("close_time"),
            pl.col(
                [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                ]
            ).cast(pl.Float64),
            pl.col("count").cast(pl.Int64),
            pl.col("ignore").cast(pl.Int32),
        )
        .sort("open_time")
    )


def normalize_local_partitions(source_glob: str) -> pl.DataFrame:
    lf = pl.scan_parquet(source_glob)
    schema = lf.collect_schema()
    columns = set(schema.names())

    count_expr = (
        pl.col("count").cast(pl.Int64).alias("count")
        if "count" in columns
        else pl.col("trade_count").cast(pl.Int64).alias("count")
    )
    ignore_expr = (
        pl.col("ignore").cast(pl.Int32).alias("ignore")
        if "ignore" in columns
        else pl.lit(0).cast(pl.Int32).alias("ignore")
    )

    return (
        lf.select(
            [
                pl.col("open_time").dt.replace_time_zone(None).alias("open_time"),
                pl.col("open").cast(pl.Float64).alias("open"),
                pl.col("high").cast(pl.Float64).alias("high"),
                pl.col("low").cast(pl.Float64).alias("low"),
                pl.col("close").cast(pl.Float64).alias("close"),
                pl.col("volume").cast(pl.Float64).alias("volume"),
                pl.col("close_time").dt.replace_time_zone(None).alias("close_time"),
                pl.col("quote_volume").cast(pl.Float64).alias("quote_volume"),
                count_expr,
                pl.col("taker_buy_volume").cast(pl.Float64).alias("taker_buy_volume"),
                pl.col("taker_buy_quote_volume").cast(pl.Float64).alias("taker_buy_quote_volume"),
                ignore_expr,
            ]
        )
        .sort("open_time")
        .collect()
    )


def remote_url(base_url: str, month: YearMonth) -> str:
    stem = f"ETHUSDT-5m-{month}"
    return f"{base_url}/{stem}.zip"


def download_remote_month(base_url: str, month: YearMonth) -> pl.DataFrame:
    url = remote_url(base_url, month)
    try:
        with urlopen(url, timeout=60) as response:
            payload = response.read()
    except HTTPError as exc:
        raise RuntimeError(f"download failed for {month}: {url}") from exc

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_name = archive.namelist()[0]
        with archive.open(csv_name) as fp:
            df = pl.read_csv(
                fp,
                has_header=True,
                schema_overrides={
                    "open_time": pl.Int64,
                    "close_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Float64,
                    "quote_volume": pl.Float64,
                    "count": pl.Int64,
                    "taker_buy_volume": pl.Float64,
                    "taker_buy_quote_volume": pl.Float64,
                    "ignore": pl.Int32,
                },
            )
    return normalize_remote_month(df)


def build_history(args: argparse.Namespace) -> pl.DataFrame:
    local_df = normalize_local_partitions(args.source_glob)
    local_start = local_df["open_time"].min()
    local_start_month = YearMonth(local_start.year, local_start.month)
    requested_start = YearMonth.parse(args.start_month)

    remote_months: list[YearMonth] = []
    if requested_start < local_start_month:
        remote_months = month_range(requested_start, local_start_month.previous())

    remote_frames: list[pl.DataFrame] = []
    for month in remote_months:
        print(f"downloading {month} ...")
        remote_frames.append(download_remote_month(args.remote_base_url, month))

    combined = pl.concat([*remote_frames, local_df], how="vertical_relaxed")
    return combined.unique(subset=["open_time"], keep="last").sort("open_time")


def summarize(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.len().alias("rows"),
        pl.col("open_time").min().alias("min_open_time"),
        pl.col("open_time").max().alias("max_open_time"),
    )


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.csv_output:
        args.csv_output.parent.mkdir(parents=True, exist_ok=True)

    df = build_history(args)
    print(summarize(df))

    df.write_parquet(args.output)
    print(f"saved parquet: {args.output}")

    if args.csv_output:
        df.write_csv(args.csv_output)
        print(f"saved csv: {args.csv_output}")


if __name__ == "__main__":
    main()
