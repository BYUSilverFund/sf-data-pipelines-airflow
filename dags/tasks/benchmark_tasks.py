import datetime as dt
import logging
import os

import config
import dotenv
import pandas_market_calendars as mcal
import polars as pl
import requests
import yfinance as yf
from airflow.sdk import task
from aws.rds import db

dotenv.load_dotenv(override=True)


def get_benchmark_data(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    column_mapping = {
        "Date": "date",
        "Ticker": "ticker",
        "Close": "adjusted_close",
        "Dividends": "dividends_per_share",
    }

    query_end = end_date + dt.timedelta(days=1)
    data = yf.download(
        tickers=["IWV"],
        start=(start_date - dt.timedelta(days=7)).isoformat(),
        end=query_end.isoformat(),
        actions=True,
    )
    if data.empty:
        raise ValueError(f"Yahoo returned no IWV price for {start_date} to {end_date}")
    df = pl.from_pandas(data.stack(future_stack=True).reset_index())
    invalid_prices = df.filter(pl.col("Close").is_null() | (pl.col("Close") <= 0))
    if not invalid_prices.is_empty():
        raise ValueError(
            f"Yahoo returned {invalid_prices.height} invalid IWV closing prices"
        )
    last_completed_date = min(end_date, dt.date.today() - dt.timedelta(days=1))
    if start_date <= last_completed_date:
        schedule = mcal.get_calendar("NYSE").schedule(
            start_date=start_date.isoformat(),
            end_date=last_completed_date.isoformat(),
        )
        expected_dates = set(schedule.index.date)
        received_dates = set(df.get_column("Date").cast(pl.Date).to_list())
        missing_dates = sorted(expected_dates - received_dates)

    if missing_dates:
        missing_date_strings = ", ".join(
            f"'{date.isoformat()}'" for date in missing_dates
        )

        fallback = db.read_sql(
            f"""
            SELECT
                date AS "Date",
                ticker AS "Ticker",
                adjusted_close AS "Close",
                dividends_per_share AS "Dividends"
            FROM benchmark
            WHERE ticker = 'IWV'
            AND date IN ({missing_date_strings})
            """
        )

        fallback_dates = (
            set(fallback.get_column("Date").cast(pl.Date).to_list())
            if not fallback.is_empty()
            else set()
        )

        still_missing = sorted(set(missing_dates) - fallback_dates)

        if still_missing:
            raise ValueError(
                "IWV prices missing from both Yahoo and benchmark DB for market dates: "
                + ", ".join(map(str, still_missing))
            )

        df = pl.concat(
            [
                df,
                fallback.with_columns(
                    pl.col("Date").cast(df.schema["Date"]),
                    pl.col("Close").cast(df.schema["Close"]),
                    pl.col("Dividends").cast(df.schema["Dividends"]),
                ),
            ],
            how="diagonal_relaxed",
        ).sort("Date")
    requested_rows = df.filter(
        pl.col("Date").cast(pl.Date).is_between(start_date, end_date)
    )
    earlier_rows = df.filter(pl.col("Date").cast(pl.Date) < start_date)
    if not requested_rows.is_empty() and earlier_rows.is_empty():
        raise ValueError(
            f"Yahoo returned no prior IWV price to calculate the return for {start_date}"
        )
    return (
        df.select(column_mapping.keys())
        .rename(column_mapping)
        .sort("date")
        .with_columns(
            pl.col("date").dt.date(),
            pl.col("adjusted_close").pct_change().alias("return"),
        )
        .filter(pl.col("date").is_between(start_date, end_date))
        .with_columns(pl.col("return").fill_null(0.0))
        .drop_nulls("return")
        .sort("date")
    )


logger = logging.getLogger(__name__)

_EMPTY_INTRADAY_BM = pl.DataFrame(
    schema={"bm_timestamp": pl.Datetime("us"), "benchmark_price": pl.Float64}
)


def get_intraday_benchmark_bars(
    start_date: dt.date, end_date: dt.date, ticker: str = "IWV"
) -> pl.DataFrame:
    """Fetch 1-minute intraday benchmark bars from Alpaca Markets API.

    Returns a Polars DataFrame with columns:
    - bm_timestamp (pl.Datetime("us")): Naive datetime in US/Eastern (matching IBKR)
    - benchmark_price (pl.Float64)
    """
    api_key = os.getenv("APCA_API_KEY")
    secret_key = os.getenv("APCA_API_SECRET_KEY")

    if not api_key or not secret_key:
        logger.warning("APCA_API_KEY or APCA_API_SECRET_KEY not found in environment.")
        return _EMPTY_INTRADAY_BM

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "accept": "application/json",
    }

    url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"
    query_end = end_date + dt.timedelta(days=1)
    feed = os.getenv("ALPACA_FEED", "iex")

    all_bars = []
    page_token = None

    while True:
        params = {
            "timeframe": "1Min",
            "start": start_date.isoformat(),
            "end": query_end.isoformat(),
            "feed": feed,
            "adjustment": "all",
            "limit": 10000,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                logger.error("Alpaca API error (%s): %s", resp.status_code, resp.text)
                break

            data = resp.json()
            bars = data.get("bars") or []
            all_bars.extend(bars)

            page_token = data.get("next_page_token")
            if not page_token:
                break
        except Exception as e:
            logger.error("Failed to fetch bars from Alpaca: %s", e)
            break

    if not all_bars:
        return _EMPTY_INTRADAY_BM

    timestamps = [b["t"] for b in all_bars]
    prices = [float(b["c"]) for b in all_bars]

    return (
        pl.DataFrame({"bm_timestamp": timestamps, "benchmark_price": prices})
        .with_columns(
            pl.col("bm_timestamp")
            .str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
            .dt.convert_time_zone("America/New_York")
            .dt.replace_time_zone(None)
            .cast(pl.Datetime("us"))
        )
        .sort("bm_timestamp")
    )


@task(task_id="benchmark_etl")
def benchmark_etl_daily() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Create core table if not exists
    db.execute_sql_file("dags/sql/benchmark_create.sql")

    # 2. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 3. Load into stage table
    stage_table = f"{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="benchmark_etl")
def benchmark_etl_backfill(from_date: dt.date, to_date: dt.date) -> None:
    # 1. Create core table if not exists
    db.execute_sql_file("dags/sql/benchmark_create.sql")
    
    # 2. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="benchmark_etl")
def benchmark_etl_reload() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Create core table if not exists
    db.execute_sql_file("dags/sql/benchmark_create.sql")
    
    # 2. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')
