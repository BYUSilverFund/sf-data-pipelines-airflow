import datetime as dt
import io
import time
import xml.etree.ElementTree as ET

import dateutil.relativedelta as du
import pandas_market_calendars as mcal
import polars as pl
import requests
import tqdm


def _get_trading_days(
    start_date: dt.date, end_date: dt.date, calendar_name="NYSE"
) -> list[dt.date]:
    """
    Retrieve all valid trading days between two dates for a given market calendar.

    Args:
        start_date (datetime.date): The first date to include in the search range.
        end_date (datetime.date): The last date to include in the search range.
        calendar_name (str, optional): The market calendar identifier used by
            `pandas_market_calendars` (e.g., 'NYSE', 'NASDAQ'). Defaults to 'NYSE'.
    """
    # Get market schedule
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(
        start_date=start_date.isoformat(), end_date=end_date.isoformat()
    )

    # Convert to list of datetime.date
    schedule_pl = pl.from_pandas(schedule)
    trading_days = schedule_pl["market_open"].cast(pl.Date).sort().to_list()
    return trading_days


def _get_trading_date_intervals(
    start_date: dt.date, end_date: dt.date, cal_name="NYSE"
):
    """
    Splits the range from start_date to end_date into month-sized intervals,
    but aligns both interval start and end to valid trading days.
    """
    trading_days = _get_trading_days(start_date, end_date, cal_name)

    chunks = []
    current_start = trading_days[0]

    while current_start <= end_date:
        # Determine calendar month end
        month_end = (
            current_start.replace(day=1)
            + du.relativedelta(months=1)
            - dt.timedelta(days=1)
        )

        # Filter trading days <= month_end
        possible = [d for d in trading_days if d <= month_end and d >= current_start]
        if not possible:
            break
        chunk_end = possible[-1]
        chunks.append((current_start, chunk_end))

        # Advance to first trading day after chunk_end
        next_possible = [d for d in trading_days if d > chunk_end]
        if not next_possible:
            break
        current_start = next_possible[0]

    return chunks


def ibkr_query(
    token: int,
    query_id: str,
    from_date: dt.date,
    to_date: dt.date,
    flex_version: int = 3,
) -> pl.DataFrame:
    """Function for pulling data from IBKR.

    Args:
        token (int): Token from IBKR flex query dashboard (expires annualy).
        query_id (str): ID from IBKR flex query dashboard.
        from_Date (dt.date): Date from which to start the query.
        to_date (dt.date): Date from which to end the query.
        flex_version (int): Version of Flex Query Service to use (we use version 3).
    """
    from_date = from_date.strftime("%Y%m%d")
    to_date = to_date.strftime("%Y%m%d")

    # Step 1: get reference code from SendRequest endpoint
    request_base = (
        "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
    )

    send_slug = "/SendRequest"
    send_params = {
        "t": token,
        "q": query_id,
        "v": flex_version,
        "fd": from_date,
        "td": to_date,
    }

    send_response = requests.get(url=request_base + send_slug, params=send_params)

    # Validate http status code
    if send_response.status_code != 200:
        # Do not include the provider response in the exception: task failures are
        # logged by Airflow and responses may contain sensitive request details.
        raise RuntimeError(
            f"IBKR SendRequest failed with HTTP {send_response.status_code}"
        )

    # Parse XML
    tree = ET.ElementTree(ET.fromstring(send_response.text))
    root = tree.getroot()

    # Validate XML status and get reference code
    status = root.findtext("Status")
    if status != "Success":
        error_code = root.findtext("ErrorCode")
        # Never log send_params: it contains the IBKR Flex token under ``t``.
        # Keep the provider code for diagnosis without emitting request/response
        # content that could contain sensitive data.
        raise RuntimeError(f"IBKR SendRequest failed (error code: {error_code})")

    reference_code = root.findtext("ReferenceCode")

    # Step 2: get statement from GetStatement endpoint
    time.sleep(1)

    receive_slug = "/GetStatement"
    receive_params = {
        "t": token,
        "q": reference_code,
        "v": flex_version,
        "fd": from_date,
        "td": to_date,
    }

    # Check every 5 seconds if the report is done generating.
    while True:
        receive_response = requests.get(
            url=request_base + receive_slug, params=receive_params, allow_redirects=True
        )

        if receive_response.status_code != 200:
            raise Exception("Failed to send request:", receive_response.text)

        # CSV response
        if not receive_response.text.startswith("<"):
            break

        # XML response
        else:
            tree = ET.ElementTree(ET.fromstring(receive_response.text))
            root = tree.getroot()

            error_code = root.findtext("ErrorCode")
            error_message = root.findtext("ErrorMessage")

            match error_code:
                case "1019" | "1021":
                    print(f"{error_code} status code. Waiting 5 seconds.")
                    time.sleep(5)

                case _:
                    raise Exception(
                        "Failed to send request:", error_code, error_message
                    )

    # Parse results as polars dataframe
    df = pl.read_csv(io.StringIO(receive_response.text), infer_schema_length=10000)

    return df


def ibkr_query_batches(
    token: int,
    query_id: str,
    from_date: dt.date,
    to_date: dt.date,
    flex_version: int = 3,
) -> pl.DataFrame:
    """Function for pulling data from IBKR in batches.

    IBKR allows for pulling data from up to a year ago.
    Due to the slowness of IBKR to generate year long reports,
    we chunk our API requests into 1 month queries.

    IBKR Docs: https://www.ibkrguides.com/clientportal/performanceandstatements/flex3.htm

    Args:
        token (int): Token from IBKR flex query dashboard (expires annualy).
        query_id (str): ID from IBKR flex query dashboard.
        from_date (dt.date): Date from which to start the query.
        to_date (dt.date): Date from which to end the query.
        flex_version (int): Version of Flex Query Service to use (we use version 3).
    """
    # Handle 1-year IBKR API retention limit
    min_start_date = dt.date.today() - du.relativedelta(years=1)
    min_start_date = min_start_date.replace(day=1) + du.relativedelta(months=1)

    if to_date < min_start_date:
        # Entire requested range is older than 1 year; skip API call
        return pl.DataFrame()

    if from_date < min_start_date:
        # Clamp start date to the earliest date available in IBKR API
        from_date = min_start_date

    max_end_date = dt.date.today() - du.relativedelta(days=1)
    if to_date > max_end_date:
        to_date = max_end_date

    if from_date > to_date:
        return pl.DataFrame()

    # Get date intervals
    date_intervals = _get_trading_date_intervals(from_date, to_date)

    df_list = []
    for f_date, t_date in tqdm.tqdm(date_intervals, desc="Pulling data from IBKR"):
        df = ibkr_query(token, query_id, f_date, t_date, flex_version)
        if df is not None and not df.is_empty():
            df_list.append(df)
        time.sleep(5)  # Prevents rate limiting

    if not df_list:
        return pl.DataFrame()

    return pl.concat(df_list)


def get_last_market_date(
    exchange: str = "NYSE", reference_date: dt.date = None
) -> dt.date:
    """
    Returns the most recent trading day on or before reference_date for the given exchange.

    Args:
        exchange (str): Exchange calendar name (default 'NYSE').
        ref_date (datetime.date): Reference date. If None, uses today.

    Returns:
        datetime.date: The last market trading day on or before ref_date.
    """
    if reference_date is None:
        reference_date = dt.date.today()

    calendar = mcal.get_calendar(exchange)

    # Get schedule from some time in the past up to ref_date
    start_date = reference_date - du.relativedelta(
        days=30
    )  # 30 days back as a safe buffer
    schedule = calendar.schedule(
        start_date=start_date.isoformat(), end_date=reference_date.isoformat()
    )

    # The market dates are the index of the schedule DataFrame (as Timestamps)
    market_days = schedule.index.to_pydatetime()

    # Filter only dates <= ref_date (in case schedule includes future dates)
    valid_days = [d.date() for d in market_days if d.date() <= reference_date]

    if not valid_days:
        raise ValueError(f"No market days found before or on {reference_date}")

    # Return the max date (last trading day)
    return max(valid_days)
