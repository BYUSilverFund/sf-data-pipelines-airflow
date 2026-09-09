import datetime as dt

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
from slack_notifier import slack_on_failure

CONN_ID = "postgres_rds_conn"
default_args = {"on_failure_callback": slack_on_failure}


@dag(
    dag_id="data_integrity",
    schedule="30 10 * * *",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
)
def data_integrity_pipeline():
    # -------------------------------------------------------------------------
    # BASIC COLUMN INTEGRITY CHECKS (Task Group: table_structure_validation)
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="table_structure_validation") as table_structure_validation:
        SQLColumnCheckOperator(
            task_id="positions_columns",
            conn_id=CONN_ID,
            table="positions",
            column_mapping={
                "symbol": {"null_check": {"equal_to": 0}},
                "report_date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="trades_columns",
            conn_id=CONN_ID,
            table="trades",
            column_mapping={
                "symbol": {"null_check": {"equal_to": 0}},
                "report_date": {"null_check": {"equal_to": 0}},
                "trade_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="holding_returns_columns",
            conn_id=CONN_ID,
            table="holding_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "ticker": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="fund_returns_columns",
            conn_id=CONN_ID,
            table="fund_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="benchmark_columns",
            conn_id=CONN_ID,
            table="benchmark",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "ticker": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="dividends_columns",
            conn_id=CONN_ID,
            table="dividends",
            column_mapping={
                "report_date": {"null_check": {"equal_to": 0}},
                "symbol": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="delta_nav_columns",
            conn_id=CONN_ID,
            table="delta_nav",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="historical_data_columns",
            conn_id=CONN_ID,
            table="historical_data",
            column_mapping={
                "report_date": {"null_check": {"equal_to": 0}},
                "symbol": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="risk_free_rate_columns",
            conn_id=CONN_ID,
            table="risk_free_rate",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="calendar_columns",
            conn_id=CONN_ID,
            table="calendar",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="all_fund_returns_columns",
            conn_id=CONN_ID,
            table="all_fund_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )

    # -------------------------------------------------------------------------
    # COMPLEX MATHEMATICAL ALIGNMENT & CALCULATIONS (Business Logic)
    # -------------------------------------------------------------------------
    @task(task_id="delta_nav_vs_all_fund_returns")
    def delta_nav_vs_all_fund_returns():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Aggregates delta_nav by trading date (from calendar) and compares summed ending values (net of deposits/withdrawals) against all_fund_returns.
            -- Verifies (delta_nav.ending_value - delta_nav.deposits_withdrawals) on date_t equals all_fund_returns value on date_t.
            -- Automatically ignores non-market trading dates (weekends and holidays) and restricts to actual data bounds.
            WITH delta_nav_daily AS (
                SELECT 
                    date,
                    SUM(starting_value) AS sum_starting_value,
                    SUM(ending_value) AS sum_ending_value,
                    SUM(deposits_withdrawals) AS sum_deposits_withdrawals,
                    SUM(dividends) AS sum_dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            ),
            all_fund_daily AS (
                SELECT 
                    date,
                    value AS fund_value
                FROM all_fund_returns
            ),
            date_bounds AS (
                SELECT 
                    MIN(date) AS min_date,
                    MAX(date) AS max_date
                FROM delta_nav_daily
            ),
            trading_day_checks AS (
                SELECT 
                    c.date,
                    d.sum_starting_value,
                    d.sum_ending_value,
                    d.sum_deposits_withdrawals,
                    f.fund_value,
                    CASE WHEN d.sum_ending_value IS NOT NULL AND f.fund_value IS NOT NULL 
                         THEN ABS((d.sum_ending_value - d.sum_deposits_withdrawals) - f.fund_value) ELSE NULL END AS ending_val_diff
                FROM calendar c
                CROSS JOIN date_bounds b
                LEFT JOIN delta_nav_daily d ON c.date = d.date
                LEFT JOIN all_fund_daily f ON c.date = f.date
                WHERE c.date BETWEEN b.min_date AND b.max_date
                  -- Exclude fund inception date
                  AND c.date != '2020-07-17'
            )
            SELECT 
                date,
                COALESCE(sum_starting_value, 0) AS sum_starting_value,
                0 AS prev_fund_value,
                0 AS starting_val_diff,
                COALESCE(sum_ending_value - sum_deposits_withdrawals, 0) AS sum_ending_value,
                COALESCE(fund_value, 0) AS fund_value,
                COALESCE(ending_val_diff, 0) AS ending_val_diff,
                CASE 
                    WHEN sum_ending_value IS NULL THEN 'MISSING_IN_DELTA_NAV'
                    WHEN fund_value IS NULL THEN 'MISSING_IN_ALL_FUND_RETURNS'
                    WHEN ending_val_diff > 0.01 
                        THEN 'ENDING_VAL_MISMATCH (delta_nav.ending - deposits != all_fund.value)'
                END AS failure_reason
            FROM trading_day_checks
            WHERE sum_ending_value IS NULL 
               OR fund_value IS NULL
               OR ending_val_diff > 0.01
            ORDER BY date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      DELTA_NAV VS ALL_FUND_RETURNS VALUE ALIGNMENT MISMATCHES DETECTED",
                "=" * 90,
            ]
            for (
                date,
                sum_start,
                prev_fund,
                start_diff,
                sum_end,
                fund_val,
                end_diff,
                reason,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Reason: {reason}\n"
                    f"   -> StartingVal (delta_nav): {sum_start:,.2f} | PrevFundVal: {prev_fund:,.2f} | Diff: {start_diff:,.2f}\n"
                    f"   -> EndingVal (delta_nav): {sum_end:,.2f} | FundVal: {fund_val:,.2f} | Diff: {end_diff:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} dates where delta_nav starting/ending values do not match all_fund_returns!"
            )

    @task(task_id="all_fund_returns_math")
    def all_fund_returns_math():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies all_fund_returns daily return matches the formula:
            -- (ending_value - starting_value - deposits_withdrawals) / starting_value.
            WITH delta_nav_daily AS (
                SELECT 
                    date,
                    SUM(starting_value) AS sum_starting_value,
                    SUM(ending_value) AS sum_ending_value,
                    SUM(deposits_withdrawals) AS sum_deposits_withdrawals,
                    SUM(dividends) AS sum_dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            ),
            all_fund_joined AS (
                SELECT 
                    f.date,
                    f.value AS fund_value,
                    f.return AS actual_return,
                    d.sum_starting_value,
                    d.sum_ending_value,
                    d.sum_deposits_withdrawals,
                    d.sum_dividends,
                    CASE 
                        WHEN d.sum_starting_value IS NOT NULL AND d.sum_starting_value > 0
                        THEN (d.sum_ending_value - d.sum_starting_value - d.sum_deposits_withdrawals) / d.sum_starting_value
                        ELSE NULL
                    END AS expected_return
                FROM all_fund_returns f
                JOIN delta_nav_daily d ON f.date = d.date
            )
            SELECT 
                date,
                fund_value,
                sum_starting_value,
                sum_ending_value,
                sum_deposits_withdrawals,
                sum_dividends,
                actual_return,
                expected_return,
                ABS(actual_return - expected_return) AS diff
            FROM all_fund_joined
            -- Tolerance threshold (0.5%): Accounts for minor post-holiday starting value shifts
            -- (IBKR interest, FX, and settlement adjustments over multi-day closures) while catching true math errors.
            WHERE expected_return IS NOT NULL
              AND ABS(actual_return - expected_return) > 0.005
            ORDER BY date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ALL_FUND_RETURNS DAILY RETURN (WITH CASHFLOWS & DIVIDENDS) MISMATCHES",
                "=" * 90,
            ]
            for (
                date,
                fund_val,
                start_val,
                end_val,
                deposits,
                divs,
                act_ret,
                exp_ret,
                diff,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | FundVal (all_fund_returns): {fund_val:,.2f} | "
                    f"StartVal (delta_nav): {start_val:,.2f} | EndVal (delta_nav): {end_val:,.2f} | "
                    f"DepWith (delta_nav): {deposits:,.2f} | Divs (delta_nav): {divs:,.2f}\n"
                    f"   -> Actual Return (all_fund_returns): {act_ret:.6f} | "
                    f"Expected Return (delta_nav formula): {exp_ret:.6f} | Diff: {diff:.6f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows in all_fund_returns where daily return does not match delta_nav cashflow calculation!"
            )

    @task(task_id="all_fund_returns_dividends_match")
    def all_fund_returns_dividends_match():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies all_fund_returns dividends on each date equals sum(delta_nav.dividends) on that date.
            WITH delta_nav_divs AS (
                SELECT 
                    date,
                    SUM(dividends) AS sum_delta_nav_divs
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            )
            SELECT 
                f.date,
                f.dividends AS fund_dividends,
                d.sum_delta_nav_divs,
                ABS(f.dividends - d.sum_delta_nav_divs) AS diff
            FROM all_fund_returns f
            JOIN delta_nav_divs d ON f.date = d.date
            WHERE ABS(f.dividends - d.sum_delta_nav_divs) > 0.01
            ORDER BY f.date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ALL_FUND_RETURNS VS DELTA_NAV DIVIDENDS MISMATCHES DETECTED",
                "=" * 90,
            ]
            for date, fund_divs, delta_divs, diff in failing_rows:
                error_msg.append(
                    f"Date: {date} | AllFundReturns Divs: {fund_divs:,.2f} | DeltaNAV Sum Divs: {delta_divs:,.2f} | Diff: {diff:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} dates where all_fund_returns dividends do not match sum(delta_nav.dividends)!"
            )

    @task(task_id="benchmark_math")
    def benchmark_math():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies benchmark daily return per ticker matches: (adjusted_close_t - adjusted_close_{t-1}) / adjusted_close_{t-1}.
            WITH lag_values AS (
                SELECT 
                    date,
                    ticker,
                    adjusted_close,
                    return as actual_return,
                    LAG(adjusted_close) OVER (PARTITION BY ticker ORDER BY date) as prev_adj_close
                FROM benchmark
            )
            SELECT 
                date,
                ticker,
                adjusted_close,
                prev_adj_close,
                actual_return,
                ((adjusted_close - prev_adj_close) / prev_adj_close) as expected_return,
                ABS(actual_return - ((adjusted_close - prev_adj_close) / prev_adj_close)) as diff
            FROM lag_values
            WHERE prev_adj_close IS NOT NULL 
              AND prev_adj_close > 0
              AND ABS(actual_return - ((adjusted_close - prev_adj_close) / prev_adj_close)) > 0.0001
            ORDER BY ticker, date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 80,
                "      BENCHMARK DAILY RETURN MISMATCHES DETECTED",
                "=" * 80,
            ]
            for (
                date,
                ticker,
                adj_close,
                prev_close,
                act_ret,
                exp_ret,
                diff,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Ticker: {ticker} | AdjClose: {adj_close:,.2f} | PrevClose: {prev_close:,.2f} | "
                    f"Actual Return: {act_ret:.6f} | Expected Return: {exp_ret:.6f} | Diff: {diff:.6f}"
                )
            error_msg.append("=" * 80)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows in benchmark with invalid daily return calculations!"
            )

    @task(task_id="fund_returns_math")
    def fund_returns_math():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies fund_returns daily return per client_account_id matches delta_nav cashflow formula:
            -- (ending_value - starting_value - deposits_withdrawals) / starting_value.
            WITH delta_nav_acc AS (
                SELECT 
                    date,
                    client_account_id,
                    starting_value,
                    ending_value,
                    deposits_withdrawals,
                    dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
            ),
            fund_acc_joined AS (
                SELECT 
                    f.date,
                    f.client_account_id,
                    f.value AS fund_value,
                    f.return AS actual_return,
                    d.starting_value,
                    d.ending_value,
                    d.deposits_withdrawals,
                    d.dividends,
                    CASE 
                        WHEN d.starting_value IS NOT NULL AND d.starting_value > 0
                        THEN (d.ending_value - d.starting_value - d.deposits_withdrawals) / d.starting_value
                        ELSE NULL
                    END AS expected_return
                FROM fund_returns f
                JOIN calendar c ON f.date = c.date
                JOIN delta_nav_acc d ON f.date = d.date AND f.client_account_id = d.client_account_id
                WHERE f.client_account_id != 'DU8843649'
                  AND f.date != '2020-07-17'
            )
            SELECT 
                date,
                client_account_id,
                fund_value,
                starting_value,
                ending_value,
                deposits_withdrawals,
                actual_return,
                expected_return,
                ABS(actual_return - expected_return) AS diff
            FROM fund_acc_joined
            -- Tolerance threshold (1.0%): Accounts for minor post-holiday IBKR starting value shifts per account.
            WHERE expected_return IS NOT NULL
              AND ABS(actual_return - expected_return) > 0.01
            ORDER BY date DESC, client_account_id;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      FUND_RETURNS DAILY RETURN PER ACCOUNT MISMATCHES DETECTED",
                "=" * 90,
            ]
            for (
                date,
                account,
                fund_val,
                start_val,
                end_val,
                dep_with,
                act_ret,
                exp_ret,
                diff,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Account: {account} | FundVal: {fund_val:,.2f} | StartVal: {start_val:,.2f} | EndVal: {end_val:,.2f} | DepWith: {dep_with:,.2f}\n"
                    f"   -> Actual Return: {act_ret:.6f} | Expected Return: {exp_ret:.6f} | Diff: {diff:.6f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows in fund_returns where account daily return does not match delta_nav formula!"
            )

    @task(task_id="delta_nav_vs_fund_returns")
    def delta_nav_vs_fund_returns():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Compares delta_nav ending values (net of deposits/withdrawals) per client_account_id against fund_returns.
            -- Verifies (delta_nav.ending_value - delta_nav.deposits_withdrawals) on date_t equals fund_returns.value on date_t for each account.
            WITH delta_nav_acc AS (
                SELECT 
                    date,
                    client_account_id,
                    starting_value,
                    ending_value,
                    deposits_withdrawals,
                    dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
            ),
            fund_acc AS (
                SELECT 
                    date,
                    client_account_id,
                    value AS fund_value,
                    return AS fund_return
                FROM fund_returns
                WHERE client_account_id != 'DU8843649'
            ),
            all_account_dates AS (
                SELECT date, client_account_id FROM delta_nav_acc
                UNION
                SELECT date, client_account_id FROM fund_acc
            ),
            trading_day_checks AS (
                SELECT 
                    ad.date,
                    ad.client_account_id,
                    d.starting_value,
                    d.ending_value,
                    d.deposits_withdrawals,
                    f.fund_value,
                    CASE WHEN d.ending_value IS NOT NULL AND f.fund_value IS NOT NULL 
                         THEN ABS((d.ending_value - d.deposits_withdrawals) - f.fund_value) ELSE NULL END AS ending_val_diff
                FROM all_account_dates ad
                JOIN calendar c ON ad.date = c.date
                LEFT JOIN delta_nav_acc d ON ad.date = d.date AND ad.client_account_id = d.client_account_id
                LEFT JOIN fund_acc f ON ad.date = f.date AND ad.client_account_id = f.client_account_id
                WHERE ad.date != '2020-07-17'
            )
            SELECT 
                date,
                client_account_id,
                COALESCE(starting_value, 0) AS starting_value,
                COALESCE(ending_value - deposits_withdrawals, 0) AS expected_fund_value,
                COALESCE(fund_value, 0) AS fund_value,
                COALESCE(ending_val_diff, 0) AS ending_val_diff,
                CASE 
                    WHEN ending_value IS NULL THEN 'MISSING_IN_DELTA_NAV'
                    WHEN fund_value IS NULL THEN 'MISSING_IN_FUND_RETURNS'
                    WHEN ending_val_diff > 0.01 
                        THEN 'ENDING_VAL_MISMATCH (delta_nav.ending - deposits != fund.value)'
                END AS failure_reason
            FROM trading_day_checks
            WHERE ending_value IS NULL 
               OR fund_value IS NULL
               OR ending_val_diff > 0.01
            ORDER BY date DESC, client_account_id;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      DELTA_NAV VS FUND_RETURNS VALUE ALIGNMENT MISMATCHES DETECTED",
                "=" * 90,
            ]
            for (
                date,
                account,
                start_val,
                exp_fund,
                fund_val,
                diff,
                reason,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Account: {account} | Reason: {reason}\n"
                    f"   -> StartingVal (delta_nav): {start_val:,.2f} | ExpectedFundVal: {exp_fund:,.2f} | FundVal: {fund_val:,.2f} | Diff: {diff:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} account rows where delta_nav does not match fund_returns!"
            )

    @task(task_id="positions_vs_historical_symbols")
    def positions_vs_historical_symbols():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Deduplicates positions by (symbol, report_date) and joins to historical_data on symbol AND report_date
            -- across ALL historical dates to verify mark_price is identical.
            WITH pos_deduped AS (
                SELECT 
                    symbol,
                    report_date,
                    MAX(mark_price) AS mark_price
                FROM positions
                GROUP BY symbol, report_date
            ),
            hist_deduped AS (
                SELECT 
                    symbol,
                    report_date,
                    MAX(mark_price) AS mark_price
                FROM historical_data
                GROUP BY symbol, report_date
            ),
            joined AS (
                SELECT 
                    p.report_date,
                    p.symbol,
                    p.mark_price AS pos_mark_price,
                    h.mark_price AS hist_mark_price,
                    ABS(p.mark_price - h.mark_price) AS price_diff
                FROM pos_deduped p
                INNER JOIN hist_deduped h ON p.symbol = h.symbol AND p.report_date = h.report_date
            )
            SELECT 
                report_date,
                symbol,
                pos_mark_price,
                hist_mark_price,
                price_diff,
                'MARK_PRICE_MISMATCH' AS failure_reason
            FROM joined
            WHERE price_diff > 0
              AND (
                  (pos_mark_price > 0.01 AND (price_diff / pos_mark_price) > 0.05)
               OR (pos_mark_price <= 0.01 AND price_diff > 0.01)
              )
            ORDER BY report_date DESC, symbol;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      POSITIONS VS HISTORICAL_DATA MARK_PRICE MISMATCHES DETECTED",
                "=" * 90,
            ]
            for date, symbol, pos_price, hist_price, diff, reason in failing_rows:
                error_msg.append(
                    f"Date: {date} | Symbol: {symbol:<15} | PosPrice: {pos_price:,.2f} | HistPrice: {hist_price:,.2f} | Diff: {diff:,.2f} | Reason: {reason}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows where mark_price does not match between positions and historical_data!"
            )

    @task
    def trades_vs_positions_qty():
        """
        Reconciles daily position quantity changes (pos_qty_t - pos_qty_t-1)
        against total executed trade volume in trades table per account and symbol
        for recent trading days (within the last 14 days).
        Flags discrepancies caused by missing trades or unhandled corporate actions.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
            -- Ensures daily position quantity changes match the executed trade volume in trades table per account and symbol.
            WITH daily_pos AS (
                SELECT 
                    report_date,
                    client_account_id,
                    symbol,
                    SUM(quantity) AS pos_qty,
                    LAG(SUM(quantity)) OVER (PARTITION BY client_account_id, symbol ORDER BY report_date) AS prev_pos_qty,
                    LAG(report_date) OVER (PARTITION BY client_account_id, symbol ORDER BY report_date) AS prev_date
                FROM positions
                WHERE client_account_id != 'DU8843649'
                GROUP BY report_date, client_account_id, symbol
            ),
            daily_trades AS (
                SELECT 
                    report_date,
                    client_account_id,
                    symbol,
                    SUM(quantity) AS trade_qty
                FROM trades
                WHERE client_account_id != 'DU8843649'
                GROUP BY report_date, client_account_id, symbol
            ),
            reconciled AS (
                SELECT 
                    p.report_date,
                    p.prev_date,
                    p.client_account_id,
                    p.symbol,
                    p.prev_pos_qty,
                    p.pos_qty,
                    COALESCE(t.trade_qty, 0) AS trade_qty,
                    (p.pos_qty - COALESCE(p.prev_pos_qty, 0)) AS actual_qty_change
                FROM daily_pos p
                LEFT JOIN daily_trades t 
                  ON p.report_date = t.report_date 
                 AND p.client_account_id = t.client_account_id 
                 AND p.symbol = t.symbol
                WHERE p.prev_date IS NOT NULL
                  AND p.report_date >= CURRENT_DATE - INTERVAL '14 days'
            )
            SELECT 
                report_date,
                client_account_id,
                symbol,
                prev_pos_qty,
                pos_qty,
                actual_qty_change,
                trade_qty
            FROM reconciled
            WHERE actual_qty_change != trade_qty
              -- STOCK SPLIT HEURISTIC:
              -- Ignore discrepancies if the net position change aligns with an integer forward stock split (e.g., 2:1, 3:1, 4:1)
              -- or integer reverse stock split (e.g., 1-for-2, 1-for-5).
              -- NOTE: If dedicated corporate action / stock split tracking is implemented in the future, remove this heuristic.
              AND NOT (
                  -- Forward split check (pos_qty net of trades is an exact integer multiple >= 2 of prev_pos_qty)
                  (
                      ABS(prev_pos_qty) > 0 
                      AND SIGN(pos_qty - trade_qty) = SIGN(prev_pos_qty)
                      AND ABS(pos_qty - trade_qty) > ABS(prev_pos_qty)
                      AND MOD(ABS(pos_qty - trade_qty), ABS(prev_pos_qty)) = 0
                  )
                  OR
                  -- Reverse split check (prev_pos_qty is an exact integer multiple >= 2 of pos_qty net of trades)
                  (
                      ABS(pos_qty - trade_qty) > 0
                      AND SIGN(pos_qty - trade_qty) = SIGN(prev_pos_qty)
                      AND ABS(prev_pos_qty) > ABS(pos_qty - trade_qty)
                      AND MOD(ABS(prev_pos_qty), ABS(pos_qty - trade_qty)) = 0
                  )
              )
            ORDER BY report_date DESC, client_account_id, symbol;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      TRADES VS POSITIONS QUANTITY DISCREPANCIES DETECTED",
                "=" * 90,
            ]
            for (
                date,
                account,
                symbol,
                prev_qty,
                curr_qty,
                actual_change,
                trade_qty,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Account: {account} | Symbol: {symbol:<10} | PrevQty: {prev_qty} | CurrQty: {curr_qty} | QtyChange: {actual_change} | TradeQty: {trade_qty}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} position quantity discrepancies against executed trades!"
            )

    @task
    def orphan_dividends():
        """
        Ensures all client account IDs receiving dividend payouts in dividends table
        exist as registered active accounts in delta_nav.
        Flags orphan dividend transactions attributed to unknown/unmapped accounts.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
            -- Checks for dividend payouts attributed to client accounts not registered in delta_nav.
            SELECT DISTINCT
                d.report_date,
                d.client_account_id,
                d.symbol,
                d.net_amount
            FROM dividends d
            LEFT JOIN (
                SELECT DISTINCT client_account_id FROM delta_nav
            ) n ON d.client_account_id = n.client_account_id
            WHERE n.client_account_id IS NULL
              AND d.client_account_id != 'DU8843649'
            ORDER BY d.report_date DESC, d.client_account_id, d.symbol;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ORPHAN DIVIDENDS DETECTED (UNKNOWN CLIENT ACCOUNTS)",
                "=" * 90,
            ]
            for date, account, symbol, net_amount in failing_rows:
                error_msg.append(
                    f"Date: {date} | Account: {account:<15} | Symbol: {symbol:<10} | NetAmount: ${net_amount:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} orphan dividend entries for unregistered client accounts!"
            )

    @task
    def zero_negative_nav():
        """
        Scans delta_nav for any client account record where ending_value <= 0
        or starting_value <= 0 without a cash deposit (excluding demo account DU8843649).
        Flags account NAV anomalies that break return calculations.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
            -- Checks for zero or negative NAV entries in delta_nav that would break return calculations.
            SELECT 
                date,
                client_account_id,
                starting_value,
                ending_value,
                deposits_withdrawals,
                dividends
            FROM delta_nav
            WHERE (ending_value <= 0 OR (starting_value <= 0 AND deposits_withdrawals <= 0))
              AND client_account_id != 'DU8843649'
            ORDER BY date DESC, client_account_id;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ZERO OR NEGATIVE NAV ANOMALIES DETECTED IN DELTA_NAV",
                "=" * 90,
            ]
            for dt_val, account, start_val, end_val, dep_with, divs in failing_rows:
                error_msg.append(
                    f"Date: {dt_val} | Account: {account:<15} | StartNAV: ${start_val:,.2f} | EndNAV: ${end_val:,.2f} | Flow: ${dep_with:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} zero or negative NAV entries in delta_nav!"
            )

    @task
    def date_sync_across_tables():
        """
        1. Scans core tables for any future-dated records (report_date/date > CURRENT_DATE).
        2. Ensures data is up-to-date by comparing MAX(date) across positions, historical_data,
           holding_returns, fund_returns, all_fund_returns, and risk_free_rate against the latest
           trading calendar date (calendar.date).
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # 1. Future date check
        future_sql = """
            -- Checks for future-dated records across core portfolio tables.
            SELECT 'positions' AS table_name, COUNT(*) AS future_count FROM positions WHERE report_date > CURRENT_DATE
            UNION ALL
            SELECT 'trades', COUNT(*) FROM trades WHERE report_date > CURRENT_DATE
            UNION ALL
            SELECT 'dividends', COUNT(*) FROM dividends WHERE report_date > CURRENT_DATE
            UNION ALL
            SELECT 'delta_nav', COUNT(*) FROM delta_nav WHERE date > CURRENT_DATE
            UNION ALL
            SELECT 'historical_data', COUNT(*) FROM historical_data WHERE report_date > CURRENT_DATE
            UNION ALL
            SELECT 'holding_returns', COUNT(*) FROM holding_returns WHERE date > CURRENT_DATE
            UNION ALL
            SELECT 'fund_returns', COUNT(*) FROM fund_returns WHERE date > CURRENT_DATE
            UNION ALL
            SELECT 'all_fund_returns', COUNT(*) FROM all_fund_returns WHERE date > CURRENT_DATE
            UNION ALL
            SELECT 'risk_free_rate', COUNT(*) FROM risk_free_rate WHERE date > CURRENT_DATE
            UNION ALL
            SELECT 'benchmark', COUNT(*) FROM benchmark WHERE date > CURRENT_DATE;
        """
        future_rows = hook.get_records(future_sql)
        future_failures = [(tbl, cnt) for tbl, cnt in future_rows if cnt > 0]

        # 2. Freshness / max date synchronicity check against calendar table
        freshness_sql = """
            -- Compares max report dates across core portfolio tables against the latest calendar trading date.
            WITH max_dates AS (
                SELECT 
                    (SELECT MAX(date) FROM calendar) AS max_calendar,
                    (SELECT MAX(date) FROM calendar WHERE date < (SELECT MAX(date) FROM calendar)) AS prev_calendar,
                    (SELECT MAX(report_date) FROM positions) AS max_positions,
                    (SELECT MAX(report_date) FROM historical_data) AS max_historical_data,
                    (SELECT MAX(date) FROM holding_returns) AS max_holding_returns,
                    (SELECT MAX(date) FROM fund_returns) AS max_fund_returns,
                    (SELECT MAX(date) FROM all_fund_returns) AS max_all_fund_returns,
                    (SELECT MAX(date) FROM risk_free_rate) AS max_risk_free_rate
            )
            SELECT 
                max_calendar,
                prev_calendar,
                max_positions,
                max_historical_data,
                max_holding_returns,
                max_fund_returns,
                max_all_fund_returns,
                max_risk_free_rate,
                CASE WHEN max_positions != max_calendar THEN 'POSITIONS_STALE' END AS pos_err,
                CASE WHEN max_historical_data != max_calendar THEN 'HISTORICAL_DATA_STALE' END AS hist_err,
                CASE WHEN max_holding_returns != max_calendar THEN 'HOLDING_RETURNS_STALE' END AS hold_err,
                CASE WHEN max_fund_returns != max_calendar THEN 'FUND_RETURNS_STALE' END AS fund_err,
                CASE WHEN max_all_fund_returns != max_calendar THEN 'ALL_FUND_RETURNS_STALE' END AS all_fund_err,
                CASE WHEN max_risk_free_rate < prev_calendar THEN 'RISK_FREE_RATE_STALE' END AS rfr_err
            FROM max_dates;
        """
        freshness_row = hook.get_records(freshness_sql)[0]
        (
            max_cal,
            prev_cal,
            max_pos,
            max_hist,
            max_hold,
            max_fund,
            max_all_fund,
            max_rfr,
            pos_err,
            hist_err,
            hold_err,
            fund_err,
            all_fund_err,
            rfr_err,
        ) = freshness_row

        stale_errors = [
            err
            for err in [
                pos_err,
                hist_err,
                hold_err,
                fund_err,
                all_fund_err,
                rfr_err,
            ]
            if err is not None
        ]

        if future_failures or stale_errors:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      DATA FRESHNESS AND FUTURE DATE ANOMALIES DETECTED",
                "=" * 90,
            ]
            if future_failures:
                error_msg.append("FUTURE DATED RECORDS:")
                for tbl, cnt in future_failures:
                    error_msg.append(f"   -> Table {tbl:<20}: {cnt} future rows")
            if stale_errors:
                error_msg.append(
                    f"MAX DATE SYNCHRONICITY MISMATCHES (Latest Calendar Date: {max_cal}):"
                )
                error_msg.append(
                    f"   -> positions        : {max_pos} {'[STALE]' if pos_err else '[OK]'}"
                )
                error_msg.append(
                    f"   -> historical_data  : {max_hist} {'[STALE]' if hist_err else '[OK]'}"
                )
                error_msg.append(
                    f"   -> holding_returns  : {max_hold} {'[STALE]' if hold_err else '[OK]'}"
                )
                error_msg.append(
                    f"   -> fund_returns     : {max_fund} {'[STALE]' if fund_err else '[OK]'}"
                )
                error_msg.append(
                    f"   -> all_fund_returns : {max_all_fund} {'[STALE]' if all_fund_err else '[OK]'}"
                )
                error_msg.append(
                    f"   -> risk_free_rate   : {max_rfr} {'[STALE]' if rfr_err else '[OK (FRED 1-day lag)]'}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Data integrity failed with {len(future_failures)} future date tables and {len(stale_errors)} stale tables!"
            )

    @task
    def missing_calendar_dates():
        """
        Ensures all trading days in calendar since 2023-07-17 are covered in
        positions, holding_returns, fund_returns, and all_fund_returns.
        Flags missing trading dates in materializations and position data.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
            -- Checks for missing trading dates in positions, holding_returns, fund_returns, and all_fund_returns.
            WITH trading_days AS (
                SELECT date 
                FROM calendar 
                WHERE date >= '2023-07-17' AND date <= (SELECT MAX(date) FROM calendar)
            )
            SELECT 'all_fund_returns' AS table_name, c.date AS missing_date
            FROM trading_days c
            LEFT JOIN (SELECT DISTINCT date FROM all_fund_returns) a ON c.date = a.date
            WHERE a.date IS NULL

            UNION ALL

            SELECT 'fund_returns' AS table_name, c.date AS missing_date
            FROM trading_days c
            LEFT JOIN (SELECT DISTINCT date FROM fund_returns) f ON c.date = f.date
            WHERE f.date IS NULL

            UNION ALL

            SELECT 'holding_returns' AS table_name, c.date AS missing_date
            FROM trading_days c
            LEFT JOIN (SELECT DISTINCT date FROM holding_returns) h ON c.date = h.date
            WHERE h.date IS NULL

            UNION ALL

            SELECT 'positions' AS table_name, c.date AS missing_date
            FROM trading_days c
            LEFT JOIN (SELECT DISTINCT report_date AS date FROM positions) p ON c.date = p.date
            WHERE p.date IS NULL

            ORDER BY table_name, missing_date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      MISSING TRADING CALENDAR DATES DETECTED (SINCE 2023-07-17)",
                "=" * 90,
            ]
            for tbl, dt_val in failing_rows:
                error_msg.append(f"Table: {tbl:<20} | Missing Trading Date: {dt_val}")
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} missing trading calendar date entries across portfolio tables!"
            )

    # -------------------------------------------------------------------------
    # DAG FLOW DEPENDENCIES
    # -------------------------------------------------------------------------
    task_delta_nav_vs_all_fund = delta_nav_vs_all_fund_returns()
    task_all_fund_math = all_fund_returns_math()
    task_all_fund_divs = all_fund_returns_dividends_match()
    task_fund_returns_math = fund_returns_math()
    task_delta_nav_vs_fund = delta_nav_vs_fund_returns()
    task_benchmark_math = benchmark_math()
    task_positions_vs_hist_symbols = positions_vs_historical_symbols()
    task_trades_vs_positions = trades_vs_positions_qty()
    task_orphan_divs = orphan_dividends()
    task_zero_neg_nav = zero_negative_nav()
    task_future_date = date_sync_across_tables()
    task_missing_dates = missing_calendar_dates()

    table_structure_validation >> [
        task_delta_nav_vs_all_fund,
        task_all_fund_math,
        task_all_fund_divs,
        task_fund_returns_math,
        task_delta_nav_vs_fund,
        task_benchmark_math,
        task_positions_vs_hist_symbols,
        task_trades_vs_positions,
        task_orphan_divs,
        task_zero_neg_nav,
        task_future_date,
        task_missing_dates,
    ]


data_integrity_dag = data_integrity_pipeline()
