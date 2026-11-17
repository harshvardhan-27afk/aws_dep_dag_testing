"""
combined_sf_dags.py : one DAG that exercises 7 Snowflake operators in
Maestro-PI, one task per operator. Written for a free trial account, so the two
operators that need paid features (SnowparkContainerJobOperator and
SnowflakeCortexAgentOperator) are left out.

Run snowflake/sf.sql in Snowflake first (see its header). It creates every
table and the stage this file uses. Then push this file to your DAGs git repo
and trigger the DAG manually (schedule=None).

Task graph (operators in brackets):

    t01_sql_execute_query [SQLExecuteQueryOperator] --+--> t02_check            [SnowflakeCheckOperator]
                                                      +--> t03_value_check      [SnowflakeValueCheckOperator]
                                                      +--> t04_interval_check   [SnowflakeIntervalCheckOperator]

    t05_copy_into_from_stage [CopyFromExternalStageToSnowflakeOperator]
        --> t06_sql_api [SnowflakeSqlApiOperator]
        --> t07_snowpark [SnowparkOperator]

t01 and t05 have no upstream, so they start together.

Expected end state (every task SUCCESS):
  t01  one new row in PIFLOW_SF_RUN_LOG; return value is the statement result
  t02  passes: PIFLOW_SF_ORDERS has rows and a non-zero total
  t03  passes: PIFLOW_SF_CUSTOMERS has exactly 100 rows
  t04  passes: today vs 7 days ago ratio is 1.08, below the 1.5 threshold
  t05  PIFLOW_SF_COPY_TARGET = 208 rows on the first run (re-runs load 0 new
       files and still succeed; see sf.sql section 10 to reload)
  t06  PIFLOW_SF_API_STAGE_A = 208 rows, PIFLOW_SF_API_STAGE_B = 4 rows; XCom
       "query_ids" holds the two statement handles
  t07  PIFLOW_SF_SNOWPARK_SUMMARY = 4 rows (52 orders, 26000.00 each); return value 4

Edit the connection constant below if yours differs.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    SQLExecuteQueryOperator,
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
    SnowflakeIntervalCheckOperator,
    CopyFromExternalStageToSnowflakeOperator,
    SnowflakeSqlApiOperator,
    SnowparkOperator,
)

# --- Edit to match your Maestro-PI connection ---------------------------------
SF_CONN_ID = "harsh_conn"          # Maestro-PI Snowflake connection (Connections page)
# ------------------------------------------------------------------------------


# t07 callable. SnowparkOperator injects a live snowflake.snowpark.Session into
# any callable that declares a parameter literally named `session`. It must be
# a module-level function; imports of snowflake.* stay inside it so DAG parsing
# does not need the Snowpark package.
def summarize_orders(session):
    from snowflake.snowpark.functions import col, count, sum as sum_

    orders = session.table("PIFLOW_SF_API_STAGE_A")
    summary = orders.group_by("REGION").agg(
        count("*").alias("ORDER_COUNT"),
        sum_(col("AMOUNT")).alias("TOTAL_AMOUNT"),
    )
    # mode="truncate" keeps the table and empties it first, so re-runs do not
    # accumulate rows. column_order="name" is required here: the target table
    # has a RUN_TS column with a DEFAULT that this DataFrame does not supply,
    # and the default (positional) matching fails on the column-count mismatch.
    summary.write.save_as_table(
        "PIFLOW_SF_SNOWPARK_SUMMARY", mode="truncate", column_order="name"
    )
    rows = summary.count()
    print(f"[snowpark] wrote {rows} summary rows to PIFLOW_SF_SNOWPARK_SUMMARY")
    return rows


with DAG(
    dag_id="combined_sf_dags",
    description="One task per Snowflake operator (7 operators)",
    tags=["snowflake", "operator-test"],
    schedule=None,                 # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        # Every Snowflake operator below resolves its connection from here, so
        # no task repeats snowflake_conn_id.
        "snowflake_conn_id": SF_CONN_ID,
    },
) as dag:

    # 1) SQLExecuteQueryOperator: runs SQL on the Snowflake connection.
    #    `parameters` are real bind parameters: named binds use :name, and the
    #    value never gets spliced into the SQL text. autocommit=True so the
    #    INSERT is committed (the operator's default is False).
    t01_sql_execute_query = SQLExecuteQueryOperator(
        task_id="t01_sql_execute_query",
        sql=(
            "INSERT INTO PIFLOW_SF_RUN_LOG (OPERATOR, NOTE) "
            "VALUES ('SQLExecuteQueryOperator', :note)"
        ),
        parameters={"note": "bind parameter test"},
        autocommit=True,
    )

    # 2) SnowflakeCheckOperator: fails unless EVERY value in the query's FIRST
    #    row is truthy (not NULL, 0, false or ''). No rows returned also fails.
    #    retry_on_failure=False: the data will not change between attempts, so a
    #    failed check should fail outright instead of burning retries.
    t02_check = SnowflakeCheckOperator(
        task_id="t02_check",
        sql="SELECT COUNT(*), SUM(AMOUNT) FROM PIFLOW_SF_ORDERS",
        retry_on_failure=False,
    )

    # 3) SnowflakeValueCheckOperator: fails unless the first row equals
    #    pass_value. With a numeric pass_value, tolerance=0.05 accepts +/-5%
    #    (95 to 105 here). The actual count is exactly 100.
    t03_value_check = SnowflakeValueCheckOperator(
        task_id="t03_value_check",
        sql="SELECT COUNT(*) FROM PIFLOW_SF_CUSTOMERS",
        pass_value=100,
        tolerance=0.05,
        retry_on_failure=False,
    )

    # 4) SnowflakeIntervalCheckOperator: compares each metric for the run's date
    #    ({{ .DS }}) against `days_back` days earlier and fails if the ratio is
    #    not below the threshold. Seeded data: 100 rows 7 days ago, 108 today,
    #    ratio 1.08, so 1.5 passes. The dates are relative to the run date, so
    #    re-run sf.sql section 3 if you trigger on a different day than you
    #    seeded.
    t04_interval_check = SnowflakeIntervalCheckOperator(
        task_id="t04_interval_check",
        table="PIFLOW_SF_ORDERS",
        date_filter_column="ORDER_DATE",
        metrics_thresholds={"COUNT(*)": 1.5, "SUM(AMOUNT)": 1.5},
        days_back=7,
        ratio_formula="max_over_min",
        retry_on_failure=False,
    )

    # 5) CopyFromExternalStageToSnowflakeOperator: COPY INTO <table> from a
    #    stage. `stage` is the stage NAME WITHOUT the leading @ (the operator
    #    adds it). `prefix` limits the search to the orders/ folder and
    #    `pattern` is a regex over file names. sf.sql section 4 put two CSVs
    #    there; this is an internal stage, which COPY INTO reads the same way as
    #    an external one. file_format is SQL, written inline with spaces as
    #    Snowflake expects.
    t05_copy_into_from_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id="t05_copy_into_from_stage",
        table="PIFLOW_SF_COPY_TARGET",
        stage="PIFLOW_SF_STAGE",
        prefix="orders/",
        pattern=".*orders_batch_.*[.]csv",
        file_format="(TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0)",
        copy_options="ON_ERROR = 'ABORT_STATEMENT'",
    )

    # 6) SnowflakeSqlApiOperator: submits the SQL through Snowflake's REST SQL
    #    API and polls, instead of holding a database session open. Two
    #    statements go in one request, so statement_count=2. The statement
    #    handles are pushed to XCom under "query_ids". Reads what t05 loaded.
    t06_sql_api = SnowflakeSqlApiOperator(
        task_id="t06_sql_api",
        sql=(
            "INSERT OVERWRITE INTO PIFLOW_SF_API_STAGE_A "
            "SELECT ORDER_ID, REGION, AMOUNT FROM PIFLOW_SF_COPY_TARGET; "
            "INSERT OVERWRITE INTO PIFLOW_SF_API_STAGE_B "
            "SELECT REGION, COUNT(*), SUM(AMOUNT) FROM PIFLOW_SF_API_STAGE_A GROUP BY REGION;"
        ),
        statement_count=2,
        poll_interval=2,
    )

    # 7) SnowparkOperator: runs the Python callable above with a live Snowpark
    #    session injected as `session`. It executes in the Python task
    #    subprocess (not in Go like the other eight), and the callable's return
    #    value becomes the task's XCom, same as PythonOperator.
    t07_snowpark = SnowparkOperator(
        task_id="t07_snowpark",
        python_callable=summarize_orders,
    )

    # Dependencies: validate after the write; load -> transform -> Snowpark.
    t01_sql_execute_query >> [t02_check, t03_value_check, t04_interval_check]
    t05_copy_into_from_stage >> t06_sql_api >> t07_snowpark
