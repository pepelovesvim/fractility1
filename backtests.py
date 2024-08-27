import glob
from datetime import datetime

import altair as alt
import duckdb
import pandas as pd

alt.data_transformers.enable("vegafusion")

COLUMNS = "unix", "date", "symbol", "open", "filename"

_duck_rels = map(
    lambda x: duckdb.read_csv(x, filename=True).project(",".join(COLUMNS)),
    glob.glob("*.csv"),
)
duck_rels = list(_duck_rels)

duck_rels[0].to_table("__OHLC")
_ = list(map(lambda x: x.insert_into("__OHLC"), duck_rels[1:]))

duckdb.table("__OHLC").project(
    r"""* REPLACE(replace(symbol, 'USDT', 'USD') as symbol)
    , regexp_replace(filename, '_(?:BTC|ETH)USD[T]*_1h.csv', '') Exchange
    , log(Open/lag(Open) over(partition by symbol, Exchange order by date)) logReturn"""
).to_view("_OHLC")
# duckdb.view("OHLC").project("symbol").distinct()
# duckdb.view("_OHLC").project("Exchange").distinct()
_date_lowerb = (
    duckdb.view("_OHLC")
    .aggregate("Exchange, symbol, max(date), min(date)")
    .aggregate('max("min(date)")')
    .fetchone()
)
assert _date_lowerb is not None and isinstance(_date_lowerb[0], datetime.datetime)
date_lowerb = _date_lowerb[0]

duckdb.view("_OHLC").filter(f"date > '{date_lowerb}'").to_view("OHLC")

duckdb.table("OHLC").project("date, symbol").distinct().sort("date").to_table("history")

duckdb.sql(
    """
SELECT Exchange, symbol,
    time_bucket(INTERVAL '6h', date) AS "dt",
    sum(logReturn) "logReturn",
FROM OHLC
GROUP BY Exchange, symbol, "dt"
ORDER BY ALL
           """
).to_view("every 6 hours")


duckdb.sql(
    """
SELECT symbol,
    dt,
    list(Exchange order by logReturn) ExchangeList,
    list(Exchange order by logReturn)[:2] Top2, 
    list(Exchange order by logReturn)[-2:] Bot2
FROM "every 6 hours"
GROUP BY symbol, dt
ORDER BY dt, len(ExchangeList)
           """
).to_view("_6 hour signal")


def pnl(lag: int):
    duckdb.sql(
        """
    select date
        , history.symbol
        , last_value(Top2 IGNORE NULLS) over(partition by history.symbol order by history.date) _Top2
        , last_value(Bot2 IGNORE NULLS) over(partition by history.symbol order by history.date) _Bot2
        from history left join "_6 hour signal" on dt = date
        and "_6 hour signal".symbol = history.symbol
        order by history.date
            """
    ).project(
        f"* REPLACE(date_add(date, INTERVAL {lag} HOUR) as date), unnest(_Top2) Top2, unnest(_Bot2) Bot2"
    ).to_view(
        "6 hour signal"
    )

    # duckdb.sql("""select *, unnest(Top2) from "6 hour signal" """).count("*")
    # duckdb.sql("""select *, unnest(Top2), unnest(Bot2) from "6 hour signal" """).count("*")
    duckdb.sql("drop table if exists Long; drop table if exists Short")
    duckdb.sql("drop table if exists BackTest")

    duckdb.view("6 hour signal").set_alias("s").join(
        duckdb.view("OHLC").set_alias("o"),
        "s.symbol = o.symbol and s.date=o.date and Exchange=Bot2",
    ).aggregate("s.date, sum(logReturn) logReturn").to_table("Long")

    duckdb.view("6 hour signal").set_alias("s").join(
        duckdb.view("OHLC").set_alias("o"),
        "s.symbol = o.symbol and s.date=o.date and Exchange=Top2",
    ).aggregate("s.date, sum(logReturn) logReturn").to_table("Short")

    duckdb.sql(
        "select date, sum(Long.logReturn) - sum(Short.logReturn) stratReturn from Long join Short using (date) group by date"
    ).to_view("PnL")

    duckdb.sql(
        """
        select date
            , stratReturn
            from PnL
            """
    ).aggregate(
        "time_bucket(INTERVAL '1 day', date) date, sum(stratReturn) stratReturn"
    ).project(
        "*, sum(stratReturn) over (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) totalReturn"
    ).sort(
        "date"
    ).to_table(
        "BackTest"
    )

    back_test = duckdb.table("BackTest").project(f"*, '{lag}H' LagTime").df()
    return back_test


back_tests = pd.concat(pnl(x) for x in [1, 2, 3])


(
    alt.Chart(back_tests)
    .mark_line()
    .encode(x="date", y="totalReturn", color="LagTime:O")
    .properties(
        height=600,
        width=800,
        title="Total PnL via Exchange Reversion by LagTime, total log returns",
    )
)
