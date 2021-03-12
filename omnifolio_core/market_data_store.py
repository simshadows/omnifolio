# -*- coding: ascii -*-

"""
Filename: market_data_store.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

MarketDataStore encapsulates the actual storage mechanism behind MarketDataAggregator.
"""

import os
import logging
import datetime
import sqlite3
from collections import namedtuple
from contextlib import contextmanager, closing

import numpy as np
import pandas as pd

from .utils import fwrite_json, fread_json

logger = logging.getLogger(__name__)



_NUMPY_INT = np.longlong
_NUMPY_FLOAT = np.double
assert np.iinfo(_NUMPY_INT).bits >= 64



class MarketDataStore:

    _STOCK_TIMESERIES_DAILY__FILEPATH = "stock_timeseries_daily.db"

    def __init__(self, config):
        # TODO: Set the directory name somewhere else.
        self._market_data_store_path = os.path.join(config["generated_data_path"], "market_data_store")
        return

    @staticmethod
    def _check_if_db_is_set_up(conn):
        """
        Returns True if the database seems to have the expected schema.

        (May not check everything.)
        (TODO: Implement more thorough checking.)
        """
        query = """
                SELECT name
                FROM sqlite_master
                WHERE
                    type = 'table'
                    AND name = 'stock_timeseries_daily';
            """
        cursor = conn.execute(query)
        num_rows = len(cursor.fetchall())
        return (num_rows != 0)

    @staticmethod
    def _set_up_db(conn):
        query = """
                CREATE TABLE stock_timeseries_daily (
                    symbol                   TEXT, -- RIC code
                    date                     DATE,

                    data_source              TEXT,
                    data_trust_value         INTEGER,
                    data_collection_time_utc TIMESTAMP,

                    unit                     TEXT, -- Currency Unit (e.g. "USD" or "AUD")

                    ----------------
                    -- PRICE DATA --
                    ----------------
                    open                     INTEGER, -- Divide open/high/low/close/adjusted_close
                    high                     INTEGER, -- values by price_denominator to get
                    low                      INTEGER, -- the actual price value in the unit
                    close                    INTEGER, -- currency.
                    adjusted_close           INTEGER, -- E.g. if unit="USD", open=92350, and price_denominator=10000,
                    price_denominator        INTEGER, -- then the opening price is 9.2350 USD.

                    --------------------
                    -- TRADING VOLUME --
                    --------------------
                    volume                   INTEGER,

                    -----------------
                    -- EX-DIVIDEND -- 
                    -----------------
                    -- Numerator and denominator represents the allocated dividend per unit
                    -- on an ex-dividend day.
                    --
                    -- Example 1: if unit=USD, num=12, and denom=1000 represents an 0.012 USD dividend.
                    -- Example 2: If a day is not ex-dividend, numerator is zero.
                    --
                    dividend_numerator       INTEGER,
                    dividend_denominator     INTEGER,

                    ----------------
                    -- UNIT SPLIT --
                    ----------------
                    -- Pre-split units get multiplied by this split value to get post-split units.
                    -- Pre-split prices get divided by this split value to get post-split prices.
                    -- (TODO: Consider changing the data type to a pair of integers. Although
                    -- reverse splits and other splits where this value has a fractional component
                    -- are quite rare, they still happen. The chances of this causing precision
                    -- errors are rarer still, but I'd much rather we have 100% precision to
                    -- begin with. The only thing keeping me from implementing 100% precision
                    -- is the high added complexity versus the small benefit.)
                    split                    REAL,

                    CONSTRAINT stock_timeseries_daily_pk
                        PRIMARY KEY (symbol, date, data_source)
                );
            """
        conn.execute(query)
        return

    @contextmanager
    def _get_db_connection(self):
        """
        Returns a database connection object, wrapped in a context manager.
        """
        path = os.path.join(
                self._market_data_store_path,
                self._STOCK_TIMESERIES_DAILY__FILEPATH,
            )
        detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        with sqlite3.connect(path, detect_types=detect_types) as conn:
            if not self._check_if_db_is_set_up(conn):
                self._set_up_db(conn)
                if not self._check_if_db_is_set_up(conn):
                    raise RuntimeError("Database schema check failed, even after setting it up.")
            yield conn

    @staticmethod
    def _get_price_data_dates_as_set(conn, *, symbol, provider):
        query = """
                SELECT date [date]
                FROM stock_timeseries_daily
                WHERE
                    symbol = ?
                    AND data_source = ?;
            """
        cursor = conn.execute(query, (symbol, provider))
        ret = {x[0] for x in cursor}
        assert all(isinstance(x, datetime.date) for x in ret)
        return ret

    @staticmethod
    def _add_price_data(conn, *, symbol, date, df_row):
        assert isinstance(symbol, str)
        assert isinstance(date, datetime.date)
        assert isinstance(df_row, pd.Series)

        assert isinstance(df_row["data_source"], str)
        assert isinstance(df_row["data_trust_value"].item(), int)
        assert isinstance(df_row["data_collection_time"].to_pydatetime(), datetime.datetime)

        assert isinstance(df_row["unit"], str)

        assert isinstance(df_row["open"].item(), int)
        assert isinstance(df_row["high"].item(), int)
        assert isinstance(df_row["low"].item(), int)
        assert isinstance(df_row["close"].item(), int)
        assert isinstance(df_row["adjusted_close"].item(), int)
        assert isinstance(df_row["price_denominator"].item(), int)

        assert isinstance(df_row["volume"].item(), int)

        assert isinstance(df_row["exdividend"].item(), int)
        assert isinstance(df_row["dividend_denominator"].item(), int)

        assert isinstance(df_row["split"].item(), float)

        query = """
                INSERT INTO stock_timeseries_daily VALUES (
                    ?, -- symbol
                    ?, -- date

                    ?, -- data_source
                    ?, -- data_trust_value
                    ?, -- data_collection_time

                    ?, -- unit

                    ?, -- open
                    ?, -- high
                    ?, -- low
                    ?, -- close
                    ?, -- adjusted_close
                    ?, -- price_denominator

                    ?, -- volume

                    ?, -- dividend_numerator
                    ?, -- dividend_denominator

                    ? -- split
                );
            """
        params = (
                symbol,
                date,

                df_row["data_source"],
                df_row["data_trust_value"].item(),
                df_row["data_collection_time"].to_pydatetime(),

                df_row["unit"],

                df_row["open"].item(),
                df_row["high"].item(),
                df_row["low"].item(),
                df_row["close"].item(),
                df_row["adjusted_close"].item(),
                df_row["price_denominator"].item(),

                df_row["volume"].item(),

                df_row["exdividend"].item(),
                df_row["dividend_denominator"].item(),

                df_row["split"].item(),
            )
        conn.execute(query, params)
        return

    @staticmethod
    def _update_price_data(conn, *, symbol, date, df_row):
        assert isinstance(symbol, str)
        assert isinstance(date, datetime.date)
        assert isinstance(df_row, pd.Series)

        assert isinstance(df_row["data_source"], str)
        assert isinstance(df_row["data_trust_value"].item(), int)
        assert isinstance(df_row["data_collection_time"].to_pydatetime(), datetime.datetime)

        assert isinstance(df_row["unit"], str)

        assert isinstance(df_row["open"].item(), int)
        assert isinstance(df_row["high"].item(), int)
        assert isinstance(df_row["low"].item(), int)
        assert isinstance(df_row["close"].item(), int)
        assert isinstance(df_row["adjusted_close"].item(), int)
        assert isinstance(df_row["price_denominator"].item(), int)

        assert isinstance(df_row["volume"].item(), int)

        assert isinstance(df_row["exdividend"].item(), int)
        assert isinstance(df_row["dividend_denominator"].item(), int)

        assert isinstance(df_row["split"].item(), float)

        query = """
                UPDATE stock_timeseries_daily SET
                    data_trust_value         = ?,
                    data_collection_time_utc = ?,

                    unit = ?,

                    open              = ?,
                    high              = ?,
                    low               = ?,
                    close             = ?,
                    adjusted_close    = ?,
                    price_denominator = ?,

                    volume = ?,

                    dividend_numerator   = ?,
                    dividend_denominator = ?,

                    split = ?
                WHERE
                    symbol = ?
                    AND date = ?
                    AND data_source = ?
                ;
            """
        params = (
                df_row["data_trust_value"].item(),
                df_row["data_collection_time"].to_pydatetime(),

                df_row["unit"],

                df_row["open"].item(),
                df_row["high"].item(),
                df_row["low"].item(),
                df_row["close"].item(),
                df_row["adjusted_close"].item(),
                df_row["price_denominator"].item(),

                df_row["volume"].item(),

                df_row["exdividend"].item(),
                df_row["dividend_denominator"].item(),

                df_row["split"].item(),

                symbol,
                date,
                df_row["data_source"],
            )
        conn.execute(query, params)
        return

    @staticmethod
    def _get_price_data(conn, *, symbols_list):
        # TODO: Make this pull only one row for every date. For now, it just pulls everything.
        markers = ",".join(["?"] * len(symbols_list))
        query = f"""
                SELECT
                    symbol,
                    date,

                    data_source,
                    data_trust_value,
                    data_collection_time_utc,

                    unit,

                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    price_denominator,

                    volume,

                    dividend_numerator,
                    dividend_denominator,

                    split
                    
                FROM stock_timeseries_daily
                WHERE
                    symbol IN ({markers})
                ;
            """
        cursor = conn.execute(query, symbols_list)
        ret = [x for x in cursor]
        return ret

    ######################################################################################

    def get_stock_timeseries_daily(self, symbols_list):
        with self._get_db_connection() as conn:
            data = self._get_price_data(conn, symbols_list=symbols_list)

            column_names = [
                    "symbol",
                    "date",
                    "data_source",
                    "data_trust_value",
                    "data_collection_time",
                    "unit",
                    "open",
                    "high",
                    "low",
                    "close",
                    "adjusted_close",
                    "price_denominator",
                    "volume",
                    "exdividend",
                    "dividend_denominator",
                    "split",
                ]
            df = pd.DataFrame(data, columns=column_names)

            new_column_types = {
                    "date": np.datetime64,
                }
            df = df.astype(new_column_types)

            # Check types

            expected_type_names = [
                    "object",
                    "datetime64[ns]",
                    "object",
                    "int64",
                    "datetime64[ns]",
                    "object",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "float64",
                ]
            if list(str(x) for x in df.dtypes) != expected_type_names:
                raise TypeError("Unexpected types: " + str(df.dtypes))

            # Separate symbols into different dataframes, and make final adjustments to dataframe formatting

            dfs = {}

            for symbol in symbols_list:
                new_df = df.loc[df["symbol"] == symbol].drop(labels=["symbol"], axis="columns")
                new_df = new_df.set_index(keys=["date"], verify_integrity=True)
                if not new_df.index.is_monotonic:
                    raise RuntimeError
                dfs[symbol] = new_df

            return dfs

    def update_stock_timeseries_daily(self, symbol, provider_name, df):
        """
        Updates just one symbol with data from one or more providers.

        'symbol' is a string indicating Omnifolio's internally-used symbol for the stock/ETF.

        'provider_name" is self-explanatory.

        'df' is a pandas.DataFrame object containing all price data.

        (TODO: Document the DataFrame object.)
        """
        assert isinstance(symbol, str) and (len(symbol) > 0) and (symbol == symbol.strip())
        assert isinstance(provider_name, str) and (len(provider_name) > 0) and (provider_name == provider_name.strip())
        assert isinstance(df, pd.DataFrame)

        with self._get_db_connection() as conn:

            stored_dates = self._get_price_data_dates_as_set(
                    conn,
                    symbol=symbol,
                    provider=provider_name,
                )
            pulled_dates = set(x.date() for x in df.index.to_pydatetime())

            assert all(isinstance(x, datetime.date) for x in stored_dates)
            assert all(isinstance(x, datetime.date) for x in pulled_dates)

            # First, we update the three latest entries.
            # This is because data sources often report the price of a day before the day is
            # actually complete. Additionally, I added an allowance of additional days that
            # can be updated, to give a small chance to fix erroneous data.
            # (Technically, I should allow the entire pulled data to update the stored data,
            # but I generally want to make data immutable once it's entered into my system,
            # to prevent bugs from wiping valuable data.)
            if len(stored_dates) > 0:
                dates_to_update = set(sorted(stored_dates)[-3:])
                provided_dates_in_pulled_data = pulled_dates & dates_to_update
                for date in provided_dates_in_pulled_data:
                    row = df.loc[np.datetime64(date)]

                    self._update_price_data(
                            conn,
                            symbol=symbol,
                            date=date,
                            df_row=row,
                        )

            # Now, we add missing data.

            dates_missing_in_store = pulled_dates - stored_dates
            for date in dates_missing_in_store:
                row = df.loc[np.datetime64(date)]

                self._add_price_data(
                        conn,
                        symbol=symbol,
                        date=date,
                        df_row=row,
                    )

        return

