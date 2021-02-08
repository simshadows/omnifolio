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

from .market_data_containers import (DayPrices,
                                     DayEvents,
                                     StockTimeSeriesDailyResult)
from .utils import fwrite_json, fread_json

logger = logging.getLogger(__name__)

class MarketDataStore:

    _STOCK_TIMESERIES_DAILY__FILEPATH = "stock_timeseries_daily.db"

    def __init__(self, config):
        self._market_data_store_path = config["market_data_store_path"]
        return

    @staticmethod
    def _check_if_db_is_set_up(conn):
        """
        Returns True if the database seems to have the expected schema.
        """
        query = """
                SELECT name
                FROM sqlite_master
                WHERE
                    type = 'table'
                    AND name = 'stock_timeseries_daily_prices';
            """
        cursor = conn.execute(query)
        num_rows = len(cursor.fetchall())
        return (num_rows != 0)

    @staticmethod
    def _set_up_db(conn):
        query = """
                CREATE TABLE stock_timeseries_daily_prices (
                    symbol               TEXT,
                    exchange             TEXT,
                    date                 DATE,

                    data_source          TEXT,
                    data_trust_value     INTEGER,
                    data_collection_time DATETIME,

                    open                 INTEGER,
                    high                 INTEGER,
                    low                  INTEGER,
                    close                INTEGER,
                    adjusted_close       INTEGER,

                    volume               INTEGER,

                    unit                 TEXT,
                    price_denominator    TEXT,

                    CONSTRAINT stock_timeseries_daily_prices_pk
                        PRIMARY KEY (symbol, exchange, date, data_source)
                );
            """
        conn.execute(query)

        query = """
                CREATE TABLE stock_timeseries_daily_exdividend_events (
                    symbol               TEXT,
                    exchange             TEXT,
                    date                 DATE,

                    data_source          TEXT,
                    data_trust_value     INTEGER,
                    data_collection_time DATETIME,

                    dividend_per_unit    INTEGER,

                    unit                 TEXT,
                    price_denominator    TEXT,

                    CONSTRAINT stock_timeseries_daily_prices_pk
                        PRIMARY KEY (symbol, exchange, date, data_source)
                );
            """
        conn.execute(query)

        query = """
                CREATE TABLE stock_timeseries_daily_split_events (
                    symbol               TEXT,
                    exchange             TEXT,
                    date                 DATE,

                    data_source          TEXT,
                    data_trust_value     INTEGER,
                    data_collection_time DATETIME,

                    -- split_numerator and split_denominator represent the split value as a fraction.
                    -- Pre-split units get multiplied by this split value to get post-split units.
                    -- Pre-split prices get divided by this split value to get post-split prices.
                    split_numerator      INTEGER,
                    split_denominator    INTEGER,

                    unit                 TEXT,

                    CONSTRAINT stock_timeseries_daily_prices_pk
                        PRIMARY KEY (symbol, exchange, date, data_source)
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
        with sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            if not self._check_if_db_is_set_up(conn):
                self._set_up_db(conn)
                if not self._check_if_db_is_set_up(conn):
                    raise RuntimError("Database schema check failed, even after setting it up.")
            yield conn

    @staticmethod
    def _get_price_data_dates_as_set(conn, *, symbol, exchange, provider):
        query = """
                SELECT date [date]
                FROM stock_timeseries_daily_prices
                WHERE
                    symbol = ?
                    AND exchange = ?
                    AND data_source = ?;
            """
        cursor = conn.execute(query, (symbol, exchange, provider))
        ret = {x[0] for x in cursor}
        assert all(isinstance(x, datetime.date) for x in ret)
        return ret

    @staticmethod
    def _add_price_data(conn, *, symbol, exchange, date, day_data):
        assert isinstance(symbol, str)
        assert isinstance(exchange, str)
        assert isinstance(date, datetime.date)
        assert isinstance(day_data, DayPrices)
        query = """
                INSERT INTO stock_timeseries_daily_prices VALUES (
                    ?, -- symbol
                    ?, -- exchange
                    ?, -- date

                    ?, -- data_source
                    ?, -- data_trust_value
                    ?, -- data_collection_time

                    ?, -- open
                    ?, -- high
                    ?, -- low
                    ?, -- close
                    ?, -- adjusted_close

                    ?, -- volume

                    ?, -- unit
                    ?  -- price_denominator
                );
            """
        params = (
                symbol,
                exchange,
                date,

                day_data.data_source,
                day_data.data_trust_value,
                day_data.data_collection_time,

                day_data.open,
                day_data.high,
                day_data.low,
                day_data.close,
                day_data.adjusted_close,

                day_data.volume,

                day_data.unit,
                day_data.price_denominator,
            )
        conn.execute(query, params)
        return

    ######################################################################################

    def stock_timeseries_daily__update_one_symbol(self, symbol, data):
        """
        Updates just one symbol with data from one or more providers.

        'symbol' is a string indicating Omnifolio's internally-used symbol for the stock/ETF.

        'data' is a dictionary where the key is Omnifolio's internally-used provider name as
        a string, and the value is a StockTimeSeriesDailyResult object.
        """
        assert isinstance(symbol, str) and (len(symbol.strip()) > 0)
        assert isinstance(data, dict)

        with self._get_db_connection() as conn:

            for (provider_name, provider_data) in data.items():

                assert isinstance(provider_name, str) and (len(provider_name.strip()) > 0)
                assert isinstance(provider_data, StockTimeSeriesDailyResult)

                prices = provider_data.prices

                pulled_dates = set(prices.keys())
                stored_dates = self._get_price_data_dates_as_set(
                        conn,
                        symbol=symbol,
                        exchange="PLACEHOLDER",
                        provider=provider_name,
                    )
                dates_missing_in_store = pulled_dates - stored_dates

                for date in dates_missing_in_store:
                    day_data = provider_data.prices[date]

                    self._add_price_data(
                            conn,
                            symbol=symbol,
                            exchange="PLACEHOLDER",
                            date=date,
                            day_data=day_data,
                        )

                # TODO: Events!

        return

