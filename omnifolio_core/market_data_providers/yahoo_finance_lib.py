# -*- coding: ascii -*-

"""
Filename: yahoo_finance_lib.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import datetime
import logging
from collections import OrderedDict

import numpy as np
import pandas as pd
import yfinance

from ..market_data_containers import (DayPrices,
                                      DayEvents,
                                      StockTimeSeriesDailyResult)
from ..market_data_provider import MarketDataProvider

logger = logging.getLogger(__name__)

class YahooFinanceLib(MarketDataProvider):

    _PRICE_DENOMINATOR = 10000

    def __init__(self):
        # No API key necessary!
        return

    def get_trust_value(self):
        return self.TRUST_LEVEL_UNSURE - self.PRECISION_GUESS_DEDUCTION

    def stock_timeseries_daily(self, symbols_list):
        if not isinstance(symbols_list, list):
            raise TypeError
        if len(symbols_list) == 0:
            raise ValueError("Expected non-empty list.")
        if not all(isinstance(x, str) and (len(x.strip()) > 0) for x in symbols_list):
            raise TypeError
        if len(symbols_list) != len(set(symbols_list)):
            raise ValueError("List must not have duplicates.")

        curr_time = datetime.datetime.utcnow()

        df = yfinance.download(
                tickers=symbols_list,
                period="max",
                interval="1d",
                actions=True,
                group_by="ticker",
                threads=True,
            )

        self._verify_raw_data_format(df, symbols_list)
        self._rename_column_labels(df)
        self._change_values_to_presplit_and_sort(df)

        ret = {}

        for symbol in symbols_list:
            sub_df = df[symbol].dropna(axis="index", how="all")

            prices_list = [] # list(datetime.date, DayPrices)

            for (index, values) in sub_df.iterrows():
                date = index.to_pydatetime().date() # Converts to native datetime.date object
                data_point = DayPrices(
                        data_source=self.get_provider_name(),
                        data_trust_value=self.get_trust_value(),
                        data_collection_time=curr_time,

                        open=int(round(values["open"] * self._PRICE_DENOMINATOR, 0)),
                        high=int(round(values["high"] * self._PRICE_DENOMINATOR, 0)),
                        low=int(round(values["low"] * self._PRICE_DENOMINATOR, 0)),
                        close=int(round(values["close"] * self._PRICE_DENOMINATOR, 0)),
                        adjusted_close=int(round(values["adjusted_close"] * self._PRICE_DENOMINATOR, 0)),

                        volume=int(values["volume"]),

                        unit="unknown",
                        price_denominator=self._PRICE_DENOMINATOR,
                    )
                #assert data_point.volume == values["Volume"]
                prices_list.append((date, data_point, ))

            # TODO: Events?

            prices_list.sort(key=lambda x : x[0])

            # TODO: Consider adding back in this data rejection logic
            #date_to_reject = prices_list[-1][0]
            ## TODO: What about events?
            #
            #prices=OrderedDict(prices_list)
            #events=OrderedDict(events_list)
            #if date_to_reject in prices:
            #    logger.debug(f"Rejecting date {date_to_reject} in the prices dict.")
            #    del prices[date_to_reject]
            #if date_to_reject in events:
            #    logger.debug(f"Rejecting date {date_to_reject} in the events dict.")
            #    del events[date_to_reject]

            ret[symbol] = StockTimeSeriesDailyResult(
                    symbol=symbol,
                    prices=OrderedDict(prices_list),
                    events=OrderedDict(), # Empty for now
                    extra_data={},
                )

        return ret

    @staticmethod
    def _verify_raw_data_format(df, symbols_list):
        if not isinstance(df, pd.DataFrame):
            raise TypeError
        if df.index.name != "Date":
            raise ValueError
        if (set(symbols_list) != set(df.columns.levels[0])) or (len(symbols_list) == 0):
            raise ValueError
        expected_column_labels = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends",
                                  "Stock Splits"]
        if list(df.columns.levels[1]) != expected_column_labels:
            raise ValueError
        if df.index.dtype.name != "datetime64[ns]":
            raise TypeError
        if not all((x == "float64") for x in df.dtypes):
            raise TypeError
        return

    @staticmethod
    def _rename_column_labels(df):
        new_column_names = {
                "Open":         "open",
                "High":         "high",
                "Low":          "low",
                "Close":        "close",
                "Adj Close":    "adjusted_close",
                "Volume":       "volume",
                "Dividends":    "exdividend",
                "Stock Splits": "split",
            }
        df.rename(columns=new_column_names, level=1, inplace=True)
        df.index.names = ["date"]
        return

    @staticmethod
    def _change_values_to_presplit_and_sort(df):
        """
        Transforms the Yahoo Finance library's download() function's split-adjusted values
        to pre-split values.

        E.g. TSLA on 2020-08-28 closed at 2213.40 pre-split.
        It then opened the following trading session at 444.61 because of the split.
        However, the library instead shows a 2020-08-28 closing price of 442.68.
        This function makes it so the dataframe will show 2020-08-28 with a closing price of 2213.40.

        Pre-split values are desirable for easier data consistency. TSLA's pre-split close on 2020-08-28
        will always be 2213.40, while its split-adjusted close depends on future splits. I will never
        need to go back and recalculate a stock's price history once it's in the database.
        """
        df.sort_index(ascending=True)

        symbols = list(df.columns.levels[0])

        for symbol in symbols:
            df.loc[:, (symbol, "split")] = np.where(df.loc[:, (symbol, "split")] == 0, 1, df.loc[:, (symbol, "split")])
            cum_split = df.loc[:, (symbol, "split")].cumprod()
            latest_cum_split = cum_split.dropna(axis="index", how="all")[-1]

            for k in ["open", "high", "low", "close", "adjusted_close", "exdividend"]:
                df.loc[:, (symbol, k)] *= (latest_cum_split / cum_split)
            df.loc[:, (symbol, "volume")] /= (latest_cum_split / cum_split)

        return

