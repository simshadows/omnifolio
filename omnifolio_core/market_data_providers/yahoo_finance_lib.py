# -*- coding: ascii -*-

"""
Filename: yahoo_finance_lib.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import datetime
import logging
from collections import OrderedDict

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

        prices = yfinance.download(
                tickers=symbols_list,
                period="max",
                interval="1d",
                actions=True,
                group_by="ticker",
                threads=True,
            )

        if set(prices.columns.levels[0]) != set(symbols_list):
            raise ValueError("Expected all price data to be pulled.")

        # If any of these assumptions of the data format changes, I'll need to look into it.
        # I might need to make some changes towards architecture-agnostic assertions.
        assert isinstance(prices, pd.DataFrame)
        assert prices.index.name == "Date"
        # prices.columns.levels[0] is a list of symbols
        assert list(prices.columns.levels[1]) == ["Open", "High", "Low", "Close", "Adj Close",
                                                  "Volume", "Dividends", "Stock Splits"]
        assert prices.index.dtype.name == "datetime64[ns]"
        assert list(prices.dtypes) == (["float64"] * len(prices.dtypes)) # Checking if they're all float64's

        ret = {}

        for symbol in symbols_list:
            df = prices[symbol].dropna(axis="index", how="all")

            prices_list = [] # list(datetime.date, DayPrices)

            for (index, values) in df.iterrows():
                date = index.to_pydatetime().date() # Converts to native datetime.date object
                data_point = DayPrices(
                        data_source=self.get_provider_name(),
                        data_trust_value=self.get_trust_value(),
                        data_collection_time=curr_time,

                        open=int(round(values["Open"] * self._PRICE_DENOMINATOR, 0)),
                        high=int(round(values["High"] * self._PRICE_DENOMINATOR, 0)),
                        low=int(round(values["Low"] * self._PRICE_DENOMINATOR, 0)),
                        close=int(round(values["Close"] * self._PRICE_DENOMINATOR, 0)),
                        adjusted_close=int(round(values["Adj Close"] * self._PRICE_DENOMINATOR, 0)),

                        volume=int(values["Volume"]),

                        unit="unknown",
                        price_denominator=self._PRICE_DENOMINATOR,
                    )
                assert data_point.volume == values["Volume"]
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

