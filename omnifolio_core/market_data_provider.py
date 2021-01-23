# -*- coding: ascii -*-

"""
Filename: market_data_provider.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Base class for MarketDataProvider.

MarketDataProvider encapsulates everything specific to a particular data provider (e.g. Morningstar,
or Yahoo Finance).
"""

from collections import namedtuple
from abc import ABC, abstractmethod

import pandas as pd

class MarketDataProvider(ABC):

    # Trust values guidelines for the get_trust_value() function.
    VERY_TRUSTED        = 300
    GENERALLY_TRUSTED   = 200
    SOMEWHAT_TRUSTED    = 100
    TRUST_LEVEL_UNSURE  = 0
    SOMEWHAT_UNTRUSTED  = -100
    GENERALLY_UNTRUSTED = -200
    VERY_UNTRUSTED      = -300

    @abstractmethod
    def get_trust_value(self):
        """
        Get an integer arbitrarily indicating how trustworthy a data source is.

        ---

        Positive integers are "trusted", while negative integers are "distrusted".

        Guidelines for values can be seen above (VERY_TRUSTED, GENERALLY_TRUSTED, etc.).

        Generally, scrapers tend to be untrusted (~-200), while APIs direct to a reputable
        provider such as Morningstar are trusted (~200). API calls directly to a stock exchange
        (if that's ever possible for a small projects such as this one) will be greatly trusted
        (~300).

        Values may deviate from the guideline as desired.
        """
        raise NotImplementedError

    ######################################################################################

    DayPrices = namedtuple("DayPrices", [
            "date",

            "open",           # int (Must be divided by price_denominator to get actual price.)
            "high",           # int (Must be divided by price_denominator to get actual price.)
            "low",            # int (Must be divided by price_denominator to get actual price.)
            #"close",         # We will assume we only care about adjusted close prices for now.
            "adjusted_close", # int (Must be divided by price_denominator to get actual price.)

            "volume", # int

            "unit",              # str (E.g. "USD")
            "price_denominator", # int (All prices must be divided by price_denominator to get the actual price.)
        ])

    DayEvents = namedtuple("DayEvents", [
            "date",

            "dividend",                 # int (Must be divided by price_denominator to get actual price.)
            "split_factor_numerator",   # int
            "split_factor_denominator", # int
        ])

    StockTimeSeriesDailyResult = namedtuple("StockTimeSeriesDailyResult", [
            "symbol",          # str
            "prices_list",     # list(DayPrices)
            "events_list",     # list(DayEvents)
            "extra_data_dict", # dict
        ])

    @abstractmethod
    def stock_timeseries_daily(self, symbols_list):
        """
        Get full daily history of a list of stocks and/or ETFs.

        Parameters:

            symbols_list:
                A list of strings, with each string representing a stock or ETF.
                Must have at least one item in the list.

        Returns:

            A dict of StockTimeSeriesDailyResult objects, where the key is the symbol (as written
            in symbols_list), and the value is a StockTimeSeriesDailyResult object that details
            the history of the symbol.
        """
        raise NotImplementedError

    def stock_timeseries_daily_pandas(self, symbols_list):
        data = self.stock_timeseries_daily(symbols_list)

        to_return = {}
        for (symbol, v) in data.items():
            to_return[symbol] = {
                    "symbol": v.symbol,
                    "prices": pd.DataFrame(v.prices_list),
                    "events": pd.DataFrame(v.events_list),
                    "extra_data_dict": v.extra_data_dict
                }
        return to_return

