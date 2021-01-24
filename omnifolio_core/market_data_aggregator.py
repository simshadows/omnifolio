# -*- coding: ascii -*-

"""
Filename: market_data_aggregator.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Defines the MarketDataAggregator class, which aggregates and stores data provided by MarketDataProvider.
"""

from collections import namedtuple
from abc import ABC, abstractmethod

import pandas as pd

from .market_data_providers.rapidapi_apidojo_yahoo_finance import RAADYahooFinance

class MarketDataAggregator:

    def __init__(self, config):
        self._config = config

        self._providers = {
                "RAADYahooFinance": RAADYahooFinance(config),
            }
        return

    ######################################################################################

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
        return self._providers["RAADYahooFinance"].stock_timeseries_daily(symbols_list)

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

