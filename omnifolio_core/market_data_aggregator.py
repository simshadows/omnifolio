# -*- coding: ascii -*-

"""
Filename: market_data_aggregator.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Defines the MarketDataAggregator class, which aggregates and stores data provided by MarketDataProvider.
"""

import logging
from collections import namedtuple

import pandas as pd

from .market_data_store import MarketDataStore
from .market_data_providers.rapidapi_apidojo_yahoo_finance import RAADYahooFinance

logger = logging.getLogger(__name__)

class MarketDataAggregator:

    def __init__(self, config):
        self._config = config

        self._providers = [
                RAADYahooFinance(config),
            ]
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
        store = MarketDataStore(self._config)
        provider = self._providers[0]

        from_provider = provider.stock_timeseries_daily(symbols_list)
        
        for symbol in symbols_list:
            data = {provider.get_provider_name(): from_provider[symbol]}
            store.stock_timeseries_daily__update_one_symbol(symbol, data)

        return from_provider

    def stock_timeseries_daily_pandas(self, symbols_list):
        data = self.stock_timeseries_daily(symbols_list)

        to_return = {}
        for (symbol, v) in data.items():
            to_return[symbol] = {
                    "symbol": v.symbol,
                    "prices": pd.DataFrame(({"date": k} | v._asdict()) for (k, v) in v.prices.items()),
                    "events": pd.DataFrame(({"date": k} | v._asdict()) for (k, v) in v.events.items()),
                    "extra_data": v.extra_data,
                }
            if "date" in to_return[symbol]["prices"].columns:
                to_return[symbol]["prices"].sort_values("date")
            if "date" in to_return[symbol]["events"].columns:
                to_return[symbol]["events"].sort_values("date")

        return to_return

