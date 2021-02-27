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
from .market_data_providers.yahoo_finance_lib import YahooFinanceLib

logger = logging.getLogger(__name__)

class MarketDataAggregator:

    def __init__(self, config):
        self._config = config

        self._providers = [
                YahooFinanceLib(),
            ]
        return

    ######################################################################################

    def stock_timeseries_daily(self, symbols_list, update_store=True):
        """
        Get full daily history of a list of stocks and/or ETFs.

        Parameters:

            symbols_list:
                A list of strings, with each string representing a stock or ETF.
                Must have at least one item in the list.

        Returns:

            (TODO: Document this later.)
        """
        assert isinstance(update_store, bool)

        store = MarketDataStore(self._config)
        provider = self._providers[0] # TODO: Use multiple providers later?

        if update_store:
            from_provider = provider.stock_timeseries_daily(symbols_list)

            assert set(from_provider.keys()) == set(symbols_list)
            
            for symbol in symbols_list:
                store.update_stock_timeseries_daily(symbol, provider.get_provider_name(), from_provider[symbol])

        from_store = store.get_stock_timeseries_daily(symbols_list)

        assert set(from_store.keys()) == set(symbols_list)

        return from_store

