# -*- coding: ascii -*-

"""
Filename: market_data_aggregator.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Defines the MarketDataAggregator class, which aggregates and stores data provided by MarketDataProvider.
"""

import copy
import logging
from collections import namedtuple

import pandas as pd

from .market_data_store import MarketDataStore
from .market_data_providers.yahoo_finance_lib import YahooFinanceLib

from .exceptions import MissingData
from .utils import str_is_nonempty_and_compact

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

        if (set(from_store.keys()) != set(symbols_list)):
            raise RuntimeError
        if any(len(v) == 0 for (k, v) in from_store.items()):
            raise MissingData("Cannot find data for one or more symbols.")

        return from_store

    ######################################################################################

    @staticmethod
    def stock_timeseries_daily__convert_to_splitadjusted(dfs):
        """
        Takes in whatever stock_timeseries_daily() returns.

        Returns a new set of dataframes with all relevant values split-adjusted, with integer
        types converted to floating point.
        """
        assert isinstance(dfs, dict)
        ret = {}
        for (symbol, df) in dfs.items():
            assert str_is_nonempty_and_compact(symbol)
            assert isinstance(df, pd.DataFrame)

            df = copy.deepcopy(df)

            cum_split = df.loc[:, "split"].cumprod()
            latest_cum_split = cum_split.dropna(axis="index", how="all")[-1]

            # Multiply a bunch of columns by (cum_split / latest_cum_split)
            columns_to_multiply = ["open", "high", "low", "close", "adjusted_close", "exdividend"]
            df.loc[:, columns_to_multiply] = df.loc[:, columns_to_multiply].multiply(cum_split / latest_cum_split, axis="index")

            # Multiply only the volume column by (latest_cum_split / cum_split)
            df.loc[:, "volume"] *= (latest_cum_split / cum_split)

            ret[symbol] = df
        return ret

