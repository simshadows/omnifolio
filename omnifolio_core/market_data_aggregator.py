# -*- coding: ascii -*-

"""
Filename: market_data_aggregator.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Defines the MarketDataAggregator class, which aggregates and stores data provided by MarketDataProvider.
"""

import os
import copy
import logging
from fractions import Fraction
from decimal import Decimal
from collections import namedtuple

import pandas as pd

from .config import get_config

from .market_data_store import MarketDataStore
from .market_data_providers.yahoo_finance_lib import YahooFinanceLib

from .exceptions import MissingData
from .utils import (
        str_is_nonempty_and_compact,
        pandas_index_union,
        dump_df_to_csv_debugging_file,
    )

logger = logging.getLogger(__name__)



class MarketDataAggregator:

    def __init__(self, config=get_config()):
        self._config = config

        # TODO: Set the directory name somewhere else.
        self._debugging_path = os.path.join(config["generated_data_path"], "debugging")

        self._providers = [
                YahooFinanceLib(),
            ]
        return

    ######################################################################################

    def stock_timeseries_daily__update_store(self, symbols):
        """
        Updates (usually by downloading data off the internet) all relevant data for stock_timeseries_daily(),
        specifically only updating the symbols listed.

        Parameters:
            symbols:
                A collection of strings, with each string representing a stock or ETF.
                Collection must contain at least one item.
        Returns:
            None
        """
        assert all(isinstance(x, str) for x in symbols)

        provider = self._providers[0] # TODO: Use multiple providers later?

        data = provider.stock_timeseries_daily(list(symbols)) # TODO: Remove specific type restriction of symbols?

        assert set(data.keys()) == set(symbols)
        
        store = MarketDataStore(self._config)
        for symbol in symbols:
            store.update_stock_timeseries_daily(symbol, provider.get_provider_name(), data[symbol])
        return

    def stock_timeseries_daily(self, symbols, update_store=True):
        """
        Get full daily history of a list of stocks and/or ETFs.

        Parameters:
            symbols:
                A collection of strings, with each string representing a stock or ETF.
                Collection must contain at least one item.
            update_store:
                Automatically calls stock_timeseries_daily__update_store() before compiling
                the data.
        Returns:
            (TODO: Document this later.)
        """
        assert isinstance(update_store, bool)
        if update_store:
            self.stock_timeseries_daily__update_store(symbols)

        data = MarketDataStore(self._config).get_stock_timeseries_daily(symbols)

        if (set(data.keys()) != set(symbols)):
            raise RuntimeError
        if any(len(v) == 0 for (k, v) in data.items()):
            raise MissingData("Cannot find data for one or more symbols.")

        return data

    ######################################################################################

    @staticmethod
    def stock_timeseries_daily__to_splitadjusted(dfs):
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

    def stock_timeseries_daily__to_adjclose_summary(self, dfs, split_adjusted=False):
        """
        Summarizes important closing values from the data in dfs.

        ---

        Takes in whatever stock_timeseries_daily() returns.

        Returns a single dataframe with two levels of column labels.

        First level is the symbol itself.

        Second level summarizes important columns at close:
            - 'unit' as a string
            - 'adjusted_close' as a Fraction
            - 'exdividend' as a Fraction
            - 'split' as a numpy double
        Note that here, we actually express values as a single Fraction object instead of two
        integer columns that represent a fraction.

        (I'll be reconsidering whether or not Fraction objects are suitable versus pandas native
        integers with explicit numerator/denominator arithmetic.)
        """
        assert isinstance(dfs, dict)
        assert all(isinstance(k, str) and isinstance(v, pd.DataFrame) for (k, v) in dfs.items())
        new_index = pandas_index_union(x.index for x in dfs.values())
        new_cols = []
        for (k, v) in dfs.items():
            new_cols.append(v["unit"].rename((k, "unit")).reindex(index=new_index))

            op1 = lambda x : Fraction(numerator=x["adjusted_close"], denominator=x["price_denominator"])
            new_cols.append(v.apply(op1, axis="columns").reindex(index=new_index).rename((k, "adjusted_close")))

            op2 = lambda x : Fraction(numerator=x["exdividend"], denominator=x["dividend_denominator"])
            new_cols.append(v.apply(op2, axis="columns").reindex(index=new_index).rename((k, "exdividend")))

            new_cols.append(v["split"].reindex(index=new_index).rename((k, "split")))

        df = pd.concat(new_cols, axis="columns")
        dump_df_to_csv_debugging_file(df, self._debugging_path, "stock_timeseries_daily__to_adjclose_summary_dataframe.csv")

        if split_adjusted:
            for symbol in dfs.keys():
                cum_split = df.loc[:, (symbol, "split")].cumprod()
                latest_cum_split = cum_split.dropna(axis="index", how="all")[-1]
                multiply_by = cum_split / latest_cum_split
                for c in ["adjusted_close", "exdividend"]:
                    df.loc[:, (symbol, c)] = df.loc[:, (symbol, c)] * multiply_by
            dump_df_to_csv_debugging_file(df, self._debugging_path, "stock_timeseries_daily__to_adjclose_summary_dataframe_splitadjusted.csv")
        return df

    def stock_timeseries_daily__to_dividend_reinvested_scaled(self, dfs):
        """
        Calculates stock prices scaled by dividend-reinvestment.

        ---

        Takes in whatever stock_timeseries_daily() returns.

        Returns a dataframe.
        TODO: Document the dataframe's format?
        """
        symbols = set(dfs.keys())
        df = self.stock_timeseries_daily__to_adjclose_summary(dfs, split_adjusted=True)
        df.fillna(method="ffill", inplace=True)

        new_sers = {}
        for symbol in symbols:
            dividend_ratio = (df[symbol]["exdividend"] / df[symbol]["adjusted_close"]) + 1
            scaling_coef = dividend_ratio.cumprod()
            scaled_adjusted_close = df[symbol]["adjusted_close"] * scaling_coef

            df.loc[:,(symbol, "adjusted_close")] = scaled_adjusted_close
            df.drop([(symbol, "exdividend"), (symbol, "split")], axis="columns", inplace=True)

        df.rename(columns={"adjusted_close": "drscaled_adjusted_close"}, level=1, inplace=True)
        return df

    @staticmethod
    def stock_timeseries_daily__to_debugging_adjclose(dfs):
        """
        Takes in whatever stock_timeseries_daily() returns.

        Returns a single dataframe, merging adjusted close prices for all symbols from dfs.

        The output is not intended for machine use.
        It's used for debugging, to quickly glance through whether or not the values are as expected.
        """
        assert isinstance(dfs, dict)
        assert all(isinstance(k, str) and isinstance(v, pd.DataFrame) for (k, v) in dfs.items())
        new_index = pandas_index_union(x.index for x in dfs.values())
        new_cols = []
        for (k, v) in dfs.items():
            op = lambda x : Decimal(x["adjusted_close"]) / Decimal(x["price_denominator"])
            new_cols.append(v.apply(op, axis="columns").reindex(index=new_index, fill_value="-").rename(k))
        return pd.concat(new_cols, axis="columns")

    ## Currently Unused
    #@staticmethod
    #def stock_timeseries_daily__to_adjcloseprice_table(dfs):
    #    """
    #    Takes in whatever stock_timeseries_daily() returns.

    #    Returns a single dataframe, merging adjusted close prices for all symbols from dfs.
    #    """
    #    assert isinstance(dfs, dict)
    #    assert all(isinstance(k, str) and isinstance(v, pd.DataFrame) for (k, v) in dfs.items())
    #    new_index = pandas_index_union(x.index for x in dfs.values())
    #    new_cols = []
    #    for (k, v) in dfs.items():
    #        new_cols.append(v["adjusted_close"].rename((k, "numerator")).reindex(index=new_index, fill_value=-1))
    #        new_cols.append(v["price_denominator"].rename((k, "denominator")).reindex(index=new_index, fill_value=0))
    #        new_cols.append(v["unit"].rename((k, "unit")).reindex(index=new_index, fill_value="[N/A]"))
    #    return pd.concat(new_cols, axis="columns")

