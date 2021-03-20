# -*- coding: ascii -*-

"""
Filename: yahoo_finance_lib.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Pulls data from the yfinance library.

WARNING: The library presents data in floating point, and the YahooFinanceLib attempts to make
the best possible approximation from it. The maximum precision of prices will be in 100ths of
a unit of currency (e.g. cents is the maximum precision for USD prices), and dividends are very
likely to be incorrect for decimal places below a single cent.

This library is merely a stopgap until a better library or API can be found.

As a result, this data provider also has a lower data_trust_value score to deprioritize it as
necessary.

(I might modify the yfinance library later to have the option for decimal calculations, and
submit a pull request...)
"""

import copy
import datetime
import logging
from collections import OrderedDict

import numpy as np
import pandas as pd
import yfinance

from ..market_data_provider import MarketDataProvider

from ..structs import (
        CurrencyPair,
    )
from ..utils import (
        str_is_nonempty_and_compact,
        pandas_add_column_level_above,
        dump_df_to_csv_debugging_file, # Convenient to use occasionally when debugging
    )

_logger = logging.getLogger(__name__)


_NUMPY_INT = np.longlong
_NUMPY_FLOAT = np.double

_INT_MAX = np.iinfo(_NUMPY_INT).max
assert np.iinfo(_NUMPY_INT).bits >= 64


class YahooFinanceLib(MarketDataProvider):

    _PRICE_DENOMINATOR = 100
    _DIVIDEND_DENOMINATOR = 1000**3
    
    _FOREX_DENOMINATOR = 1000000

    def __init__(self):
        # No API key necessary!
        return

    def get_trust_value(self):
        return (self.SOMEWHAT_UNTRUSTED
                - self.FLOATING_POINT_IMPRECISION_DEDUCTION
                - self.PRECISION_GUESS_DEDUCTION
                - self.CURRENCY_ADJUSTMENT_DEDUCTION)

    ######################################################################################
    # stock_timeseries_daily #############################################################
    ######################################################################################

    def stock_timeseries_daily(self, symbols_list):
        if not isinstance(symbols_list, list):
            raise TypeError
        if len(symbols_list) == 0:
            raise ValueError("Expected non-empty list.")
        if not all(str_is_nonempty_and_compact(x) for x in symbols_list):
            raise TypeError
        if len(symbols_list) != len(set(symbols_list)):
            raise ValueError("List must not have duplicates.")

        curr_time = datetime.datetime.utcnow()

        _logger.info("Downloading stock price history.")
        df = self._stock_timeseries_daily__download_raw_data(symbols_list)
        self._stock_timeseries_daily__verify_raw_data_format(df, symbols_list)
        ret = self._stock_timeseries_daily__process_data(df, symbols_list, curr_time)

        assert isinstance(ret, dict)
        assert all(isinstance(k, str) and isinstance(v, pd.DataFrame) for (k, v) in ret.items())

        return ret

    ######################################################################################
    ######################################################################################

    def _stock_timeseries_daily__download_raw_data(self, symbols_list):
        assert len(symbols_list) != 0
        df = yfinance.download(
                tickers=symbols_list,
                period="max",
                interval="1d",
                actions=True,
                group_by="ticker",
                threads=True,
            )
        if len(symbols_list) == 1:
            df = pandas_add_column_level_above(df, symbols_list[0])
        return df

    def _stock_timeseries_daily__verify_raw_data_format(self, df, symbols_list):
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
        if not all((x == "float64") or (x == "int64")for x in df.dtypes):
            raise TypeError
        return

    def _stock_timeseries_daily__process_data(self, df, symbols_list, data_collection_time):
        df.sort_index(ascending=True, inplace=True)

        _logger.info("Downloading symbol currency data.")
        currencies = self._stock_timeseries_daily__get_currencies(symbols_list)

        ret = {}
        for symbol in symbols_list:
            new_df = df[symbol].dropna(axis="index", how="all")

            self._stock_timeseries_daily__rename_column_labels(new_df)
            self._stock_timeseries_daily__change_values_to_presplit_and_sort(new_df)

            # Now, we multiply our currencies by their denominators.

            price_columns = ["open", "high", "low", "close", "adjusted_close"]

            new_df[price_columns] *= self._PRICE_DENOMINATOR
            new_df["exdividend"] *= self._DIVIDEND_DENOMINATOR

            # We now check the volume column for any fractional components.

            volume_fractionals_check = (new_df["volume"] % 1) != 0
            if volume_fractionals_check.any():
                # This loop will find a sample, then issue a warning along with a printout of the sample.
                # TODO: Make this more efficient.
                for row_index, row_has_fractional_volume in volume_fractionals_check.iteritems():
                    assert isinstance(row_has_fractional_volume, bool)
                    if row_has_fractional_volume:
                        sample_row = new_df.loc[row_index]
                        _logger.warning(f"Unexpected fractional volume for symbol {symbol}. "
                                        "This shouldn't have happened. "
                                        "This will be rounded to the nearest integer. "
                                        f"\nSample: \n{row_index}\n{sample_row}\n")
                        break
                else:
                    raise RuntimeError("Expected to find a sample.")

            # We now round, then cast types to integer.

            new_column_types = {
                    "open":           _NUMPY_INT,
                    "high":           _NUMPY_INT,
                    "low":            _NUMPY_INT,
                    "close":          _NUMPY_INT,
                    "adjusted_close": _NUMPY_INT,
                    "volume":         _NUMPY_INT,
                    "exdividend":     _NUMPY_INT,
                    "split":          _NUMPY_FLOAT,
                }
            int_cols_to_retype = list(k for (k, v) in new_column_types.items() if (v is _NUMPY_INT))
            new_df[int_cols_to_retype] = new_df[int_cols_to_retype].round(decimals=0)

            # We forward-fill, in case dividends and splits happen during non-trading days
            new_df.loc[:,"adjusted_close"].fillna(method="ffill", inplace=True)
            new_df.loc[:,"volume"].fillna(method="ffill", inplace=True)
            # These columns just get the previous adjusted_close values.
            new_df.loc[:,"open"].fillna(value=new_df["adjusted_close"], inplace=True)
            new_df.loc[:,"high"].fillna(value=new_df["adjusted_close"], inplace=True)
            new_df.loc[:,"low"].fillna(value=new_df["adjusted_close"], inplace=True)
            new_df.loc[:,"close"].fillna(value=new_df["adjusted_close"], inplace=True)

            # And now, we change columns
            new_df = new_df.astype(new_column_types)

            # Add some new columns
            new_df.insert(0, "dividend_denominator", _NUMPY_INT(self._DIVIDEND_DENOMINATOR))
            new_df.insert(0, "price_denominator", _NUMPY_INT(self._PRICE_DENOMINATOR))
            new_df.insert(0, "unit", currencies[symbol])
            new_df.insert(0, "data_collection_time", data_collection_time)
            new_df.insert(0, "data_trust_value", self.get_trust_value())
            new_df.insert(0, "data_source", self.get_provider_name())

            ret[symbol] = new_df
        return ret

    ######################################################################################
    ######################################################################################

    @staticmethod
    def _stock_timeseries_daily__get_currencies(symbols):
        ret = {}
        for symbol in symbols:
            assert str_is_nonempty_and_compact(symbol)
            assert symbol not in ret
            sym = yfinance.Ticker(symbol)
            currency = sym.info["currency"]
            assert str_is_nonempty_and_compact(currency)
            ret[symbol] = currency
        return ret

    @staticmethod
    def _stock_timeseries_daily__rename_column_labels(df):
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
        df.rename(columns=new_column_names, inplace=True)
        df.index.names = ["date"]
        return

    @staticmethod
    def _stock_timeseries_daily__change_values_to_presplit_and_sort(df):
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
        # We need this to be sorted
        assert df.index.is_monotonic

        df.loc[:, "split"] = np.where(df.loc[:, "split"] == 0, 1, df.loc[:, "split"])
        cum_split = df.loc[:, "split"].cumprod()
        latest_cum_split = cum_split.dropna(axis="index", how="all")[-1]

        # Multiply a bunch of columns by (latest_cum_split / cum_split)
        columns_to_multiply = ["open", "high", "low", "close", "adjusted_close", "exdividend"]
        df.loc[:, columns_to_multiply] = df.loc[:, columns_to_multiply].multiply(latest_cum_split / cum_split, axis="index")

        # Divide only the volume column by (latest_cum_split / cum_split)
        df.loc[:, "volume"] /= (latest_cum_split / cum_split)

        return

    ######################################################################################
    # forex_timeseries_daily #############################################################
    ######################################################################################

    def forex_timeseries_daily(self, currency_pairs_list):
        if not isinstance(currency_pairs_list, list):
            raise TypeError
        if len(currency_pairs_list) == 0:
            raise ValueError("Expected non-empty list.")
        if not all(isinstance(x, CurrencyPair) for x in currency_pairs_list):
            raise TypeError
        if len(currency_pairs_list) != len(set(currency_pairs_list)):
            raise ValueError("List must not have duplicates.")

        curr_time = datetime.datetime.utcnow()

        _logger.info("Downloading forex history.")
        symbols_list = self._forex_timeseries_daily__convert_to_symbols_list(currency_pairs_list)
        df = self._forex_timeseries_daily__download_raw_data(symbols_list)
        self._forex_timeseries_daily__verify_raw_data_format(df, symbols_list)
        ret = self._forex_timeseries_daily__process_data(df, symbols_list, curr_time)

        assert isinstance(ret, dict)
        assert all(isinstance(k, CurrencyPair) and isinstance(v, pd.DataFrame) for (k, v) in ret.items())

        return ret

    ######################################################################################
    ######################################################################################

    @staticmethod
    def _forex_timeseries_daily__convert_to_symbols_list(currency_pairs_list):
        return [(f"{x.base.strip()}{x.quote.strip()}=X", x) for x in currency_pairs_list]

    @staticmethod
    def _forex_timeseries_daily__download_raw_data(symbols_list):
        df = yfinance.download(
                tickers=[x[0] for x in symbols_list],
                period="max",
                interval="1d",
                actions=False,
                group_by="ticker",
                threads=True,
            )
        if len(symbols_list) == 1:
            df = pandas_add_column_level_above(df, symbols_list[0][0])
        return df

    @staticmethod
    def _forex_timeseries_daily__verify_raw_data_format(df, symbols_list):
        if not isinstance(df, pd.DataFrame):
            raise TypeError
        if df.index.name != "Date":
            raise ValueError
        if (set(x[0] for x in symbols_list) != set(df.columns.levels[0])) or (len(symbols_list) == 0):
            raise ValueError
        expected_column_labels = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        if list(df.columns.levels[1]) != expected_column_labels:
            raise ValueError
        if df.index.dtype.name != "datetime64[ns]":
            raise TypeError
        if not all((x == "float64") or (x == "int64")for x in df.dtypes):
            raise TypeError
        if any((df[x[0]]["Close"].dropna() != df[x[0]]["Adj Close"].dropna()).any() for x in symbols_list):
            raise ValueError("Expected 'Close' to be the exact same ad 'Adj Close'.")
        if any((df[x[0]]["Volume"].dropna() != 0).any() for x in symbols_list):
            raise ValueError("Expected 'Volume' to be entirely zero.")
        return

    def _forex_timeseries_daily__process_data(self, df, symbols_list, data_collection_time):
        df.sort_index(ascending=True, inplace=True)

        ret = {}
        for (symbol, currency_pair) in symbols_list:
            new_df = df[symbol].dropna(axis="index", how="all")

            # We already verified that Adj Close is equivalent to Close, and Volume is all zero.
            columns_to_delete = ["Adj Close", "Volume"]
            new_df.drop(columns_to_delete, axis="columns", inplace=True)

            self._forex_timeseries_daily__rename_column_labels(new_df)

            # Now, we multiply our currencies by their denominators.
            new_df *= self._FOREX_DENOMINATOR

            # We now round, then cast types to integer.
            new_column_types = {
                    "open":           _NUMPY_INT,
                    "high":           _NUMPY_INT,
                    "low":            _NUMPY_INT,
                    "close":          _NUMPY_INT,
                }
            int_cols_to_retype = list(new_column_types.keys())
            new_df[int_cols_to_retype] = new_df[int_cols_to_retype].round(decimals=0)
            new_df = new_df.astype(new_column_types)

            # Add some new columns
            new_df.insert(len(new_df.columns), "denominator", _NUMPY_INT(self._FOREX_DENOMINATOR))
            new_df.insert(0, "data_collection_time", data_collection_time)
            new_df.insert(0, "data_trust_value", self.get_trust_value())
            new_df.insert(0, "data_source", self.get_provider_name())

            ret[currency_pair] = new_df
        return ret

    ######################################################################################
    ######################################################################################

    @staticmethod
    def _forex_timeseries_daily__rename_column_labels(df):
        new_column_names = {
                "Open":         "open",
                "High":         "high",
                "Low":          "low",
                "Close":        "close",
            }
        df.rename(columns=new_column_names, inplace=True)
        df.index.names = ["date"]
        return

