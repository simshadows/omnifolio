# -*- coding: ascii -*-

"""
Filename: portfolio_tracker.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import os
import logging
import datetime
from math import isnan
from copy import copy, deepcopy
from collections import defaultdict
from fractions import Fraction
from decimal import Decimal

import numpy as np
import pandas as pd

from .market_data_aggregator import MarketDataAggregator
from .user_data_providers import get_trades
from .portfolio_holdings_avg_cost import PortfolioHoldingsAvgCost

from .config import get_config
from .structs import (
        TradeInfo,
        Currency,
    )
from .utils import (
        fwrite_json,
        fraction_to_decimal,
        create_json_writable_debugging_structure,
        pandas_add_column_level_above,
        dump_df_to_csv_debugging_file,
    )

_logger = logging.getLogger(__name__)


_NUMPY_INT = np.longlong

_PORTFOLIO_HISTORY_DEBUGGING__FILEPATH = "portfolio_history.json"

class PortfolioTracker:

    def __init__(self, benchmark_symbols, config=get_config(), *, update_store):
        assert isinstance(update_store, bool)

        self._user_data_path = config["user_data_path"]
        self._debugging_path = config["debugging_path"]
        self._preferred_currency = config["preferred_currency"]

        trade_history_iterable = get_trades(self._user_data_path)

        self._portfolio_history = list(self._generate_portfolio_history(
                trade_history_iterable,
            ))
        self._dump_portfolio_history_debugging_file(self._portfolio_history, debugging_path=self._debugging_path)

        holdings_summary_dfs = self._generate_holdings_summary_dataframes(
                self._portfolio_history,
            )

        self._summary_df = self._make_holdings_summary_dataframe(
                holdings_summary_dfs,
                debugging_path=self._debugging_path,
            )

        if update_store:
            summary_df_symbols = set(self._summary_df.columns.levels[1])
            all_symbols = summary_df_symbols | set(benchmark_symbols)
            assert all(isinstance(x, str) for x in all_symbols)
            MarketDataAggregator().stock_timeseries_daily__update_store(all_symbols)

        portfolio_value_history_df = self._make_portfolio_value_history_dataframe(
                self._summary_df,
                debugging_path=self._debugging_path,
            )

        self._portfolio_stats_history = self._add_benchmarks_to_value_history_dataframe(
                portfolio_value_history_df,
                benchmark_symbols,
                debugging_path=self._debugging_path,
            )
        self._portfolio_stats_history.dropna(how="all", inplace=True)
        return

    def get_portfolio_stats_history(self):
        return deepcopy(self._portfolio_stats_history)

    # TODO: This function is so messy. Clean it up!
    def get_current_state_summary(self, human_readable_strings=False, hide_closed_positions=True):

        holdings = self._portfolio_history[-1]["global_averages"]["holdings"].get_holdings()["stocks"][""]
        df = pd.DataFrame.from_dict(holdings, orient="index")

        df2 = self._get_last_element_of_summary_df_as_unstacked()

        market_data_source = MarketDataAggregator()
        market_data_dfs = market_data_source.stock_timeseries_daily(list(df2.index), update_store=False)
        market_data_dfs = market_data_source.stock_timeseries_daily__convert_numerics_to_object_types(market_data_dfs)
        latest_market_values_df = market_data_source.stock_timeseries_daily__to_latest_values_df(market_data_dfs)
        cols_to_keep = [
                "date",
                "adjusted_close",
                "last_exdiv_date",
                "last_exdiv",
                "last_split_date",
                "last_split",
            ]
        df3 = latest_market_values_df[cols_to_keep]
        new_cols = pd.Index([
                "price_date",
                "unit_price",
                "last_exdiv_date",
                "last_exdiv",
                "last_split_date",
                "last_split",
            ])
        df3.columns = new_cols
        df3 = pandas_add_column_level_above(df3, "market_data")

        # Verify DataFrame formats
        assert (df.columns == pd.Index([
                "total_value",
                "total_fees",
                "unit_quantity",
            ])).all()
        assert (df2.columns == pd.MultiIndex.from_tuples([
                ("cumulative", "total_fees_paid"),
                ("cumulative", "total_realized_capital_gain_before_fees"),
                ("stocks", "total_fees"),
                ("stocks", "total_value"),
                ("stocks", "unit_quantity"),
            ])).all()
        assert set(df.index) <= set(df2.index)

        # Change column labels
        new_cols = pd.MultiIndex.from_tuples([
            ("open_position", "total_price"),
            ("open_position", "fees_included"),
            ("open_position", "units"),
        ])
        df.columns = new_cols

        # Merge in
        df = df.merge(df2, how="outer", left_index=True, right_index=True)
        df.loc[:,("open_position", "units")].fillna(value=_NUMPY_INT(0), inplace=True)
        fill_values = df.loc[:,("cumulative", "total_realized_capital_gain_before_fees")].apply(lambda x : Currency(x.symbol, 0))
        df.loc[:,("open_position", "total_price")].fillna(value=fill_values, inplace=True)
        fill_values = df.loc[:,("cumulative", "total_fees_paid")].apply(lambda x : Currency(x.symbol, 0))
        df.loc[:,("open_position", "fees_included")].fillna(value=fill_values, inplace=True)

        # Merge in market data
        df = df3.merge(df, how="outer", left_index=True, right_index=True)
        df.index.names = ["ric_symbol"]

        # Run some data sanity checks
        if (df.loc[:,("open_position", "units")] != df.loc[:,("stocks", "unit_quantity")]).any():
            raise RuntimeError("Expected unit quantity columns to match.")
        ## TODO: These doesn't work due to the use of 0 USD as a fill value. This should be fixed!
        #if (df.loc[:,("open_position", "total_price")] != df.loc[:,("stocks", "total_value")]).any():
        #    raise RuntimeError("Expected purchase value columns to match.")
        #if (df.loc[:,("open_position", "fees_included")] > df.loc[:,("stocks", "total_fees")]).any():
        #    raise RuntimeError("Expected purchase fee to be less or equal to total fees.")

        # Remove unnecessary columns, and rename the remaining ones
        cols_to_delete = [
                ("stocks", "unit_quantity"),
                ("stocks", "total_fees"),
                ("stocks", "total_value"),
            ]
        df.drop(cols_to_delete, axis="columns", inplace=True)

        # Add the fees into the total price since the original total_value doesn't include them
        df.loc[:, ("open_position", "total_price")] += df.loc[:, ("open_position", "fees_included")]

        # Repurpose some columns, and rename columns as needed
        df.loc[:, ("cumulative", "total_fees_paid")] -= df.loc[:, ("open_position", "fees_included")]
        df.loc[:, ("cumulative", "total_realized_capital_gain_before_fees")] -= df.loc[:, ("cumulative", "total_fees_paid")]
        # TODO: Make this only rename what's needed rather than setting the entire index at once.
        new_cols = pd.MultiIndex.from_tuples([
                ("market_data", "price_date"),
                ("market_data", "unit_price"),
                ("market_data", "last_exdiv_date"),
                ("market_data", "last_exdiv"),
                ("market_data", "last_split_date"),
                ("market_data", "last_split"),
                ("open_position", "total_price"),
                ("open_position", "fees_included"),
                ("open_position", "units"),
                ("closed_position", "fees_included"),
                ("closed_position", "capital_gain"),
            ])
        df.columns = new_cols

        # Add new column
        position_market_values = df["open_position"]["units"] * df["market_data"]["unit_price"]
        df.insert(len(df.columns), ("open_position", "total_mkt_value"), position_market_values)

        # Add new column
        unrealized_capital_gain = df["open_position"]["total_mkt_value"] - df["open_position"]["total_price"]
        df.insert(len(df.columns), ("open_position", "capital_gain"), unrealized_capital_gain)

        # Add new column
        net_gain = df["open_position"]["capital_gain"] + df["closed_position"]["capital_gain"]
        df.insert(len(df.columns), ("total", "net_gain"), net_gain)

        # Add stand-in column
        df.insert(len(df.columns), ("open_position", "forex_gain"), "NotImpl.")

        # Add stand-in column
        df.insert(len(df.columns), ("closed_position", "dividend_gain"), "NotImpl.")

        # Add stand-in column
        df.insert(len(df.columns), ("closed_position", "forex_gain"), "NotImpl.")

        # Reorder columns
        new_column_order = [
                ("market_data", "price_date"),
                ("market_data", "unit_price"),
                ("market_data", "last_exdiv_date"),
                ("market_data", "last_exdiv"),
                ("market_data", "last_split_date"),
                ("market_data", "last_split"),
                ("open_position", "units"),
                ("open_position", "total_price"),
                ("open_position", "fees_included"),
                ("open_position", "total_mkt_value"),
                ("open_position", "capital_gain"),
                ("open_position", "forex_gain"),
                ("closed_position", "capital_gain"),
                ("closed_position", "dividend_gain"),
                ("closed_position", "forex_gain"),
                ("closed_position", "fees_included"),
                ("total", "net_gain"),
            ]
        df = df[new_column_order]

        if human_readable_strings:
            # Sort
            df.sort_index(axis="index", inplace=True)
            #df.sort_values(by=[("open_position", "units"), "ric_symbol"], axis="index", inplace=True)

            # Move all closed positions to the end
            all_open = df.loc[df.loc[:, ("open_position", "units")] != 0]
            all_closed = df.loc[df.loc[:, ("open_position", "units")] == 0]
            df = all_open.append(all_closed)

            #
            # Round numbers
            #

            currency_2dc_cols = [
                    ("open_position", "total_price"),
                    ("open_position", "fees_included"),
                    ("open_position", "total_mkt_value"),
                    ("open_position", "capital_gain"),
                    ("closed_position", "capital_gain"),
                    ("closed_position", "fees_included"),
                    ("total", "net_gain"),
                ]
            for col_name in currency_2dc_cols:
                df.loc[:, col_name] = df.loc[:, col_name].apply(lambda x : x.rounded_str(decimal_places=2))

            currency_3dc_cols = [
                    ("market_data", "unit_price"),
                ]
            for col_name in currency_3dc_cols:
                df.loc[:, col_name] = df.loc[:, col_name].apply(lambda x : x.rounded_str(decimal_places=3))

            numeric_5dc_cols = [
                    ("market_data", "last_split"),
                ]
            op = lambda x : np.nan if isnan(x) else f"{fraction_to_decimal(x):,.5f}"
            for col_name in numeric_5dc_cols:
                df.loc[:, col_name] = df.loc[:, col_name].apply(op)

        return df

    def get_aggregate_summary(self):
        # TODO
        return "TO BE IMPLEMENTED"

    ##########################
    # Various helper methods #
    ##########################

    # TODO: Oh god, I really need to find better names for these things. "summary_df" is really bad.
    def _get_last_element_of_summary_df_as_unstacked(self):
        df = pd.DataFrame(self._summary_df.iloc[-1])

        assert set(df.index.levels[0]) == {"cumulative", "stocks"}
        assert set(df.index.levels[2]) == {"total_fees", "total_fees_paid", "total_realized_capital_gain_before_fees",
                                           "total_value", "unit_quantity"}

        df = df.unstack(level=0).unstack(level=1).droplevel(0, axis="columns")
        cols_to_drop = [
                ("cumulative", "total_fees"),
                ("cumulative", "total_value"),
                ("cumulative", "unit_quantity"),
                ("stocks", "total_fees_paid"),
                ("stocks", "total_realized_capital_gain_before_fees"),
            ]
        df.drop(cols_to_drop, axis="columns", inplace=True)
        return df

    ##############################################
    # Portfolio history data processing pipeline #
    ##############################################

    @staticmethod
    def _generate_portfolio_history(trade_history_iterable, **kwargs):
        """
        [Generator function.]

        (TODO: properly document what this function is supposed to be.)

        Starts with an initial portfolio state (initial_portfolio_state), and generates evolutions
        of this state for every entry in the trade history (trade_history_iterable).
        """

        holdings_acc_avgs = deepcopy(kwargs.get("holdings_account_averages", PortfolioHoldingsAvgCost(lambda x : x.account)))
        assert isinstance(holdings_acc_avgs, PortfolioHoldingsAvgCost)

        holdings_global_avgs = deepcopy(kwargs.get("holdings_global_averages", PortfolioHoldingsAvgCost(lambda x : "")))
        assert isinstance(holdings_global_avgs, PortfolioHoldingsAvgCost)

        for individual_trade in trade_history_iterable:
            assert isinstance(individual_trade, TradeInfo)

            trade_detail = deepcopy(individual_trade)
            acc_avgs_diff = holdings_acc_avgs.trade(trade_detail)
            global_avgs_diff = holdings_global_avgs.trade(trade_detail)

            entry = {
                    # Data from these fields are not cumulative.
                    # Thus, the caller must save this data themselves.
                    "trade_detail": trade_detail,

                    "account_averages": {
                        "holdings": deepcopy(holdings_acc_avgs),
                        "disposed_during_this_trade": acc_avgs_diff.disposed,
                        "acquired_during_this_trade": acc_avgs_diff.acquired,
                    },
                    "global_averages": {
                        "holdings": deepcopy(holdings_global_avgs),
                        "disposed_during_this_trade": global_avgs_diff.disposed,
                        "acquired_during_this_trade": global_avgs_diff.acquired,
                    },

                    # None of this is needed yet.
                    ## Data from this field is derived from all the other fields, and is provided for convenience.
                    ## Ignoring it will not cause information loss.
                    #"stats": {
                    #    "portfolio_state": "NOT_IMPLEMENTED",
                    #    "portfolio_lifetime": "NOT_IMPLEMENTED",
                    #    "trade": "NOT_IMPLEMENTED",
                    #},
                }
            yield entry
        return

    @staticmethod
    def _generate_holdings_summary_dataframes(portfolio_history_iterable):
        data = {}

        new_cumulative_entry = lambda : {
                "total_realized_capital_gain_before_fees": None,
                "total_fees_paid": None,
            }
        cumulative = {} # {ric_symbol: {}}

        for curr_state in portfolio_history_iterable:
            date = curr_state["trade_detail"].trade_date
            assert isinstance(date, datetime.date)

            ric_symbol = curr_state["trade_detail"].ric_symbol
            assert isinstance(ric_symbol, str)

            fees = curr_state["trade_detail"].total_fees
            assert isinstance(fees, Currency)

            all_value_gain = []
            for disposal in curr_state["global_averages"]["disposed_during_this_trade"]["stocks"]:
                if ric_symbol != disposal["ric_symbol"]:
                    raise ValueError("At this point in development, it is not expected to dispose multiple symbols in one trade.")
                value_gain = (disposal["disposal_unit_price"] - disposal["acquisition_unit_price"]) * disposal["unit_quantity"]
                assert isinstance(value_gain, Currency)
                all_value_gain.append(value_gain)

            if len(all_value_gain) > 1:
                raise ValueError("At this point in development, it is not expected to see multiple disposal entries in one.")
            elif len(all_value_gain) == 1:
                all_value_gain = all_value_gain[0]
            else:
                assert len(all_value_gain) == 0
                all_value_gain = Currency(curr_state["trade_detail"].unit_price.symbol, 0)

            if ric_symbol in cumulative:
                cumulative[ric_symbol]["total_realized_capital_gain_before_fees"] += all_value_gain
                cumulative[ric_symbol]["total_fees_paid"] += fees
            else:
                cumulative[ric_symbol] = {
                        "total_realized_capital_gain_before_fees": all_value_gain,
                        "total_fees_paid": fees,
                    }

            d = {
                    "comment": curr_state["trade_detail"].comment,
                    "stocks": curr_state["global_averages"]["holdings"].get_holdings()["stocks"][""],
                    "cumulative": deepcopy(cumulative),
                }
            data[date] = d

        return data

    @staticmethod
    def _make_holdings_summary_dataframe(holdings_summary_dfs, *, debugging_path):
        """
        Returns a summary of holdings, using the global average cost method.

        Returned object is a tuple, where:
            [0] is the dataframe, and
            [1] is the complete set of symbols.
        """
        assert isinstance(holdings_summary_dfs, dict)
        index = []
        data = []
        symbols = set()

        # Purely for debugging
        debugging_cum_symbols = set()

        for (k, v) in holdings_summary_dfs.items():
            d = {}
            for (symbol, holding_data) in v["stocks"].items():
                if len(holding_data) > 0:
                    symbols.add(symbol)
                for (k2, v2) in holding_data.items():
                    d[("stocks", symbol, k2)] = v2
            for (symbol, cumulative_data) in v["cumulative"].items():
                if len(cumulative_data) > 0:
                    debugging_cum_symbols.add(symbol)
                for (k2, v2) in cumulative_data.items():
                    d[("cumulative", symbol, k2)] = v2
            index.append(k)
            data.append(d)

        assert symbols == debugging_cum_symbols

        df = pd.DataFrame(data=data, index=index)
        df.index.rename("date")

        new_index = df.index.astype("datetime64[ns]")
        new_columns = pd.MultiIndex.from_tuples(df.columns)
        df = df.reindex(columns=new_columns, index=new_index)

        # Now, we fill missing data
        for symbol in symbols:
            def fill(i0, i2, value):
                col = (i0, symbol, i2)
                df.loc[:,col] = df.loc[:,col].fillna(value=value)
            fill("stocks", "total_value", Currency("USD", 0))
            fill("stocks", "total_fees", Currency("USD", 0))
            fill("stocks", "unit_quantity", Fraction(0))
            fill("cumulative", "total_realized_capital_gain_before_fees", Currency("USD", 0))
            fill("cumulative", "total_fees_paid", Currency("USD", 0))

        dump_df_to_csv_debugging_file(df, debugging_path, "holdings_summary_dataframe.csv")
        return df

    @staticmethod
    def _make_portfolio_value_history_dataframe(summary_df, *, debugging_path):
        assert isinstance(summary_df, pd.DataFrame)

        # TODO: This code is currently unable to handle splits/consolidations. Fix this!

        symbols = set(summary_df.columns.levels[1])

        market_data_source = MarketDataAggregator()
        # TODO: stock_timeseries_daily is called three times. Deduplicate it?
        market_data_df = market_data_source.stock_timeseries_daily(list(symbols), update_store=False)
        market_data_df = market_data_source.stock_timeseries_daily__convert_numerics_to_object_types(market_data_df)
        market_data_df = market_data_source.stock_timeseries_daily__to_adjclose_summary(market_data_df)
        market_data_df = pandas_add_column_level_above(market_data_df, "prices")
        df = market_data_df.join(summary_df)
        assert df.index.is_monotonic

        df.fillna(method="ffill", inplace=True)
        for symbol in symbols:
            def fill(i2, val):
                df.loc[:,("prices", symbol, i2)] = df.loc[:,("prices", symbol, i2)].fillna(value=val)
            fill("adjusted_close", Currency("USD", 0))
            fill("exdividend", Currency("USD", 0))
            fill("split", Fraction(1))

        dump_df_to_csv_debugging_file(df, debugging_path, "portfolio_value_history_dataframe_intermediate.csv")

        def sum_currency_col(i0, i2):
            sers = []
            for symbol in symbols:
                coef = Fraction(1) if symbol.endswith(".AX") else Fraction(numerator=13, denominator=10) # TODO: Temporary fix until I figure out forex
                ser = df.loc[:,(i0, symbol, i2)].fillna(value=Currency("USD", 0))
                ser = ser.apply(lambda x : x.convert("AUD", coef), convert_dtype=False) # Temporary fix until I figure out forex
                sers.append(ser)
            # TODO: Make this sum() version work somehow? The loop of individual += operations is terrible.
            #return sum(sers)
            ret = sers[0]
            for ser in sers[1:]:
                ret += ser
            return ret

        market_value_sers = []
        for symbol in symbols:
            coef = Fraction(1) if symbol.endswith(".AX") else Fraction(numerator=13, denominator=10) # TODO: Temporary fix until I figure out forex
            mv_ser = df.loc[:,("prices", symbol, "adjusted_close")]#.fillna(value=Currency("USD", 0))
            mv_ser *= df.loc[:,("stocks", symbol, "unit_quantity")].fillna(value=Fraction(0))
            #mv_ser = mv_ser.fillna(value=Currency("USD", 0))
            mv_ser = mv_ser.apply(lambda x : x.convert("AUD", coef), convert_dtype=False) # Temporary fix until I figure out forex
            market_value_sers.append(mv_ser)
        # TODO: Make this sum() version work somehow? The loop of individual += operations is terrible.
        #market_value_ser = sum(market_value_sers)
        market_value_ser = market_value_sers[0]
        for mv_ser in market_value_sers[1:]:
            market_value_ser += mv_ser

        purchase_price_before_fees_ser = sum_currency_col("stocks", "total_value")
        purchase_fees_ser = sum_currency_col("stocks", "total_fees")
        realized_capital_gain_ser = sum_currency_col("cumulative", "total_realized_capital_gain_before_fees")
        total_fees_paid_ser = sum_currency_col("cumulative", "total_fees_paid")

        dividend_gain_sers = []
        for symbol in symbols:
            coef = Fraction(1) if symbol.endswith(".AX") else Fraction(numerator=13, denominator=10) # TODO: Temporary fix until I figure out forex
            dg_ser = df.loc[:,("prices", symbol, "exdividend")].apply(lambda x : x.convert("AUD", coef), convert_dtype=False)
            dg_ser = (dg_ser * df.loc[:,("stocks", symbol, "unit_quantity")].fillna(value=Fraction(0))).cumsum()
            #dg_ser = dg_ser.fillna(value=Currency("USD", 0))
            dg_ser = dg_ser.apply(lambda x : x.convert("AUD", coef), convert_dtype=False) # Temporary fix until I figure out forex
            dividend_gain_sers.append(dg_ser)
        # TODO: Make this sum() version work somehow? The loop of individual += operations is terrible.
        #dividend_gain_ser = sum(dividend_gain_sers)
        dividend_gain_ser = dividend_gain_sers[0]
        for dg_ser in dividend_gain_sers[1:]:
            dividend_gain_ser += dg_ser

        base_value_ser = (
                purchase_price_before_fees_ser
                - realized_capital_gain_ser
                - dividend_gain_ser
                + total_fees_paid_ser
            )
        base_value_ser.mask(base_value_ser == 0, other=np.nan, inplace=True)

        all_sers = {
                "market_value": market_value_ser,
                "proportion_net_profit": market_value_ser / base_value_ser,
                "net_profit": market_value_ser - base_value_ser,
                "all_fees": total_fees_paid_ser,
                "base_value": base_value_ser,

                # TODO: Calculate purchase fees and past fees. (i.e. fees considered bound
                # to existing holdings, and fees bound to sold holdings.)

                "purchase_price_before_fees": purchase_price_before_fees_ser,
                #"purchase_fees": purchase_fees_ser,
                "realized_capital_gain": realized_capital_gain_ser,
                "dividend_gain": dividend_gain_ser,
                #"past_fees": past_fees_ser,
            }
        new_df = pd.concat(all_sers, axis="columns").dropna(how="any")

        dump_df_to_csv_debugging_file(new_df, debugging_path, "portfolio_value_history_dataframe.csv")
        return new_df

    @staticmethod
    def _add_benchmarks_to_value_history_dataframe(df, benchmark_symbols, *, debugging_path):
        """
        Adds benchmarks to a portfolio value history dataframe.

        Benchmarks run a simulation using your trading history where instead of buying/selling
        what you actually bought, it instead buys/sells the benchmark security instead.

        All benchmark values assume your fees are the same as your actual trading history.

        Parameters:
            df
                Whatever _make_portfolio_value_history_dataframe() outputs, pass it into here.
            benchmark_symbols
                An iterable of stock symbols to be used as benchmarks.
            debugging_path
                Path for debugging output files.
        Returns:
            A new dataframe that includes benchmarks.
            TODO: Document what the new dataframe looks like.
        """
        assert isinstance(df, pd.DataFrame)
        benchmark_symbols = list(benchmark_symbols)
        assert len(benchmark_symbols) == len(set(benchmark_symbols))

        market_data_source = MarketDataAggregator()
        # TODO: stock_timeseries_daily is called three times. Deduplicate it?
        market_data_df = market_data_source.stock_timeseries_daily(benchmark_symbols, update_store=False)
        drs_df = market_data_source.stock_timeseries_daily__to_dividend_reinvested_scaled(market_data_df)
        assert drs_df.index.is_monotonic
        drs_df.fillna(method="bfill", inplace=True)

        # TODO: How to deal with forex movements?

        for symbol in benchmark_symbols:
            benchmark_unit_price = drs_df[symbol]["drscaled_adjusted_close"]

            # pp_changes basically describes how much we bought in our actual portfolio each
            # day, not accounting for fees.
            # TODO: Fix this hard-coded currency symbol.
            pp_changes = df["purchase_price_before_fees"] - df["purchase_price_before_fees"].shift(1, fill_value=Currency("AUD", 0))

            joined_sers = {
                    "benchmark_unit_price": benchmark_unit_price,
                    "purchase_price_changes": pp_changes,
                    "actual_portfolio_market_value": df["market_value"],
                }
            joined_df = pd.concat(joined_sers, axis="columns").dropna(how="any")

            # When base_changes is positive, we buy the equivalent in benchmark units.
            # When base_changes is negative, we sell a proportional amount of benchmark units, and record profit/loss.

            units_to_buy = joined_df["purchase_price_changes"] / joined_df["benchmark_unit_price"]
            proportion_to_sell = (
                    (-joined_df["purchase_price_changes"])
                    / df["purchase_price_before_fees"].shift(1, fill_value=np.nan)
                )

            units_bought_ser = pd.Series(index=joined_df.index, dtype="object")
            units_bought_ser = units_bought_ser.mask(joined_df["purchase_price_changes"] > 0, units_to_buy)
            units_bought_ser.fillna(value=Fraction(0), inplace=True)
            joined_df.insert(len(joined_df.columns), "benchmark_units_bought", units_bought_ser)

            proportion_sold_ser = pd.Series(index=joined_df.index, dtype="object")
            proportion_sold_ser = proportion_sold_ser.mask(joined_df["purchase_price_changes"] < 0, proportion_to_sell)
            proportion_sold_ser.fillna(value=Fraction(0), inplace=True)
            joined_df.insert(len(joined_df.columns), "benchmark_proportion_sold", proportion_sold_ser)

            units_owned = []
            realized_gain = []
            benchmark_purchase_price_before_fees = []
            units_owned_curr = Fraction(0)
            realized_gain_curr = Currency("AUD", 0) # TODO: fix hardcoded symbol
            benchmark_purchase_price_before_fees_curr = Currency("AUD", 0) # TODO: fix hardcoded symbol

            for (date, row) in joined_df.iterrows():
                if row["benchmark_units_bought"] > 0:
                    assert row["benchmark_proportion_sold"] == 0 # They are mutually exclusive conditions
                    units_owned_curr += row["benchmark_units_bought"]
                    benchmark_purchase_price_before_fees_curr += row["benchmark_units_bought"] * row["benchmark_unit_price"]
                elif row["benchmark_proportion_sold"] > 0:
                    units_sold = units_owned_curr * row["benchmark_proportion_sold"]
                    sold_base_price_before_fees = benchmark_purchase_price_before_fees_curr * row["benchmark_proportion_sold"]
                    gross_proceeds = units_sold * row["benchmark_unit_price"]

                    units_owned_curr -= units_sold
                    realized_gain_curr += gross_proceeds - sold_base_price_before_fees
                    benchmark_purchase_price_before_fees_curr -= sold_base_price_before_fees
                units_owned.append(units_owned_curr)
                realized_gain.append(realized_gain_curr)
                benchmark_purchase_price_before_fees.append(benchmark_purchase_price_before_fees_curr)

            units_owned = pd.Series(data=units_owned, index=joined_df.index)
            realized_gain = pd.Series(data=realized_gain, index=joined_df.index)
            benchmark_purchase_price_before_fees = pd.Series(data=benchmark_purchase_price_before_fees, index=joined_df.index)

            benchmark_market_value = units_owned * joined_df["benchmark_unit_price"]
            benchmark_net_profit = (
                        benchmark_market_value
                        - benchmark_purchase_price_before_fees
                        + realized_gain
                        - df["all_fees"]
                    )

            #df.insert(len(df.columns), "benchmark_" + symbol + "_purchased_before_fees", benchmark_purchase_price_before_fees)

            net_profit_colname = "benchmark_net_profit_" + symbol
            df.insert(len(df.columns), net_profit_colname, benchmark_net_profit)
            df.loc[:,net_profit_colname].fillna(method="ffill", inplace=True)

        dump_df_to_csv_debugging_file(df, debugging_path, "portfolio_value_history_dataframe_with_benchmarks.csv")
        return df #(df, drs_df)

    @staticmethod
    def _dump_portfolio_history_debugging_file(obj, *, debugging_path):
        filepath = os.path.join(
                debugging_path,
                _PORTFOLIO_HISTORY_DEBUGGING__FILEPATH,
            )
        logging.debug(f"Writing portfolio debugging history file to '{filepath}'.")
        obj = [deepcopy(x) for x in obj]
        for entry in obj:
            entry["account_averages"]["holdings"] = entry["account_averages"]["holdings"].get_holdings()
            entry["global_averages"]["holdings"] = entry["global_averages"]["holdings"].get_holdings()
        fwrite_json(filepath, data=create_json_writable_debugging_structure(obj))
        return

