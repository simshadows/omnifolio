# -*- coding: ascii -*-

"""
Filename: portfolio_tracker.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import os
import logging
import datetime
from copy import copy, deepcopy
from collections import defaultdict
from fractions import Fraction

import numpy as np
import pandas as pd

from .market_data_aggregator import MarketDataAggregator
from .user_data_providers.trades_data_provider import get_trades
from .portfolio_holdings_avg_cost import PortfolioHoldingsAvgCost

from .config import get_config
from .structs import (
        TradeInfo,
        Currency,
    )
from .utils import (
        fwrite_json,
        create_json_writable_debugging_structure,
        pandas_add_column_level_above,
        dump_df_to_csv_debugging_file,
    )

logger = logging.getLogger(__name__)

class PortfolioTracker:

    _PORTFOLIO_HISTORY_DEBUGGING__FILEPATH = "portfolio_history.json"

    def __init__(self, benchmark_symbols, config=get_config(), *, update_store):
        assert isinstance(update_store, bool)

        self._user_data_path = config["user_data_path"]

        # TODO: Set the directory name somewhere else.
        self._debugging_path = os.path.join(config["generated_data_path"], "debugging")

        trade_history_iterable = get_trades(self._user_data_path)
        portfolio_history_iterable = self._generate_portfolio_history(
                trade_history_iterable,
            )
        holdings_summary_dfs = self._generate_holdings_summary_dataframes(
                portfolio_history_iterable,
            )
        (summary_df, symbols) = self._make_holdings_summary_dataframe(
                holdings_summary_dfs,
                debugging_path=self._debugging_path,
            )
        portfolio_value_history_df = self._make_portfolio_value_history_dataframe(
                summary_df,
                symbols,
                update_store=update_store,
                debugging_path=self._debugging_path,
            )
        self._portfolio_stats_history = self._add_benchmarks_to_value_history_dataframe(
                portfolio_value_history_df,
                benchmark_symbols,
                update_store=update_store,
                debugging_path=self._debugging_path,
            )
        self._portfolio_stats_history.dropna(how="all", inplace=True)
        return

    def get_portfolio_stats_history(self):
        return deepcopy(self._portfolio_stats_history)

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
            d = {("comment", "", ""): v["comment"]}
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
        return (df, symbols)

    @staticmethod
    def _make_portfolio_value_history_dataframe(summary_df, symbols, *, update_store, debugging_path):
        assert isinstance(summary_df, pd.DataFrame)
        assert isinstance(symbols, set)

        # TODO: Derive symbols from summary df.

        # TODO: This code is currently unable to handle splits/consolidations. Fix this!

        market_data_source = MarketDataAggregator()
        market_data_df = market_data_source.stock_timeseries_daily(list(symbols), update_store=update_store)
        market_data_df = market_data_source.stock_timeseries_daily__to_adjclose_summary(market_data_df)
        pandas_add_column_level_above(market_data_df, "prices", inplace=True)
        df = market_data_df.join(summary_df)
        assert df.index.is_monotonic

        df.fillna(method="ffill", inplace=True)
        for symbol in symbols:
            def fill(i2, val):
                df.loc[:,("prices", symbol, i2)] = df.loc[:,("prices", symbol, i2)].fillna(value=val)
            fill("unit", "USD")
            fill("adjusted_close", Fraction(0))
            fill("exdividend", Fraction(0))
            fill("split", Fraction(1))

        dump_df_to_csv_debugging_file(df, debugging_path, "portfolio_value_history_dataframe_intermediate.csv")

        def sum_currency_col(i0, i2):
            sers = []
            for symbol in symbols:
                coef = 1.0 if symbol.endswith(".AX") else 1.3 # Temporary fix until I figure out forex
                ser = df.loc[:,(i0, symbol, i2)].fillna(value=Currency("USD", 0))
                ser = ser.apply(lambda x : x.value, convert_dtype=False)
                ser *= coef
                sers.append(ser)
            return sum(sers)

        market_value_sers = []
        for symbol in symbols:
            coef = 1.0 if symbol.endswith(".AX") else 1.3 # Temporary fix until I figure out forex
            mv_ser = df.loc[:,("prices", symbol, "adjusted_close")] * df.loc[:,("stocks", symbol, "unit_quantity")] * coef
            market_value_sers.append(mv_ser)
        market_value_ser = sum(market_value_sers)

        purchase_price_before_fees_ser = sum_currency_col("stocks", "total_value")
        purchase_fees_ser = sum_currency_col("stocks", "total_fees")
        realized_capital_gain_ser = sum_currency_col("cumulative", "total_realized_capital_gain_before_fees")
        total_fees_paid_ser = sum_currency_col("cumulative", "total_fees_paid")

        dividend_gain_sers = []
        for symbol in symbols:
            coef = 1.0 if symbol.endswith(".AX") else 1.3 # Temporary fix until I figure out forex
            dg_ser = (df.loc[:,("prices", symbol, "exdividend")] * df.loc[:,("stocks", symbol, "unit_quantity")]).cumsum()
            dividend_gain_sers.append(dg_ser)
        dividend_gain_ser = sum(dividend_gain_sers)

        base_value_ser = (
                purchase_price_before_fees_ser
                - realized_capital_gain_ser
                - dividend_gain_ser
                + total_fees_paid_ser
            )

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
    def _add_benchmarks_to_value_history_dataframe(df, benchmark_symbols, *, update_store, debugging_path):
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
            update_store
                Whether or not to pull market data from the internet.
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
        market_data_df = market_data_source.stock_timeseries_daily(benchmark_symbols, update_store=update_store)
        drs_df = market_data_source.stock_timeseries_daily__to_dividend_reinvested_scaled(market_data_df)
        assert drs_df.index.is_monotonic
        drs_df.fillna(method="bfill", inplace=True)

        # TODO: How to deal with forex movements?

        for symbol in benchmark_symbols:
            benchmark_currency = drs_df[symbol]["unit"]
            benchmark_unit_price = drs_df[symbol]["drscaled_adjusted_close"]

            # pp_changes basically describes how much we bought in our actual portfolio each
            # day, not accounting for fees.
            pp_changes = df["purchase_price_before_fees"] - df["purchase_price_before_fees"].shift(1, fill_value=Fraction(0))

            joined_sers = {
                    "benchmark_currency": benchmark_currency,
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

            # currency = TODO
            units_owned = []
            realized_gain = []
            benchmark_purchase_price_before_fees = []
            units_owned_curr = Fraction(0)
            realized_gain_curr = Fraction(0)
            benchmark_purchase_price_before_fees_curr = Fraction(0)

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

    def _dump_portfolio_history_debugging_file(self, obj):
        filepath = os.path.join(
                self._debugging_path,
                self._PORTFOLIO_HISTORY_DEBUGGING__FILEPATH,
            )
        logging.debug(f"Writing portfolio debugging history file to '{filepath}'.")
        obj = [copy(x) for x in obj] # Copy only deep enough for our purposes
        for entry in obj:
            entry["account_averages"]["holdings"] = entry["account_averages"]["holdings"].get_holdings()
            entry["global_averages"]["holdings"] = entry["global_averages"]["holdings"].get_holdings()
        fwrite_json(filepath, data=create_json_writable_debugging_structure(obj))
        return

    def _dump_df_to_csv_debugging_file(self, df, filename):
        assert isinstance(df, pd.DataFrame)
        assert isinstance(filename, str)

        filepath = os.path.join(
                self._debugging_path,
                filename,
            )
        logging.debug(f"Writing to debugging file '{filepath}'.")
        with open(filepath, "w") as f:
            f.write(df.dropna(how="all").to_csv())
        return

