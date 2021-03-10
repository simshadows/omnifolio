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

from .user_data_providers.trades_data_provider import get_trades
from .portfolio_holdings_avg_cost import PortfolioHoldingsAvgCost

from .config import get_config
from .structs import (
        TradeInfo,
        Currency,
    )
from .utils import (
        fwrite_json,
        create_json_writable_debugging_structure
    )

logger = logging.getLogger(__name__)

class PortfolioTracker:

    _PORTFOLIO_HISTORY_DEBUGGING__FILEPATH = "portfolio_history.json"

    def __init__(self, config=get_config()):
        self._user_data_path = config["user_data_path"]

        # TODO: Set the directory name somewhere else.
        self._debugging_path = os.path.join(config["generated_data_path"], "debugging")
        return

    def get_trades(self):
        """
        Convenient pass-through to the standalone function which doesn't require the caller
        to pass in any filepaths.
        """
        return get_trades(self._user_data_path)

    def get_full_portfolio_history(self, full_trade_history=None):
        """
        Calculates an exact and detailed portfolio history.

        If called with full_trade_history == None, then this method will first get trades data
        from the .get_trades() method before processing it into a dataframe.

        TODO: Consider implementing some kind of lazy data structure.
        """
        history = [x for x in self.generate_portfolio_history(full_trade_history)]
        self._dump_portfolio_history_debugging_file(history)
        return history

    def get_summary_dataframe(self, full_trade_history=None):
        """
        Returns a summary of holdings, using the global average cost method.

        Returned object is a tuple, where:
            [0] is the dataframe, and
            [1] is the complete set of symbols.
        """
        raw = self._generate_data_for_summary_dataframe(full_trade_history)
        index = []
        data = []
        symbols = set()

        # Purely for debugging
        debugging_cum_symbols = set()

        for (k, v) in raw.items():
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

        return (df, symbols)

    ######################################################################################
    ######################################################################################

    def generate_portfolio_history(self, trade_history_iterable, **kwargs):
        """
        [Generator function.]

        (TODO: properly document what this function is supposed to be.)

        Starts with an initial portfolio state (initial_portfolio_state), and generates evolutions
        of this state for every entry in the trade history (trade_history_iterable).
        """
        if trade_history_iterable is None:
            trade_history_iterable = self.get_trades()

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

    ######################################################################################
    ######################################################################################

    def _generate_data_for_summary_dataframe(self, full_trade_history):
        data = {}

        new_cumulative_entry = lambda : {
                "total_realized_capital_gain_before_fees": None,
                "total_fees_paid": None,
            }
        cumulative = {} # {ric_symbol: {}}

        for curr_state in self.generate_portfolio_history(full_trade_history):
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
                    "cumulative": cumulative,
                }
            data[date] = d

        return data

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

