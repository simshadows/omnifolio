# -*- coding: ascii -*-

"""
Filename: portfolio_tracker.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import os
import logging
import datetime
from copy import deepcopy
from collections import defaultdict
from fractions import Fraction

from .user_data_providers.trades_data_provider import get_trades

from .config import get_config
from .structs import TradeInfo
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

    def get_portfolio_history(self, full_trade_history=None):
        """
        Calculates an exact and detailed portfolio history.

        If called with full_trade_history == None, then this method will first get trades data
        from the .get_trades() method before processing it into a dataframe.

        TODO: Consider implementing some kind of lazy data structure.
        """
        history = [x for x in self.generate_portfolio_history(full_trade_history)]
        self._dump_portfolio_history_debugging_file(history)
        return history

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

        initial_portfolio_state = kwargs.get("initial_portfolio_state", self.get_blank_portfolio_state())
        assert isinstance(initial_portfolio_state, dict)

        prev_portfolio_state = deepcopy(initial_portfolio_state)
        for individual_trade in trade_history_iterable:
            assert isinstance(individual_trade, TradeInfo)
            trade_detail = deepcopy(individual_trade)
            portfolio_state, disposals, acquisitions = self._generate_portfolio_state_and_diff(trade_detail, prev_portfolio_state)
            portfolio_state_stats = self._generate_portfolio_state_stats(portfolio_state)
            trade_stats = self._generate_trade_stats(disposals)

            prev_portfolio_state = portfolio_state

            entry = {
                    # Data from these fields are not cumulative.
                    # Thus, the caller must save this data themselves.
                    "trade_detail": trade_detail,
                    "disposed_during_this_trade": disposals,
                    "acquired_during_this_trade": acquisitions,

                    # Data from this field is cumulative.
                    "portfolio_state": portfolio_state, # State of the portfolio after the corresponding trade

                    # Data from this field is derived from all the other fields, and is provided for convenience.
                    # Ignoring it will not cause information loss.
                    "stats": {
                        "portfolio_state": portfolio_state_stats,
                        "portfolio_lifetime": "NOT_IMPLEMENTED",
                        "trade": trade_stats,
                    },
                }
            yield entry
        return

    @staticmethod
    def get_blank_portfolio_state():
        return {
                # "stocks": {account : {symbol : [trades]}}
                "stocks": defaultdict(lambda : defaultdict(list)),
            }

    ######################################################################################
    ######################################################################################

    @staticmethod
    def _generate_portfolio_state_and_diff(trade_detail, prev_state):
        assert isinstance(trade_detail, TradeInfo)
        assert isinstance(prev_state, dict)

        curr_state = deepcopy(prev_state)
        disposed = {
                "stocks": [],
                "currency": [],
                "cryptocurrency": "NOT_IMPLEMENTED",
            }
        acquired = {
                "stocks": [],
                "currency": [],
                "cryptocurrency": "NOT_IMPLEMENTED",
            }

        if trade_detail.trade_type == "buy":
            d = {
                    "acquired_on": trade_detail.trade_date,
                    "unit_quantity": trade_detail.unit_quantity,
                    "unit_price": trade_detail.unit_price,
                    "unit_currency": trade_detail.unit_currency,
                    "fees_per_unit": (trade_detail.fees / trade_detail.unit_quantity),
                    "fees_currency": trade_detail.fees_currency,
                }
            curr_state["stocks"][trade_detail.account][trade_detail.ric_symbol].append(d)
            
            # Update acquisitions
            d = deepcopy(d)
            d["account"] = trade_detail.account
            d["ric_symbol"] = trade_detail.ric_symbol
            acquired["stocks"].append(d)

            # Update dispositions
            d2 = {
                    "currency": d["unit_currency"],
                    "amount": d["unit_price"] * d["unit_quantity"],
                }
            d3 = {
                    "currency": d["fees_currency"],
                    "amount": d["fees_per_unit"] * d["unit_quantity"],
                }
            if d2["currency"] == d3["currency"]:
                d2["amount"] += d3["amount"]
            else:
                disposed["currency"].append(d3)
            disposed["currency"].append(d2)
        elif trade_detail.trade_type == "sell":
            # TODO: Implement custom sell strategy.
            #       This current one is a simple LIFO strategy, ignoring any CGT discount rules.

            holdings_list = curr_state["stocks"][trade_detail.account][trade_detail.ric_symbol]

            yet_to_dispose = trade_detail.unit_quantity
            assert yet_to_dispose > 0

            fee_per_unit_of_disposal = trade_detail.fees / yet_to_dispose

            while yet_to_dispose > 0:
                holding = holdings_list[-1]
                if holding["unit_quantity"] > yet_to_dispose:
                    holding["unit_quantity"] -= yet_to_dispose

                    # Add new disposal
                    disposal = {
                            "ric_symbol": trade_detail.ric_symbol,
                            "account": trade_detail.account,
                            "acquired_on": holding["acquired_on"],
                            "disposed_on": trade_detail.trade_date,
                            "unit_quantity": yet_to_dispose, # No need to copy because we're reassigning later anyway
                            "unit_price_of_acquisition": holding["unit_price"],
                            "unit_price_of_disposal": trade_detail.unit_price,
                            "unit_currency": holding["unit_currency"],
                            "fees_per_unit_of_acquisition": holding["fees_per_unit"],
                            "fees_per_unit_of_disposal": fee_per_unit_of_disposal,
                            "fees_currency": holding["fees_currency"],
                        }
                    disposed["stocks"].append(disposal)

                    # Update yet_to_dispose
                    yet_to_dispose = Fraction(0)

                elif holding["unit_quantity"] <= yet_to_dispose:

                    # Add new disposal
                    disposal = {
                            "ric_symbol": trade_detail.ric_symbol,
                            "account": trade_detail.account,
                            "acquired_on": holding["acquired_on"],
                            "disposed_on": trade_detail.trade_date,
                            "unit_quantity": holding["unit_quantity"],
                            "unit_price_of_acquisition": holding["unit_price"],
                            "unit_price_of_disposal": trade_detail.unit_price,
                            "unit_currency": holding["unit_currency"],
                            "fees_per_unit_of_acquisition": holding["fees_per_unit"],
                            "fees_per_unit_of_disposal": fee_per_unit_of_disposal,
                            "fees_currency": holding["fees_currency"],
                        }
                    disposed["stocks"].append(disposal)

                    # Update yet_to_dispose and holdings_list
                    yet_to_dispose -= holding["unit_quantity"]
                    del holdings_list[-1]

            if yet_to_dispose > 0:
                raise ValueError("Attempted to sell more units than owned.")
            elif yet_to_dispose < 0:
                raise RuntimeError("Unexpected negative value for 'yet_to_dispose'.")
        else:
            raise RuntimeError("Unexpected trade type.")

        return (curr_state, disposed, acquired)

    @staticmethod
    def _generate_portfolio_state_stats(curr_state):
        assert isinstance(curr_state, dict)

        def shs_process_parcels(parcel_list):
            total_units = Fraction(0)
            total_price_before_fees = Fraction(0)
            total_fees = Fraction(0)
            unit_currency = parcel_list[0]["unit_currency"] if (len(parcel_list) > 0) else "[unknown]"
            fees_currency = parcel_list[0]["fees_currency"] if (len(parcel_list) > 0) else "[unknown]"
            for d in parcel_list:
                if (d["unit_currency"] != unit_currency) or (d["fees_currency"] != fees_currency):
                    raise NotImplementedError("This codebase is not yet equipped to handle currency changes of a holding.")
                total_units += d["unit_quantity"]
                total_price_before_fees += (d["unit_price"] * d["unit_quantity"])
                total_fees += (d["fees_per_unit"] * d["unit_quantity"])
            return {
                    "total_units": total_units,
                    "total_price_before_fees": total_price_before_fees,
                    "total_price_before_fees_currency": unit_currency,
                    "total_fees": total_fees,
                    "total_fees_currency": fees_currency,
                }
        def shs_process_symbols(d):
            return {k: shs_process_parcels(v) for (k, v) in d.items()}
        stock_holdings_stats = {k: shs_process_symbols(v) for (k, v) in curr_state["stocks"].items()}

        return {
                "stocks": stock_holdings_stats
            }

    @staticmethod
    def _generate_trade_stats(disposals):
        assert isinstance(disposals, dict)

        # This function's logic will definitely need to be changed when I deal with other asset classes.

        stock_disposals = disposals["stocks"]
        assert isinstance(stock_disposals, list)

        total_cg_without_fees = Fraction(0)
        total_cg_without_fees_currency = stock_disposals[0]["unit_currency"] if (len(stock_disposals) > 0) else "[unknown]"
        total_fees = Fraction(0)
        total_fees_currency = stock_disposals[0]["fees_currency"] if (len(stock_disposals) > 0) else "[unknown]"
        for d in disposals["stocks"]:
            qty = d["unit_quantity"]
            total_cg_without_fees += (d["unit_price_of_disposal"] - d["unit_price_of_acquisition"]) * qty
            total_fees += (d["fees_per_unit_of_disposal"] + d["fees_per_unit_of_acquisition"]) * qty
        return {
                "total_realized_capital_gain_without_fees": total_cg_without_fees,
                "total_realized_capital_gain_without_fees_currency": total_cg_without_fees_currency,
                "total_fees": total_fees,
                "total_fees_currency": total_fees_currency,
            }

    def _dump_portfolio_history_debugging_file(self, obj):
        filepath = os.path.join(
                self._debugging_path,
                self._PORTFOLIO_HISTORY_DEBUGGING__FILEPATH,
            )
        logging.debug(f"Writing portfolio debugging history file to '{filepath}'.")
        fwrite_json(filepath, data=create_json_writable_debugging_structure(obj))
        return

