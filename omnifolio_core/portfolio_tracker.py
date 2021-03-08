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

from .user_data_providers.trades_data_provider import get_trades
from .portfolio_holdings_accwise_avg import PortfolioHoldingsAccwiseAvg

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

        portfolio_state = deepcopy(kwargs.get("initial_portfolio_state", PortfolioHoldingsAccwiseAvg()))
        assert isinstance(portfolio_state, PortfolioHoldingsAccwiseAvg)

        for individual_trade in trade_history_iterable:
            assert isinstance(individual_trade, TradeInfo)

            trade_detail = deepcopy(individual_trade)
            portfolio_diff = portfolio_state.trade(trade_detail)

            entry = {
                    # Data from these fields are not cumulative.
                    # Thus, the caller must save this data themselves.
                    "trade_detail": trade_detail,
                    "disposed_during_this_trade": portfolio_diff.disposed,
                    "acquired_during_this_trade": portfolio_diff.acquired,

                    # Data from this field is cumulative.
                    "portfolio_state": deepcopy(portfolio_state),

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

    def _dump_portfolio_history_debugging_file(self, obj):
        filepath = os.path.join(
                self._debugging_path,
                self._PORTFOLIO_HISTORY_DEBUGGING__FILEPATH,
            )
        logging.debug(f"Writing portfolio debugging history file to '{filepath}'.")
        obj = [copy(x) for x in obj] # Copy only deep enough for our purposes
        for entry in obj:
            entry["portfolio_state"] = entry["portfolio_state"].get_holdings()
        fwrite_json(filepath, data=create_json_writable_debugging_structure(obj))
        return

