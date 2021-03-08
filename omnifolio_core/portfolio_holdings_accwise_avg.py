# -*- coding: ascii -*-

"""
Filename: portfolio_holdings_accwise_avg.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

PortfolioHoldingsAvgCost encapsulates the moment-to-moment state of a portfolio's holdings.

In particular, it tracks holdings using the average cost method for each account.
"""

import os
import logging
import datetime
from copy import deepcopy
from collections import defaultdict, namedtuple
from fractions import Fraction

from .config import get_config
from .structs import (
        TradeInfo,
        TradeDiff,
        Currency,
    )
from .utils import (
        create_json_writable_debugging_structure
    )

logger = logging.getLogger(__name__)

class PortfolioHoldingsAccwiseAvg:

    __slots__ = [
            "_stocks",
        ]

    def __init__(self):
        self._stocks = defaultdict(dict) # {account : {symbol : {}}}
        return

    def trade(self, trade_detail):
        """
        Modifies the state of the PortfolioHoldings object according to the trade as detailed
        by trade_detail.

        Returns a TradeDiff namedtuple showing what has changed in the portfolio as a result
        of the trade.
        """
        assert isinstance(trade_detail, TradeInfo)
        
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
            
            if trade_detail.ric_symbol in self._stocks[trade_detail.account]:
                d = self._stocks[trade_detail.account][trade_detail.ric_symbol]
                d["total_value"] += trade_detail.unit_price * trade_detail.unit_quantity
                d["total_fees"] += trade_detail.total_fees
                d["unit_quantity"] += trade_detail.unit_quantity
            else:
                self._stocks[trade_detail.account][trade_detail.ric_symbol] = {
                        "total_value": trade_detail.unit_price * trade_detail.unit_quantity,
                        "total_fees": trade_detail.total_fees,
                        "unit_quantity": trade_detail.unit_quantity,
                    }

            acquired["stocks"].append({
                    "account": trade_detail.account,
                    "ric_symbol": trade_detail.ric_symbol,
                    "acquisition_date": trade_detail.trade_date,

                    "unit_price": trade_detail.unit_price,
                    "fees_per_unit": trade_detail.total_fees / trade_detail.unit_quantity,
                    "unit_quantity": trade_detail.unit_quantity,
                })

            assert trade_detail.unit_price > 0
            assert trade_detail.total_fees >= 0
            disposed["currency"].append(trade_detail.unit_price * trade_detail.unit_quantity)
            if trade_detail.total_fees > 0:
                disposed["currency"].append(trade_detail.total_fees)

        elif trade_detail.trade_type == "sell":

            d = self._stocks[trade_detail.account][trade_detail.ric_symbol]

            remaining_units = d["unit_quantity"] - trade_detail.unit_quantity
            if remaining_units < 0:
                existing_units = d["unit_quantity"]
                raise ValueError(f"Attempted to sell {trade_detail.unit_quantity} units, but "
                                 f"we only have {existing_units} units. Naked shorts are not "
                                  "implemented.")

            acquisition_unit_price = d["total_value"] / d["unit_quantity"]
            acquisition_fees_per_unit = d["total_fees"] / d["unit_quantity"]

            disposed["stocks"].append({
                    "account": trade_detail.account,
                    "ric_symbol": trade_detail.ric_symbol,

                    "acquisition_date": None, # Not applicable for this accounting method.
                    "acquisition_unit_price": acquisition_unit_price,
                    "acquisition_fees_per_unit": acquisition_fees_per_unit,
                    
                    "disposal_date": trade_detail.trade_date,
                    "disposal_unit_price": trade_detail.unit_price,
                    "disposal_fees_per_unit": trade_detail.total_fees / trade_detail.unit_quantity,

                    "unit_quantity": trade_detail.unit_quantity,
                })

            assert trade_detail.unit_price > 0
            assert trade_detail.total_fees >= 0
            acquired["currency"].append(trade_detail.unit_price * trade_detail.unit_quantity)
            if trade_detail.total_fees > 0:
                disposed["currency"].append(trade_detail.total_fees)

            # We update the portfolio state accordingly.
            if remaining_units > 0:
                d["total_value"] = acquisition_unit_price * remaining_units
                d["total_fees"] = acquisition_fees_per_unit * remaining_units
                d["unit_quantity"] = remaining_units
            else:
                assert remaining_units == 0
                del (self._stocks[trade_detail.account])[trade_detail.ric_symbol]
                if len(self._stocks[trade_detail.account]) == 0:
                    del self._stocks[trade_detail.account]

        return TradeDiff(acquired=acquired, disposed=disposed)

    def get_holdings(self):
        return {
                "stocks": deepcopy(self._stocks),
            }

    def _prune_stock_symbol(self, account, symbol):
        if len(self._stocks[account][symbol]) == 0:
            del (self._stocks[account])[symbol]
            if len(self._stocks[account]) == 0:
                del self._stocks[account]
        return

