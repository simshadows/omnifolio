# -*- coding: ascii -*-

"""
Filename: portfolio_holdings.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

PortfolioHoldings encapsulates the moment-to-moment state of a portfolio's holdings.

To reiterate: PortfolioHoldings does NOT deal with the past. It only tracks what is currently
held, and doesn't hold any information about the past.
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
        Currency,
    )
from .utils import (
        create_json_writable_debugging_structure
    )

logger = logging.getLogger(__name__)

TradeDiff = namedtuple(
    "TradeDiff",
    [
        "acquired",
        "disposed",
    ],
)

class PortfolioHoldings:

    __slots__ = [
            "_stocks",
        ]

    def __init__(self):
        self._stocks = defaultdict(lambda : defaultdict(list)) # {account : {symbol : [trades]}}
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

            d = {
                    "acquired_on": trade_detail.trade_date,
                    "unit_quantity": trade_detail.unit_quantity,
                    "unit_price": trade_detail.unit_price,
                    "fees_per_unit": trade_detail.total_fees / trade_detail.unit_quantity,
                }
            self._stocks[trade_detail.account][trade_detail.ric_symbol].append(d)
            
            # Update acquisitions
            d = deepcopy(d)
            d["account"] = trade_detail.account
            d["ric_symbol"] = trade_detail.ric_symbol
            acquired["stocks"].append(d)

            # Update disposals
            a = d["unit_price"] * d["unit_quantity"]
            b = d["fees_per_unit"] * d["unit_quantity"]
            if a.symbol == b.symbol:
                a += b
            else:
                disposed["currency"].append(b)
            disposed["currency"].append(a)

        elif trade_detail.trade_type == "sell":

            # TODO: Implement custom sell strategy.
            #       This current one is a simple LIFO strategy, ignoring any CGT discount rules.

            holdings_list = self._stocks[trade_detail.account][trade_detail.ric_symbol]

            yet_to_dispose = trade_detail.unit_quantity
            assert yet_to_dispose > 0

            fee_per_unit_of_disposal = trade_detail.total_fees / yet_to_dispose

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
                            "acquisition_unit_price": holding["unit_price"],
                            "disposal_unit_price": trade_detail.unit_price,
                            "acquisition_fees_per_unit": holding["fees_per_unit"],
                            "disposal_fees_per_unit": fee_per_unit_of_disposal,
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
                            "acquisition_unit_price": holding["unit_price"],
                            "disposal_unit_price": trade_detail.unit_price,
                            "acquisition_fees_per_unit": holding["fees_per_unit"],
                            "disposal_fees_per_unit": fee_per_unit_of_disposal,
                        }
                    disposed["stocks"].append(disposal)

                    # Update yet_to_dispose and holdings_list
                    yet_to_dispose -= holding["unit_quantity"]
                    del holdings_list[-1]

            # Update cash acquisition and disposal
            a = trade_detail.unit_price * trade_detail.unit_quantity
            b = trade_detail.total_fees
            assert (a >= 0) and (b >= 0)
            if a.symbol == b.symbol:
                c = a - b
                if c > 0:
                    acquired["currency"].append(c)
                elif c < 0:
                    disposed["currency"].append(c)
            else:
                if a > 0:
                    acquired["currency"].append(a)
                if b > 0:
                    disposed["currency"].append(b)

            if yet_to_dispose > 0:
                raise ValueError("Attempted to sell more units than owned.")
            elif yet_to_dispose < 0:
                raise RuntimeError("Unexpected negative value for 'yet_to_dispose'.")
        else:
            raise RuntimeError("Unexpected trade type.")

        return TradeDiff(acquired=acquired, disposed=disposed)

    def stats(self):
        """
        Generate stats derived entirely from the current holdings (with nothing about the past).
        """
        def shs_process_parcels(parcel_list):
            total_units = Fraction(0)
            total_price_before_fees = Fraction(0)
            total_fees = Fraction(0)
            unit_currency = parcel_list[0]["unit_price"].symbol if (len(parcel_list) > 0) else "???"
            fees_currency = parcel_list[0]["fees_per_unit"].symbol if (len(parcel_list) > 0) else "???"
            for d in parcel_list:
                if (d["unit_price"].symbol != unit_currency) or (d["fees_per_unit"].symbol != fees_currency):
                    raise NotImplementedError("This codebase is not yet equipped to handle currency changes of a holding.")
                total_units += d["unit_quantity"]
                total_price_before_fees += (d["unit_price"].value * d["unit_quantity"])
                total_fees += (d["fees_per_unit"].value * d["unit_quantity"])
            return {
                    "total_units": total_units,
                    "total_price_before_fees": Currency(unit_currency, total_price_before_fees),
                    "total_fees": Currency(fees_currency, total_fees),
                }
        def shs_process_symbols(d):
            return {k: shs_process_parcels(v) for (k, v) in d.items()}
        stock_holdings_stats = {k: shs_process_symbols(v) for (k, v) in self._stocks.items()}

        return {
                "stocks": stock_holdings_stats
            }

    def get_holdings(self):
        return {
                "stocks": deepcopy(self._stocks),
            }

