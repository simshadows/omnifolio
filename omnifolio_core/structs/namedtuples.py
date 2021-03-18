# -*- coding: ascii -*-

"""
Filename: namedtuples.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

This file contains commonly used namedtuple class definitions.
"""

from collections import namedtuple

_CurrencyPairBaseClass = namedtuple(
    "_CurrencyPairBaseClass",
    [
        "base",
        "quote",
    ],
)

class CurrencyPair(_CurrencyPairBaseClass):
    __slots__ = []

    def __str__(self):
        assert isinstance(self.base, str) and (len(self.base.strip()) != 0)
        assert isinstance(self.quote, str) and (len(self.quote.strip()) != 0)
        return f"{self.base}/{self.quote}"

TradeInfo = namedtuple(
    "TradeInfo",
    [
        "comment",
        "trade_date",
        "account",

        "ric_symbol",
        "trade_type",

        "unit_quantity",
        "unit_price",
        "total_fees",
    ],
)

TradeDiff = namedtuple(
    "TradeDiff",
    [
        "acquired",
        "disposed",
    ],
)

