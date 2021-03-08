# -*- coding: ascii -*-

"""
Filename: namedtuples.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

This file contains commonly used namedtuple class definitions.
"""

from collections import namedtuple

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

