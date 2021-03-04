# -*- coding: ascii -*-

"""
Filename: trades_data_provider.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Reads the user's trades data file, and possibly creates a new one if one is not present.
"""

import os
import logging
import csv
import datetime
from fractions import Fraction
from itertools import chain

from ..structs import TradeInfo
from ..utils import re_decimal

logger = logging.getLogger(__name__)

_TRADES_FILEPATH = "trades.csv"

_DEFAULT_ACCOUNT_VALUE = "[default_account]"
_ALLOWED_TRADE_TYPE_STRINGS = {"buy", "sell"}

_SAMPLE_TRADES_FILE_DATA = [
    [
        "comment",
        "trade_date",
        "account",

        "ric_symbol",
        "trade_type",

        "unit_quantity",
        "unit_price",
        "unit_currency",

        "fees",
        "fees_currency",

        "unit_quantity_denominator", # Divides 'unit_quantity' by this number.
        "unit_price_denominator", # Divides 'unit_price' by this number.
        "fees_denominator", # Divides 'fees' by this number.
    ],
    [
        "1 - First trade!",
        "2019-09-06",
        "CommSec ...123",

        "IVV.AX", "buy",

        "32", "439.21", "AUD",

        "29.95", "AUD",

        "1", "1", "1",
    ],
    [
        "2",
        "2020-01-14",
        "CommSec ...123",

        "IVV.AX", "buy",

        "20", "478.03", "AUD",

        "19.95", "AUD",

        "1", "1", "1",
    ],
    [
        "3",
        "2020-03-02",
        "CommSec ...123",

        "IVV.AX", "sell",

        "22", "449.35", "AUD",

        "19.95", "AUD",

        "1", "1", "1",
    ],
    [
        "4",
        "2020-08-25",
        "CommSec ...123",

        "NDQ.AX", "buy",

        "120", "26.90", "AUD",

        "19.95", "AUD",

        "1", "1", "1",
    ],
    [
        "5 - We'll test the stock split and fractional ownership. Split occurs before 2020-08-31 opening bell.",
        "2020-08-27",
        "Stake ...456",

        "TSLA", "buy",

        "4.5", "443.12", "USD",

        "0", "USD",

        "1", "1", "1",
    ],
]

def _only_allow_nonempty_str(s, name):
    assert isinstance(s, str)
    assert isinstance(name, str)
    s = s.strip()
    if len(s) == 0:
        raise ValueError(f"{name} must be non-empty.")
    return s

def _only_allow_decimal_rep(s, name):
    assert isinstance(s, str)
    assert isinstance(name, str)
    s = s.strip()
    if not re_decimal.fullmatch(s):
        raise ValueError(f"{name} must be in a decimal representation.")
    return Fraction(s)


def get_trades(user_data_path):
    """
    Reads the user's trades data file and returns the data.
    """
    filepath = os.path.join(
            user_data_path,
            _TRADES_FILEPATH,
        )

    if not os.path.isfile(filepath):
        logger.debug(f"No trades file at path {filepath}.")
        logger.debug(f"Creating a new trades file.")
        with open(filepath, "w") as f:
            csv.writer(f, dialect="excel").writerows(_SAMPLE_TRADES_FILE_DATA)
        logger.debug(f"New trades file saved.")

    data = None

    logger.debug(f"Reading trades file at {filepath}.")
    with open(filepath, "r") as f:
        data = list(list(x) for x in csv.reader(f))

    assert isinstance(data, list)
    assert all(isinstance(x, list) for x in data)
    assert all(isinstance(x, str) for x in chain(*data))
    
    if len(data) == 0:
        raise ValueError("CSV file '{filepath}' must be non-empty.")
    if not all((len(x) == 13) for x in data):
        raise ValueError("CSV file '{filepath}' must be comprised of only 13-column rows.")

    # We check the first row, which should just be labels, then we strip it away.

    if data[0] != _SAMPLE_TRADES_FILE_DATA[0]:
        raise ValueError("CSV file '{filepath}' must contain the column labels row.")
    del data[0]

    ret = []
    for row in data:
        comment = row[0].strip()
        # comment is allowed to be whatever the user wants it to be.

        trade_date = datetime.date.fromisoformat(row[1].strip())

        account = row[2].strip()
        if len(account) == 0:
            account = _DEFAULT_ACCOUNT_VALUE
        # account is *generally* allowed to be whatever the user wants it to be, except
        # it can't be empty.

        ric_symbol = _only_allow_nonempty_str(row[3], "RIC symbol")
        trade_type = row[4].strip()

        if trade_type not in _ALLOWED_TRADE_TYPE_STRINGS:
            raise ValueError("Trade types must be either 'buy' or 'sell'.")

        unit_quantity = _only_allow_decimal_rep(row[5], "Unit quantity")
        unit_price = _only_allow_decimal_rep(row[6], "Unit price")
        unit_currency = _only_allow_nonempty_str(row[7], "Unit currency")

        if unit_quantity <= 0:
            raise ValueError("Unit quantity must be greater than zero.")
        if unit_price < 0:
            raise ValueError("Unit price must not be negative.")

        fees = _only_allow_decimal_rep(row[8], "Fees")
        fees_currency = _only_allow_nonempty_str(row[9], "Fees currency")

        if fees < 0:
            raise ValueError("Fees must not be negative.")

        unit_quantity_denominator = _only_allow_decimal_rep(row[10], "Unit quantity denominator")
        unit_price_denominator = _only_allow_decimal_rep(row[11], "Unit price denominator")
        fees_denominator = _only_allow_decimal_rep(row[12], "Fees denominator")

        if unit_quantity_denominator <= 0:
            raise ValueError("Unit quantity denominator must be greater than zero.")
        if unit_price_denominator <= 0:
            raise ValueError("Unit price denominator must be greater than zero.")
        if fees_denominator <= 0:
            raise ValueError("Fees denominator must be greater than zero.")

        unit_quantity /= unit_quantity_denominator
        unit_price /= unit_price_denominator
        fees /= fees_denominator
        # Sanity checks
        assert isinstance(unit_quantity, Fraction)
        assert isinstance(unit_price, Fraction)
        assert isinstance(fees, Fraction)

        t = TradeInfo(
                comment=comment,
                trade_date=trade_date,
                account=account,

                ric_symbol=ric_symbol,
                trade_type=trade_type,

                unit_quantity=unit_quantity,
                unit_price=unit_price,
                unit_currency=unit_currency,

                fees=fees,
                fees_currency=fees_currency,
            )
        ret.append(t)

    if not sorted(ret, key=(lambda x : x.trade_date)):
        raise ValueError("Trades must be in chronological order.")
    return ret

