# -*- coding: ascii -*-

"""
Filename: portfolio_tracker.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)
"""

import os
import copy
import re
import logging
import csv
import datetime
from collections import namedtuple
from fractions import Fraction
from itertools import chain

from .config import get_config

#from .utils import fwrite_json, fread_json

logger = logging.getLogger(__name__)


_re_decimal = re.compile(r"^\d*[.,]?\d*$")


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
        "unit_currency",

        "fees",
        "fees_currency",
    ],
)


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
    ],
    [
        "1 - First trade!",
        "2019-09-06",
        "CommSec ...123",

        "IVV.AX", "buy",

        "32", "439.21", "AUD",

        "29.95", "AUD",
    ],
    [
        "2",
        "2020-01-14",
        "CommSec ...123",

        "IVV.AX", "buy",

        "20", "478.03", "AUD",

        "19.95", "AUD",
    ],
    [
        "3",
        "2020-03-02",
        "CommSec ...123",

        "IVV.AX", "sell",

        "22", "449.35", "AUD",

        "19.95", "AUD",
    ],
    [
        "4",
        "2020-08-25",
        "CommSec ...123",

        "NDQ.AX", "buy",

        "120", "26.90", "AUD",

        "19.95", "AUD",
    ],
    [
        "5 - We'll test the stock split and fractional ownership. Split occurs before 2020-08-31 opening bell.",
        "2020-08-27",
        "Stake ...456",

        "TSLA", "buy",

        "4.5", "443.12", "USD",

        "0", "USD",
    ],
]



class PortfolioTracker:

    _TRADES__FILEPATH = "trades.csv"

    def __init__(self, config=get_config()):
        self._user_data_path = config["user_data_path"]
        return

    def _read_trades_file(self):
        filepath = os.path.join(
                self._user_data_path,
                self._TRADES__FILEPATH,
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
        if not all((len(x) == 10) for x in data):
            raise ValueError("CSV file '{filepath}' must be comprised of only 10-column rows.")

        # We check the first row, which should just be labels, then we strip it away.

        if data[0] != _SAMPLE_TRADES_FILE_DATA[0]:
            raise ValueError("CSV file '{filepath}' must contain the column labels row.")
        del data[0]

        ret = []
        for row in data:
            # TODO: Make this loop more succinct. Too much repetitive code.

            comment = row[0].strip()
            # comment is allowed to be whatever the user wants it to be.

            trade_date = datetime.date.fromisoformat(row[1].strip())

            account = row[2].strip()
            if len(account) == 0:
                account = _DEFAULT_ACCOUNT_VALUE
            # account is *generally* allowed to be whatever the user wants it to be, except
            # it can't be empty.

            ric_symbol = row[3].strip()
            if len(ric_symbol) == 0:
                raise ValueError("RIC symbols must be non-empty.")

            trade_type = row[4].strip()
            if trade_type not in _ALLOWED_TRADE_TYPE_STRINGS:
                raise ValueError("Trade types must be either 'buy' or 'sell'.")

            unit_quantity = row[5].strip()
            if not _re_decimal.fullmatch(unit_quantity):
                raise ValueError("Unit quantities must be in a decimal representation.")
            unit_quantity = Fraction(unit_quantity)

            unit_price = row[6].strip()
            if not _re_decimal.fullmatch(unit_price):
                raise ValueError("Unit prices must be in a decimal representation.")
            unit_price = Fraction(unit_price)

            unit_currency = row[7].strip()
            if len(unit_currency) == 0:
                raise ValueError("Unit currency must be non-empty.")

            fees = row[8].strip()
            if not _re_decimal.fullmatch(fees):
                raise ValueError("Fees must be in a decimal representation.")
            fees = Fraction(fees)

            fees_currency = row[9].strip()
            if len(fees_currency) == 0:
                raise ValueError("Fee currency must be non-empty.")

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

        return ret
        
    def get_trades(self):
        return self._read_trades_file()

