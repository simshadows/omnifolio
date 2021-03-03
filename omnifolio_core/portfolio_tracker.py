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
from collections import namedtuple, defaultdict
from fractions import Fraction
from itertools import chain

import pandas as pd
import numpy as np

from .config import get_config
from .utils import (
        re_decimal,
        fwrite_json,
        str_is_nonempty_and_compact,
        fraction_to_decimal,
        create_json_writable_debugging_structure
    )

logger = logging.getLogger(__name__)


_NUMPY_INT = np.longlong
_NUMPY_FLOAT = np.double

_INT_MAX = np.iinfo(_NUMPY_INT).max
assert np.iinfo(_NUMPY_INT).bits >= 64

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



class PortfolioTracker:

    _TRADES__FILEPATH = "trades.csv"

    _PORTFOLIO_HISTORY_DEBUGGING__FILEPATH = "portfolio_history.json"

    def __init__(self, config=get_config()):
        self._user_data_path = config["user_data_path"]

        # TODO: Set the directory name somewhere else.
        self._debugging_path = os.path.join(config["generated_data_path"], "debugging")
        return

    def get_trades(self):
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

            fees = _only_allow_decimal_rep(row[8], "Fees")
            fees_currency = _only_allow_nonempty_str(row[9], "Fees currency")

            unit_quantity_denominator = _only_allow_decimal_rep(row[10], "Unit quantity denominator")
            unit_price_denominator = _only_allow_decimal_rep(row[11], "Unit price denominator")
            fees_denominator = _only_allow_decimal_rep(row[12], "Fees denominator")

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

        return ret
        
    ## No longer used
    #def get_trades_as_df(self, trades_data=None):
    #    """
    #    Converts the trades data from the .get_trades() method into a dataframe.

    #    Do note that all numbers are native Python Fraction objects rather than numpy types.

    #    If called with trades_data == None, then this method will first get trades data from
    #    the .get_trades() method before processing it into a dataframe.
    #    """
    #    if trades_data is None:
    #        trades_data = self.get_trades()
    #    assert isinstance(trades_data, list)
    #    assert all(isinstance(x, TradeInfo) for x in trades_data)
    #    return pd.DataFrame(trades_data).astype({"trade_date": np.datetime64})

    ## No longer used
    #def get_portfolio_holdings_history(self, trades_df=None):
    #    """
    #    Calculates a full portfolio holdings history.

    #    trades_df is whatever is returned by .get_trades_as_df().

    #    Alternatively, if called with trades_df == None, then this method will first get trades
    #    data from the .get_trades_as_df() method before processing it.
    #    """
    #    if trades_df is None:
    #        trades_df = self.get_trades_as_df()
    #    assert isinstance(trades_df, pd.DataFrame)
    #    df = copy.deepcopy(trades_df)
    #    pandas_add_column_level_above(df, "trades", inplace=True)

    #    symbols_present = set(df["trades"]["ric_symbol"])
    #    if "trades" in symbols_present:
    #        raise ValueError("'trades' is a reserved word that cannot be used as a symbol here.")

    #    for symbol in symbols_present:
    #        assert str_is_nonempty_and_compact(symbol)
    #        df.insert(len(df.columns), (symbol, "quantity"), Fraction(0))
    #        df.insert(len(df.columns), (symbol, "total_purchase_price"), Fraction(0))

    #    return df

    def get_portfolio_history(self, trades_data=None):
        """
        Calculates an exact and detailed portfolio history.

        If called with trades_data == None, then this method will first get trades data from
        the .get_trades() method before processing it into a dataframe.
        """
        if trades_data is None:
            trades_data = self.get_trades()
        assert isinstance(trades_data, list)

        history = []
        for trade in trades_data:
            assert isinstance(trade, TradeInfo)
            trade_detail = copy.deepcopy(trade)
            entry = {
                    # Details of the corresponding trade
                    "trade_detail": trade_detail,

                    # State of the portfolio after the corresponding trade
                    "portfolio_state": self._generate_portfolio_state(trade_detail),

                    # Statistics derived from the portfolio state
                    "portfolio_state_statistics": "not_yet_implemented",
                }
            history.append(entry)

        self._dump_portfolio_history_debugging_file(history)
        return history

    ######################################################################################
    ######################################################################################

    @staticmethod
    def _generate_portfolio_state(trade_detail):
        assert isinstance(trade_detail, TradeInfo)
        stock_holdings = {}
        if trade_detail.trade_type == "buy":
            pass
        elif trade_detail.trade_type == "sell":
            pass
        else:
            raise RuntimeError("Unexpected trade type.")
        return {
                "stock_holdings": stock_holdings,
            }

    def _dump_portfolio_history_debugging_file(self, obj):
        filepath = os.path.join(
                self._debugging_path,
                self._PORTFOLIO_HISTORY_DEBUGGING__FILEPATH,
            )
        logging.debug(f"Writing portfolio debugging history file to '{filepath}'.")
        fwrite_json(filepath, data=create_json_writable_debugging_structure(obj))
        return
