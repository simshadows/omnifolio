# -*- coding: ascii -*-

"""
Filename: stock_dividends.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Reads manually-inputted stock dividend data.

The intention of this data is to override any downloaded stock dividend data with it. This is
especially useful to correct any anomalies (such as adding missing dividend data).
"""

import os
import logging
import datetime
import csv
from fractions import Fraction
from decimal import Decimal
from itertools import chain

from ...utils import (
        re_decimal,
        mkdir_recursive_for_filepath
    )
from ...structs import Currency

_logger = logging.getLogger(__name__)

_STOCK_DIVIDENDS_FILEPATH = "stock_dividends.csv"

_SAMPLE_STOCK_DIVIDENDS_FILE_DOCROWS = [
    [
        "This file is used to correct any anomalies in stock dividend data.",
        "",
        "",
        "",
        "",
    ],
    [
        "All dividends entered here will be used to override downloaded data.",
        "",
        "",
        "",
        "",
    ],
    [
        "Do note that the order of dates and symbols in this file doesn't matter. Feel free to organize the data however you want.",
        "",
        "",
        "",
        "",
    ],
    [
        "You can use the comment column to add any note you want.",
        "",
        "",
        "",
        "",
    ],
    [
        "",
        "",
        "",
        "",
        "",
    ],
]

_SAMPLE_STOCK_DIVIDENDS_FILE_LABELS = [
    [
        "ric_symbol",
        "date",
        "currency",
        "ex_dividend_amount_per_unit",
        "comment",
    ],
]

_SAMPLE_STOCK_DIVIDENDS_FILE_DATA = [

    # TODO: Consider adding more "common" stocks and funds to the sample data, particularly US, Canada, and Europe.
    #       Or maybe I shouldn't add too many things...

    # TODO: Also, consider loading the sample data on an as-needed basis.

    ["VDHG.AX", "2019-01-02", "AUD", "0.344362", ""],
    ["VDHG.AX", "2019-04-01", "AUD", "0.448912", ""],
    ["VDHG.AX", "2019-07-01", "AUD", "1.048382", ""],
    ["VDHG.AX", "2019-10-01", "AUD", "0.380916", ""],
    ["VDHG.AX", "2020-01-02", "AUD", "0.332774", ""],
    ["VDHG.AX", "2020-04-01", "AUD", "0.449162", ""],
    ["VDHG.AX", "2020-07-01", "AUD", "1.259746", ""],
    ["VDHG.AX", "2020-10-01", "AUD", "0.898915", ""],
    ["VDHG.AX", "2021-01-04", "AUD", "0.993767", ""],
    ["VDHG.AX", "2021-04-01", "AUD", "2.009896", ""],

    ["VGS.AX", "2019-01-02", "AUD", "0.390286", ""],
    ["VGS.AX", "2019-04-01", "AUD", "0.484093", ""],
    ["VGS.AX", "2019-07-01", "AUD", "0.729679", ""],
    ["VGS.AX", "2019-10-01", "AUD", "0.369838", ""],
    ["VGS.AX", "2020-01-02", "AUD", "0.450855", ""],
    ["VGS.AX", "2020-04-01", "AUD", "0.454245", ""],
    ["VGS.AX", "2020-07-01", "AUD", "0.635546", ""],
    ["VGS.AX", "2020-10-01", "AUD", "0.345002", ""],
    ["VGS.AX", "2021-01-04", "AUD", "0.403359", ""],
    ["VGS.AX", "2021-04-01", "AUD", "0.315584", ""],

    ["VAS.AX", "2019-01-02", "AUD", "0.710610", ""],
    ["VAS.AX", "2019-04-01", "AUD", "0.915933", ""],
    ["VAS.AX", "2019-07-01", "AUD", "0.821362", ""],
    ["VAS.AX", "2019-10-01", "AUD", "1.070957", ""],
    ["VAS.AX", "2020-01-02", "AUD", "0.721369", ""],
    ["VAS.AX", "2020-04-01", "AUD", "0.672656", ""],
    ["VAS.AX", "2020-07-01", "AUD", "0.206023", ""],
    ["VAS.AX", "2020-10-01", "AUD", "0.568418", ""],
    ["VAS.AX", "2021-01-04", "AUD", "0.434171", ""],
    ["VAS.AX", "2021-04-01", "AUD", "0.769961", ""],

    ["IVV.AX", "2019-01-04", "AUD", "2.11594445", ""],
    ["IVV.AX", "2019-03-28", "AUD", "1.35075482", ""],
    ["IVV.AX", "2019-07-01", "AUD", "2.27223769", ""],
    ["IVV.AX", "2019-10-02", "AUD", "1.81668685", ""],
    ["IVV.AX", "2019-12-24", "AUD", "2.50382877", ""],
    ["IVV.AX", "2020-04-01", "AUD", "2.19782185", ""],
    ["IVV.AX", "2020-07-01", "AUD", "1.67564897", ""],
    ["IVV.AX", "2020-10-01", "AUD", "1.73919789", ""],
    ["IVV.AX", "2020-12-22", "AUD", "1.79375291", ""],
    ["IVV.AX", "2021-04-01", "AUD", "1.42721572", ""],

    ["IVV", "2018-12-28", "USD", "0.322286", ""],
    ["IVV", "2019-03-20", "USD", "1.129603", ""],
    ["IVV", "2019-06-17", "USD", "1.777745", ""],
    ["IVV", "2019-09-24", "USD", "1.482701", ""],
    ["IVV", "2019-12-16", "USD", "2.039089", ""],
    ["IVV", "2020-03-25", "USD", "1.531356", ""],
    ["IVV", "2020-06-15", "USD", "1.260874", ""],
    ["IVV", "2020-09-23", "USD", "1.505804", ""],
    ["IVV", "2020-12-14", "USD", "1.610222", ""],
    ["IVV", "2021-03-25", "USD", "1.311101", ""],
]

# TODO: Deduplicate this function. (Other function is found in trades_data_provider.py)
def _only_allow_nonempty_str(s, name):
    assert isinstance(s, str)
    assert isinstance(name, str)
    s = s.strip()
    if len(s) == 0:
        raise ValueError(f"{name} must be non-empty.")
    return s

# TODO: Deduplicate this function. (Other function is found in trades_data_provider.py)
def _only_allow_decimal_rep(s, name):
    assert isinstance(s, str)
    assert isinstance(name, str)
    s = s.strip()
    if not re_decimal.fullmatch(s):
        raise ValueError(f"{name} must be in a decimal representation.")
    return Fraction(s)


def get_stock_dividend_overrides(market_data_overrides_path):
    """
    Reads the user's stock dividend overrides data file and returns the data.
    """
    filepath = os.path.join(
            market_data_overrides_path,
            _STOCK_DIVIDENDS_FILEPATH,
        )

    if not os.path.isfile(filepath):
        _logger.debug(f"No stock dividends overrides file at path {filepath}.")
        _logger.debug(f"Creating a new stock dividends overrides file.")
        file_data = (_SAMPLE_STOCK_DIVIDENDS_FILE_DOCROWS
                     + _SAMPLE_STOCK_DIVIDENDS_FILE_LABELS
                     + _SAMPLE_STOCK_DIVIDENDS_FILE_DATA)
        mkdir_recursive_for_filepath(filepath)
        with open(filepath, "w") as f:
            csv.writer(f, dialect="excel").writerows(file_data)
        _logger.debug(f"New stock dividends overrides file saved.")

    data = None

    _logger.debug(f"Reading stock dividends overrides file at {filepath}.")
    with open(filepath, "r") as f:
        data = list(list(x) for x in csv.reader(f))

    assert isinstance(data, list)
    assert all(isinstance(x, list) for x in data)
    assert all(isinstance(x, str) for x in chain(*data))
    
    if len(data) == 0:
        raise ValueError(f"CSV file '{filepath}' must be non-empty.")
    if not all((len(x) == 5) for x in data):
        raise ValueError(f"CSV file '{filepath}' must be comprised of only 5-column rows.")

    # We find the label row, then we separate the data rows out.

    label_row_index = -1
    for (i, row_data) in enumerate(data):
        if row_data == _SAMPLE_STOCK_DIVIDENDS_FILE_LABELS[0]:
            label_row_index = i
            break
    else:
        raise ValueError(f"CSV file '{filepath}' must contain the column labels row.")

    del data[:(label_row_index + 2)] # Delete the label row and everything before it

    ret = {}
    for row in data:
        ric_symbol = _only_allow_nonempty_str(row[0], "RIC symbol")
        date = datetime.date.fromisoformat(row[1].strip())
        currency_symbol = _only_allow_nonempty_str(row[2], "Currency symbol").upper()
        ex_dividend_amount_per_unit = _only_allow_decimal_rep(row[3], "Ex-dividend amount per unit")
        #comment = row[3].strip() # We don't care about the comment row yet

        if not currency_symbol.isalpha():
            raise ValueError("Unit currency must only be alphabet characters.")

        subdict = ret.setdefault(ric_symbol, {})
        if date in subdict:
            raise ValueError(f"Stock dividends overrides file contains duplicate date: {ric_symbol} {date}")
        subdict[date] = Currency(currency_symbol, ex_dividend_amount_per_unit)

    # ret looks like {ric_symbol: {date: exdividend_value}}
    return ret

