# -*- coding: ascii -*-

"""
Filename: pandas_utils.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

General reusable utility code for use with the pandas library.
"""

import os
import logging
from copy import deepcopy

import pandas as pd

from .utils import (
        public,
        mkdir_recursive_for_filepath,
    )

_logger = logging.getLogger(__name__)

@public
def pandas_index_union(iterable_obj):
    ret = None
    for index_obj in iterable_obj:
        assert isinstance(index_obj, pd.Index)
        if ret is None:
            ret = deepcopy(index_obj)
        else:
            ret = ret.union(index_obj)
    return ret

@public
def pandas_add_column_level_above(df, new_level_name):
    """
    Adds a level above the current column levels, and sets the new level to new_level_name
    for all columns.

    For example:

          A B C D
        a 0 1 2 3
        b 4 5 6 7
        c 8 9 0 1

    After running pandas_add_column_level_above(df, "X", inplace=True):

          X X X X
          A B C D
        a 0 1 2 3
        b 4 5 6 7
        c 8 9 0 1

    Returns df.
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(new_level_name, str)
    return pd.concat({new_level_name: df}, axis="columns")

@public
def dump_df_to_csv_debugging_file(df, debugging_path, filename):
    assert isinstance(df, pd.DataFrame)
    assert isinstance(filename, str)

    filepath = os.path.join(
            debugging_path,
            filename,
        )
    logging.debug(f"Writing to debugging file '{filepath}'.")
    mkdir_recursive_for_filepath(filepath)
    with open(filepath, "w") as f:
        f.write(df.dropna(how="all").to_csv())
    return

