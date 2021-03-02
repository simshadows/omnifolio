# -*- coding: ascii -*-

"""
Filename: utils.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

General reusable utility code.

This file does not contain any dependencies towards any other file within this codebase.
"""

import os
import re
import copy
import logging
import json

import pandas as pd

logger = logging.getLogger(__name__)

_CWD = os.getcwd()
_ENCODING = "utf-8"

re_decimal = re.compile(r"^\d*[.,]?\d*$")

def mkdir_recursive(relfilepath):
    absfilepath = os.path.join(_CWD, relfilepath)
    absdir = os.path.dirname(absfilepath)
    try:
        os.makedirs(absdir)
    except FileExistsError:
        pass
    return

# This overwrites whatever file is specified with the data.
def fwrite_json(relfilepath, data=None):
    mkdir_recursive(relfilepath)
    with open(relfilepath, encoding=_ENCODING, mode="w") as f:
        f.write(json.dumps(data, sort_keys=True, indent=4))
    return

def fread_json(relfilepath):
    with open(relfilepath, encoding=_ENCODING, mode="r") as f:
        return json.loads(f.read())

def str_is_nonempty_and_compact(obj):
    return (
            isinstance(obj, str)
            and (len(obj) > 0)
            and (obj == obj.strip())
        )

def pandas_index_union(iterable_obj):
    ret = None
    for index_obj in iterable_obj:
        assert isinstance(index_obj, pd.Index)
        if ret is None:
            ret = copy.deepcopy(index_obj)
        else:
            ret = ret.union(index_obj)
    return ret

