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
import logging
import json
from datetime import date, datetime
from copy import deepcopy
from uuid import uuid4
from fractions import Fraction
from decimal import Decimal

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

def fraction_to_decimal(fraction_obj):
    return Decimal(fraction_obj.numerator) / Decimal(fraction_obj.denominator)

def generate_unused_key(desired_key, container, key_regenerator=lambda x : x + str(uuid4())):
    """
    Returns 'desired_key' or some modified version of it that tests False to '(desired_key in container)'.

    Calls 'key_regenerator' to generate new keys as necessary, and may be called multiple times.

    'key_regenerator' must inject a sufficiently randomized element with a realistic expectation that
    a new unused key will eventually be reached within relatively few calls.

    The default 'key_regenerator" function is suitable for general strings.
    """
    assert callable(key_regenerator)
    if desired_key not in container:
        return desired_key
    while True:
        new_key = key_regenerator(desired_key)
        if new_key not in container:
            return new_key
    raise RuntimeError("Reached an unreachable section.")

def has_callable_attr(obj, attr_name):
    return hasattr(obj, attr_name) and callable(getattr(obj, attr_name))

def create_json_writable_debugging_structure(obj):
    """
    Generates an intended-for-debugging-use json-serializable version of 'obj'.

    The output of 'create_json_writable_debugging_structure()' is NOT intended to be machine-read.
    The mapping from 'obj' to 'create_json_writable_debugging_structure(obj)' is not intended to be
    stable. The objective of this function is to make something human readable.

    TODO: Consider checking for cycles.
    """
    is_json_value = (isinstance(obj, str)
                     or isinstance(obj, int)
                     or isinstance(obj, float)
                     or isinstance(obj, bool)
                     or (obj is None))
    if is_json_value:
        return obj
    elif isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, Fraction):
        return str(fraction_to_decimal(obj))
    elif isinstance(obj, dict):
        ret = {}
        for (k, v) in obj.items():
            k = generate_unused_key(str(k), ret)
            ret[k] = create_json_writable_debugging_structure(v)
        return ret
    elif isinstance(obj, tuple):
        if has_callable_attr(obj, "_asdict"):
            # Assumed to be namedtuple
            return create_json_writable_debugging_structure(obj._asdict()) # I'm lazy
        else:
            # Assumed to be a vanilla tuple
            return [create_json_writable_debugging_structure(x) for x in obj]
    elif isinstance(obj, list) or isinstance(obj, set):
        return [create_json_writable_debugging_structure(x) for x in obj]
    #obj_type_str = str(type(obj))
    #raise RuntimeError(f"{obj_type_str} is not a supported type by create_json_writable_debugging_structure().")
    return str(obj)

def pandas_index_union(iterable_obj):
    ret = None
    for index_obj in iterable_obj:
        assert isinstance(index_obj, pd.Index)
        if ret is None:
            ret = deepcopy(index_obj)
        else:
            ret = ret.union(index_obj)
    return ret

def pandas_add_column_level_above(df, new_level_name, *, inplace):
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

    inplace must be set to True, otherwise the function throws an error.
    (This is to make it clear that the function will be modifying df.)

    Returns df.
    """
    if not inplace:
        raise NotImplementedError("inplace=False operation is not yet implemented.")
    assert isinstance(df, pd.DataFrame)
    assert isinstance(new_level_name, str)
    df.columns = pd.MultiIndex.from_tuples([(new_level_name,) + tuple(x) for x in df.columns])
    return df

