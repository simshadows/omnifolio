# -*- coding: ascii -*-

"""
Filename: utils.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

General reusable utility code.

This file does not contain any dependencies towards any other file within this codebase.
"""

import os
import sys
import re
import logging
import json
from datetime import date, datetime
from copy import deepcopy
from uuid import uuid4
from fractions import Fraction
from decimal import Decimal

_logger = logging.getLogger(__name__)

_CWD = os.getcwd()
_ENCODING = "utf-8"

##########################################################################################
# '__all__' Management ###################################################################
##########################################################################################

def public(obj):
    """"
    Automatically adds the object's name to '__all__'.
    Also automatically sets '__all__ = []' for a module on first call.

    Originally from:
        <https://code.activestate.com/recipes/576993-public-decorator-adds-an-item-to-__all__/>
    By user 'Sam Denton', accessed on 2021-03-20.
    Additional notes:
        * Based on an idea by Duncan Booth:
          <http://groups.google.com/group/comp.lang.python/msg/11cbb03e09611b8a>
        * Improved via a suggestion by Dave Angel:
          <http://groups.google.com/group/comp.lang.python/msg/3d400fb22d8a42e1>
    """
    module_all = sys.modules[obj.__module__].__dict__.setdefault('__all__', [])
    if obj.__name__ not in module_all:
        module_all.append(obj.__name__)
    return obj

public(public) # Export public itself

##########################################################################################
# Everything Else ########################################################################
##########################################################################################

re_decimal = re.compile(r"^\d*[.,]?\d*$")
__all__.append("re_decimal")

@public
def mkdir_recursive_for_filepath(relfilepath):
    absfilepath = os.path.join(_CWD, relfilepath)
    absdir = os.path.dirname(absfilepath)
    try:
        os.makedirs(absdir)
    except FileExistsError:
        pass
    return

# This overwrites whatever file is specified with the data.
@public
def fwrite_json(relfilepath, data=None):
    mkdir_recursive_for_filepath(relfilepath)
    with open(relfilepath, encoding=_ENCODING, mode="w") as f:
        f.write(json.dumps(data, sort_keys=True, indent=4))
    return

@public
def fread_json(relfilepath):
    with open(relfilepath, encoding=_ENCODING, mode="r") as f:
        return json.loads(f.read())

@public
def str_is_nonempty_and_compact(obj):
    return (
            isinstance(obj, str)
            and (len(obj) > 0)
            and (obj == obj.strip())
        )

@public
def fraction_to_decimal(fraction_obj):
    return Decimal(fraction_obj.numerator) / Decimal(fraction_obj.denominator)

@public
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

@public
def has_callable_attr(obj, attr_name):
    return hasattr(obj, attr_name) and callable(getattr(obj, attr_name))

@public
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

