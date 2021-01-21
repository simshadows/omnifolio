# -*- coding: ascii -*-

"""
Filename: utils.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

General reusable utility code.

This file does not contain any dependencies towards any other file within this codebase.
"""

import os
import logging
import json

logger = logging.getLogger(__name__)

_CWD = os.getcwd()
_ENCODING = "utf-8"

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
        f.write(json.dumps(data, sort_keys=True, indent=3))
    return

def fread_json(relfilepath):
    with open(relfilepath, encoding=_ENCODING, mode="r") as f:
        return json.loads(f.read())

