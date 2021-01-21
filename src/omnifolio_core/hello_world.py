# -*- coding: ascii -*-

"""
Filename: hello_world.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Some test code for connecting a Jupyter notebook with Python source files.
"""

import logging
import requests

logger = logging.getLogger(__name__)

def run():
    logger.info("Hello, world!")

    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": "IBM",
        "interval": "5min",
        "apikey": "demo",
    }
    res = requests.get("https://www.alphavantage.co/query", params=params)
    return res.text

