# -*- coding: ascii -*-

"""
Filename: hello_world.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Some test code for connecting a Jupyter notebook with Python source files.
"""

import logging
import json

import requests

from .config import get_config
from .utils import fwrite_json, fread_json
from .market_data_aggregator import MarketDataAggregator

logger = logging.getLogger(__name__)

def run():
    config = get_config()

    #params = {
    #    "function": "TIME_SERIES_DAILY_ADJUSTED",
    #    "outputsize": "full",
    #    "symbol": "ASX:CBA",
    #    "interval": "5min",
    #    "apikey": config["alpha_vantage_api_key"],
    #}
    #res = requests.get("https://www.alphavantage.co/query", params=params)
    #data = json.loads(res.text)
    #print(res.status_code)

    #params = {
    #    "token": config["iex_cloud_api_key"],
    #}
    #res = requests.get("https://sandbox.iexapis.com/stable/stock/twtr/chart/max", params=params)
    #data = json.loads(res.text)
    #print(res.status_code)

    aggregator = MarketDataAggregator(config)
    data = aggregator.stock_timeseries_daily_pandas(["TSLA"])

    return data

