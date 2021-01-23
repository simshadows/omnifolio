# -*- coding: ascii -*-

"""
Filename: rapidapi_apidojo_yahoo_finance.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Base class for MarketDataProvider.

MarketDataProvider encapsulates everything specific to a particular data provider (e.g. Morningstar,
or Yahoo Finance).
"""

import json
import datetime
import logging

from requests import get

from ..exceptions import NoAPIKeyProvided
from ..market_data_provider import MarketDataProvider

logger = logging.getLogger(__name__)

class RAADYahooFinance(MarketDataProvider):

    def __init__(self, config):
        self._api_key = config["rapidapi_api_key"]
        return

    def get_trust_value(self):
        return self.GENERALLY_UNTRUSTED

    def stock_timeseries_daily(self, symbols_list):
        if len(self._api_key) == 0:
            raise NoAPIKeyProvided
        if not isinstance(symbols_list, list): # TODO: Allow all iterables?
            raise TypeError
        if len(symbols_list) == 0:
            raise ValueError("Expected non-empty list.")

        data = {}
        for symbol in symbols_list:
            if symbol in data:
                logger.warning(f"Duplicate symbol '{symbol}' in list. Ignoring.")
                continue
            data[symbol] = self._stock_timeseries_daily_one_symbol(symbol)

        return data

    def _stock_timeseries_daily_one_symbol(self, symbol):
        if not isinstance(symbol, str):
            raise TypeError
        if len(symbol) == 0:
            raise ValueError("Expected non-empty string.")

        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data"
        params = {
            "symbol": symbol,
        }
        headers = {
            "x-rapidapi-key": self._api_key,
            "x-rapidapi-host": "apidojo-yahoo-finance-v1.p.rapidapi.com",
        }
        res = get(url, params=params, headers=headers)

        if res.status_code != 200:
            logger.warning(f"API call for symbol '{symbol}' returned status {res.status_code}.")

        # TODO: Better error handling for easy error triage?
        #print(res.text)

        raw_data = json.loads(res.text)

        extra_data_dict = {
                "firstTradeDate": raw_data["firstTradeDate"],
                "timeZone": raw_data["timeZone"],
            }

        prices_list = []
        for d in raw_data["prices"]:
            if "adjclose" not in d:
                logger.debug(f"Encountered a missing 'adjclose' key in the price list. Keys are: {str(set(d))}.")
                continue
            elif {"date", "open", "high", "low", "close", "volume", "adjclose"} != set(d):
                logger.debug(f"Unexpected set of keys. Full key list: {str(set(d))}.")
            to_append = MarketDataProvider.DayPrices(
                    date=datetime.date.fromtimestamp(d["date"] + extra_data_dict["timeZone"]["gmtOffset"]),

                    open=int(round(d["open"] * 1000, 0)),
                    high=int(round(d["high"] * 1000, 0)),
                    low=int(round(d["low"] * 1000, 0)),
                    adjusted_close=int(round(d["adjclose"] * 1000, 0)),

                    volume=d["volume"],

                    unit="unknown",
                    price_denominator=1000,
                )
            prices_list.append(to_append)

        events_list = []
        for d in raw_data["eventsData"]:
            if d["type"] == "SPLIT":
                if {"data", "date", "denominator", "numerator", "splitRatio", "type"} != set(d):
                    logger.debug(f"Unexpected set of keys. Full key list: {str(set(d))}.")
                to_append = MarketDataProvider.DayEvents(
                        date=datetime.date.fromtimestamp(d["date"] + extra_data_dict["timeZone"]["gmtOffset"]),

                        dividend=0,
                        split_factor_numerator=d["numerator"],
                        split_factor_denominator=d["denominator"],
                    )
                events_list.append(to_append)
            else:
                logger.debug(f"Unrecognized type '{d['type']}'. Ignoring.")

        to_return = MarketDataProvider.StockTimeSeriesDailyResult(
                symbol=symbol,
                prices_list=prices_list,
                events_list=events_list,
                extra_data_dict=extra_data_dict,
            )
        return to_return

