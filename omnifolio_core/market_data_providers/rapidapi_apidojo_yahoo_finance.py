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
from collections import OrderedDict

from requests import get

from ..exceptions import NoAPIKeyProvided
from ..market_data_containers import (DayPrices,
                                      DayEvents,
                                      StockTimeSeriesDailyResult)
from ..market_data_provider import MarketDataProvider
from ..utils import fwrite_json, fread_json

logger = logging.getLogger(__name__)

class RAADYahooFinance(MarketDataProvider):

    def __init__(self, config):
        self._api_key = config["rapidapi_api_key"]
        return

    def get_trust_value(self):
        return self.GENERALLY_UNTRUSTED - self.CURRENCY_GUESS_DEDUCTION - self.PRECISION_GUESS_DEDUCTION

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
        curr_time = datetime.datetime.utcnow()

        if res.status_code != 200:
            logger.warning(f"API call for symbol '{symbol}' returned status {res.status_code}.")

        raw_data = json.loads(res.text)

        # TODO: Better error handling for easy error triage?
        #print(res.text)
        #fwrite_json("tmp.txt", data=raw_data)

        extra_data = {
                "firstTradeDate": raw_data["firstTradeDate"],
                "timeZone": raw_data["timeZone"],
            }

        prices_list = []
        for d in raw_data["prices"]:
            if "adjclose" not in d:
                logger.debug(
                        f"Encountered a missing 'adjclose' key in the price list. " \
                        f"Skipping this record. Keys are: {str(set(d))}."
                    )
                continue
            elif {"date", "open", "high", "low", "close", "volume", "adjclose"} != set(d):
                logger.debug(f"Unexpected set of keys. Full key list: {str(set(d))}.")

            date = datetime.date.fromtimestamp(d["date"] + extra_data["timeZone"]["gmtOffset"])

            data_point = DayPrices(
                    data_source=self.get_provider_name(),
                    data_trust_value=self.get_trust_value(),
                    data_collection_time=curr_time,

                    open=int(round(d["open"] * 1000, 0)),
                    high=int(round(d["high"] * 1000, 0)),
                    low=int(round(d["low"] * 1000, 0)),
                    close=int(round(d["close"] * 1000, 0)),
                    adjusted_close=int(round(d["adjclose"] * 1000, 0)),

                    volume=d["volume"],

                    unit="unknown",
                    price_denominator=1000,
                )
            prices_list.append((date, data_point, ))

        events_list = []
        for d in raw_data["eventsData"]:
            if d["type"] == "SPLIT":
                if {"data", "date", "denominator", "numerator", "splitRatio", "type"} != set(d):
                    logger.debug(f"Unexpected set of keys. Full key list: {str(set(d))}.")

                date = datetime.date.fromtimestamp(d["date"] + extra_data["timeZone"]["gmtOffset"])

                data_point = DayEvents(
                        data_source=self.get_provider_name(),
                        data_trust_value=self.get_trust_value(),
                        data_collection_time=curr_time,

                        dividend=0,
                        split_factor_numerator=d["numerator"],
                        split_factor_denominator=d["denominator"],
                    )
                events_list.append((date, data_point, ))
            else:
                logger.debug(f"Unrecognized type '{d['type']}'. Ignoring.")

        prices_list.sort(key=lambda x : x[0])
        events_list.sort(key=lambda x : x[0])

        # We need to reject the latest data point due to date unreliability.

        prices_latest_date = prices_list[-1][0]
        events_latest_date = events_list[-1][0]
        date_to_reject = prices_latest_date if (prices_latest_date > events_latest_date) else events_latest_date

        prices=OrderedDict(prices_list)
        events=OrderedDict(events_list)
        if date_to_reject in prices:
            logger.debug(f"Rejecting date {date_to_reject} in the prices dict.")
            del prices[date_to_reject]
        if date_to_reject in events:
            logger.debug(f"Rejecting date {date_to_reject} in the events dict.")
            del events[date_to_reject]

        to_return = StockTimeSeriesDailyResult(
                symbol=symbol,
                prices=prices,
                events=events,
                extra_data=extra_data,
            )
        return to_return

