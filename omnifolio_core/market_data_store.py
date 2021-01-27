# -*- coding: ascii -*-

"""
Filename: market_data_store.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

MarketDataStore encapsulates the actual storage mechanism behind MarketDataAggregator.
"""

import os
import logging
import datetime
from collections import namedtuple

from .market_data_containers import (DayPrices,
                                     DayEvents,
                                     StockTimeSeriesDailyResult)
from .utils import fwrite_json, fread_json

logger = logging.getLogger(__name__)

class MarketDataStore:

    _STOCK_TIMESERIES_DAILY__BASE_PATH = "stock_timeseries_daily"

    def __init__(self, config):
        self._market_data_store_path = config["market_data_store_path"]
        return

    ######################################################################################

    def stock_timeseries_daily__update_one_symbol(self, symbol, data):
        """
        Updates just one symbol with data from one or more providers.

        'symbol' is a string indicating Omnifolio's internally-used symbol for the stock/ETF.

        'data' is a dictionary where the key is Omnifolio's internally-used provider name as
        a string, and the value is a StockTimeSeriesDailyResult object.
        """
        assert isinstance(symbol, str) and (len(symbol.strip()) > 0)
        assert isinstance(data, dict)

        self._stock_timeseries_daily__write_one_symbol(symbol, data)

        # TODO: Update instead of wiping the original.

        return

    def _stock_timeseries_daily__storage_filepath(self, symbol):
        return os.path.join(
                self._market_data_store_path,
                self._STOCK_TIMESERIES_DAILY__BASE_PATH,
                symbol.lower() + ".json",
            )

    def _stock_timeseries_daily__write_one_symbol(self, symbol, data):
        """
        Writes the stock_timeseries_daily data of one symbol to disk.

        'symbol' is a string indicating Omnifolio's internally-used symbol for the stock/ETF.

        'data' is a dictionary where the key is Omnifolio's internally-used provider name as
        a string, and the value is a StockTimeSeriesDailyResult object.
        """
        assert isinstance(symbol, str) and (len(symbol.strip()) > 0)
        assert isinstance(data, dict)

        storage_filepath = self._stock_timeseries_daily__storage_filepath(symbol)

        #
        # TOP LEVEL
        #

        raw_toplevel = {
                # A string indicating Omnifolio's internally-used symbol for the stock/ETF.
                "symbol": symbol,

                # A dictionary where the key is Omnifolio's internally-used provider name as
                # a string, and the value is a StockTimeSeriesDailyResult object.
                "providers": {},
            }

        #
        # SECOND LEVEL
        #

        for (provider_name, provider_data) in data.items():

            assert isinstance(provider_name, str) and (len(provider_name.strip()) > 0)
            assert isinstance(provider_data, StockTimeSeriesDailyResult)

            psymbol = provider_data.symbol
            prices = provider_data.prices
            events = provider_data.events

            assert isinstance(psymbol, str) and (len(psymbol.strip()) > 0)
            assert isinstance(prices, dict)
            assert isinstance(events, dict)

            provider_data_json = {
                    # "symbol": psymbol, # Not needed
                    "prices": {},
                    "events": {},
                }
            raw_toplevel["providers"][provider_name] = provider_data_json

            #
            # THIRD LEVEL
            #

            for (date, day_data) in prices.items():

                assert isinstance(date, datetime.date)
                assert isinstance(day_data, DayPrices)

                day_data_json = day_data._asdict()
                day_data_json["data_collection_time"] = day_data_json["data_collection_time"].isoformat()

                provider_data_json["prices"][date.isoformat()] = day_data_json

                # TODO: Type check day_data?

            for (date, day_data) in events.items():

                assert isinstance(date, datetime.date)
                assert isinstance(day_data, DayEvents)

                day_data_json = day_data._asdict()
                day_data_json["data_collection_time"] = day_data_json["data_collection_time"].isoformat()

                provider_data_json["events"][date.isoformat()] = day_data_json

                # TODO: Type check day_data?

        fwrite_json(storage_filepath, data=raw_toplevel)
        return

