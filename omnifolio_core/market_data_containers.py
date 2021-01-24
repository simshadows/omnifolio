# -*- coding: ascii -*-

"""
Filename: market_data_containers.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Container classes primarily for MarketDataAggregator, MarketDataProvider.
"""

from collections import namedtuple

##########################################################################################
# stock_timeseries_daily #################################################################
##########################################################################################

DayPrices = namedtuple("DayPrices", [
        #
        # This is the date in which the data is applicable for.
        #
        "date",                 # datetime.date

        #
        # Metadata on where this data is from, and the time it was pulled from the server/source.
        #
        "data_source",          # str
        "data_trust_value",     # int
        "data_collection_time", # datetime.datetime

        #
        # Price data.
        #
        "open",                 # int | None (Must be divided by price_denominator to get actual price.)
        "high",                 # int | None (Must be divided by price_denominator to get actual price.)
        "low",                  # int | None (Must be divided by price_denominator to get actual price.)
        "close",                # int | None (Must be divided by price_denominator to get actual price.)
        "adjusted_close",       # int | None (Must be divided by price_denominator to get actual price.)

        #
        # Other data.
        #
        "volume",               # int | None

        #
        # Currency information and coefficients.
        #
        "unit",                 # str (E.g. "USD")
        "price_denominator",    # int (All prices must be divided by price_denominator to get the actual price.)
    ])

DayEvents = namedtuple("DayEvents", [
        #
        # This is the date in which the data is applicable for.
        #
        "date",

        # Metadata on where this data is from, and the time it was pulled from the server/source.
        #
        "data_source",          # str
        "data_trust_value",     # int
        "data_collection_time", # datetime.datetime

        #
        # Data values for this date.
        #
        "dividend",                 # int (Must be divided by price_denominator to get actual price.)
        "split_factor_numerator",   # int
        "split_factor_denominator", # int
    ])

StockTimeSeriesDailyResult = namedtuple("StockTimeSeriesDailyResult", [
        "symbol",          # str
        "prices_list",     # list(MarketDataStore.DayPrices)
        "events_list",     # list(MarketDataStore.DayEvents)
        "extra_data_dict", # dict
    ])

