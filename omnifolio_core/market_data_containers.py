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
        "symbol",     # str

        "prices",     # {datetime.date: DayPrices}
                      # The date is the day in which the particular data point is applicable for.

        "events",     # {datetime.date: DayEvents}
                      # The date is the day in which the particular data point is applicable for.

        "extra_data", # dict
    ])

