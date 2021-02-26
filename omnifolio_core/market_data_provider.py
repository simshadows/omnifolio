# -*- coding: ascii -*-

"""
Filename: market_data_provider.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Base class for MarketDataProvider.

MarketDataProvider encapsulates everything specific to a particular data provider (e.g. Morningstar,
or Yahoo Finance).
"""

from collections import namedtuple
from abc import ABC, abstractmethod

import pandas as pd

class MarketDataProvider(ABC):

    # Trust values guidelines for the get_trust_value() function.
    VERY_TRUSTED        = 300
    GENERALLY_TRUSTED   = 200
    SOMEWHAT_TRUSTED    = 100
    TRUST_LEVEL_UNSURE  = 0
    SOMEWHAT_UNTRUSTED  = -100
    GENERALLY_UNTRUSTED = -200
    VERY_UNTRUSTED      = -300

    # Further deductions to trust value
    CURRENCY_ADJUSTMENT_DEDUCTION = 5 # Deduct if a provider automatically adjusts for currency changes.
    PRECISION_GUESS_DEDUCTION = 4 # Deduct if we make a guess on decimal precision.
    FLOATING_POINT_IMPRECISION_DEDUCTION = 50 # Deduct if a provider returns floating point values.

    @abstractmethod
    def get_trust_value(self):
        """
        Get an integer arbitrarily indicating how trustworthy a data source is.

        ---

        Positive integers are "trusted", while negative integers are "distrusted".

        Guidelines for values can be seen above (VERY_TRUSTED, GENERALLY_TRUSTED, etc.).

        Generally, scrapers tend to be untrusted (~-200), while APIs direct to a reputable
        provider such as Morningstar are trusted (~200). API calls directly to a stock exchange
        (if that's ever possible for a small projects such as this one) will be greatly trusted
        (~300).

        Values may deviate from the guideline as desired.
        """
        raise NotImplementedError

    def get_provider_name(self):
        """
        Get a string that uniquely identifies the particular provider class.
        """
        return type(self).__name__ # We just return the class name.

    ######################################################################################

    @abstractmethod
    def stock_timeseries_daily(self, symbols_list):
        """
        Get full daily history of a list of stocks and/or ETFs.

        Parameters:

            symbols_list:
                A list of strings, with each string representing a stock or ETF.
                Must have at least one item in the list.

        Returns:

            A dict of pandas.DataFrame objects.
            The dict is guaranteed to have every symbol from symbols_list as a key.

            (I will document the DataFrame later.)
        """
        raise NotImplementedError

