# -*- coding: ascii -*-

"""
Filename: currency.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

TODO: Should we include checks for whether or not operations are done without loss of information?
      Or at least, we should audit the Fraction object for this, because I don't know if Fraction
      will ever truncate values.
"""

from copy import copy
from math import isnan
from fractions import Fraction

from ..utils import fraction_to_decimal


def _unary_magic_calc_op(method):
    def fn(self):
        new = copy(self)
        new._value = getattr(self._value, method)()
        assert isinstance(new._value, Fraction)
        return new
    return fn

def _binary_magic_calc_op(method, mode):
    assert isinstance(mode, str)
    if mode == "only_with_dimensionless":
        def fn(self, other):
            if isnan(other):
                return other
            elif isinstance(other, Currency):
                raise ValueError(f"'{method}' is not allowed between two Currency objects.")
            new = copy(self)
            new._value = getattr(self._value, method)(Fraction(other))
            assert isinstance(new._value, Fraction)
            return new
        return fn
    elif mode == "only_between_currencies":
        def fn(self, other):
            if isnan(other):
                return other
            elif not isinstance(other, Currency):
                raise ValueError(f"Cannot call {type(self).__name__}.{method} with a {type(other).__name__} object. "
                                 f"'{method}' is only allowed to be used with another Currency object.")
            self._err_if_same_symbol(other)
            new = copy(self)
            new._value = getattr(self._value, method)(other._value)
            assert isinstance(new._value, Fraction)
            return new
        return fn
    else:
        raise RuntimeError("Invalid mode.")

def _binary_magic_division_op(method):
    """
    If other is a Currency, we return dimensionless.
    If other is dimensionless, we return a currency.
    """
    def fn(self, other):
        if isnan(other):
            return other
        elif isinstance(other, Currency):
            self._err_if_same_symbol(other)
            ret = getattr(self._value, method)(other._value)
            assert isinstance(ret, Fraction)
            return ret
        else:
            new = copy(self)
            new._value = getattr(self._value, method)(Fraction(other))
            assert isinstance(new._value, Fraction)
            return new
    return fn

def _binary_magic_rdivision_op(method):
    """
    If other is a Currency, we return dimensionless.
    If other is dimensionless, we raise an exception since this is not implemented.
    """
    def fn(self, other):
        if isnan(other):
            return other
        elif isinstance(other, Currency):
            self._err_if_same_symbol(other)
            ret = getattr(self._value, method)(other._value)
            assert isinstance(ret, Fraction)
            return ret
        else:
            raise NotImplementedError(f"'{method}' is not implemented for a left-hand dimensionless.")
    return fn

def _binary_magic_comparison_op(method):
    def fn(self, other):
        if isnan(other):
            return other
        elif isinstance(other, Currency):
            # If the other object is a Currency, we enforce matching symbol,
            # and compare value attributes.
            self._err_if_same_symbol(other)
            return getattr(self._value, method)(other._value)
        else:
            # If the other object isn't a Currency, we instead compare with
            # the entire object.
            return getattr(self._value, method)(other)
    return fn


class Currency:
    """
    Objects instantiated by this class can be considered immutable and hashable.
    """

    __slots__ = [
            "_value",
            "_symbol",
        ]
    
    def __init__(self, symbol, *args, **kwargs):
        self._value = Fraction(*args, **kwargs)
        self._symbol = symbol.strip()
        assert isinstance(self._symbol, str)
        assert isinstance(self._value, Fraction)
        return

    @property
    def value(self):
        return self._value

    @property
    def symbol(self):
        return self._symbol

    def convert(self, new_symbol, exchange_rate):
        """
        Converts the Currency object into a different currency using the exchange rate.

        For example, if we have:
            base = Currency("USD", 100)
        We can convert this object to AUD using a USD/AUD exchange rate.
        Suppose that USD/AUD is exactly 1.3 (i.e. you get AUD$1.3 for every USD$1.0):
            quote = base.convert("AUD", 1.3)
        """
        if isinstance(new_symbol, float) and isnan(new_symbol):
            return new_symbol
        elif isnan(exchange_rate):
            return exchange_rate
        assert isinstance(new_symbol, str)
        assert not isinstance(exchange_rate, Currency)
        assert isinstance(self._value, Fraction)
        new = copy(self)
        new._value *= Fraction(exchange_rate)
        new._symbol = new_symbol
        assert isinstance(new._value, Fraction)
        return new

    def __hash__(self):
        return hash((self._value, self._symbol))

    def __repr__(self):
        s = self._symbol.replace("'", r"\'")
        return f"Currency('{s}', {repr(self._value)})"

    def __str__(self):
        return f"{self._symbol} {fraction_to_decimal(self._value)}"

    __lt__ = _binary_magic_comparison_op("__lt__")
    __le__ = _binary_magic_comparison_op("__le__")
    __eq__ = _binary_magic_comparison_op("__eq__")
    __ne__ = _binary_magic_comparison_op("__ne__")
    __gt__ = _binary_magic_comparison_op("__gt__")
    __ge__ = _binary_magic_comparison_op("__ge__")

    __add__      = _binary_magic_calc_op("__add__"     , "only_between_currencies")
    __sub__      = _binary_magic_calc_op("__sub__"     , "only_between_currencies")
    __mul__      = _binary_magic_calc_op("__mul__"     , "only_with_dimensionless")
    __truediv__  = _binary_magic_division_op("__truediv__")
    __floordiv__ = _binary_magic_division_op("__floordiv__")
    __mod__      = _binary_magic_calc_op("__mod__"     , "only_with_dimensionless")
    __divmod__   = _binary_magic_calc_op("__divmod__"  , "only_with_dimensionless")
    __pow__      = _binary_magic_calc_op("__pow__"     , "only_with_dimensionless")

    __radd__      = _binary_magic_calc_op("__radd__"     , "only_between_currencies")
    __rsub__      = _binary_magic_calc_op("__rsub__"     , "only_between_currencies")
    __rmul__      = _binary_magic_calc_op("__rmul__"     , "only_with_dimensionless")
    __rtruediv__  = _binary_magic_rdivision_op("__truediv__")
    __rfloordiv__ = _binary_magic_rdivision_op("__floordiv__")
    __rmod__      = _binary_magic_calc_op("__rmod__"     , "only_with_dimensionless")
    __rdivmod__   = _binary_magic_calc_op("__rdivmod__"  , "only_with_dimensionless")
    __rpow__      = _binary_magic_calc_op("__rpow__"     , "only_with_dimensionless")

    __neg__ = _unary_magic_calc_op("__neg__")
    __pos__ = _unary_magic_calc_op("__pos__")
    __abs__ = _unary_magic_calc_op("__abs__")

    #__complex__ = _unary_magic_calc_op("__complex__")

    def __float__(self):
        return self._value.__float__()

    def __round__(self, *args):
        new = copy(self)
        new._value = self._value.__round__(*args)
        assert isinstance(new._value, Fraction)
        return new

    __trunc__ = _unary_magic_calc_op("__trunc__")
    __floor__ = _unary_magic_calc_op("__floor__")
    __ceil__  = _unary_magic_calc_op("__ceil__" )

    # Utility Methods

    def _err_if_same_symbol(self, other):
        assert isinstance(self, Currency)
        assert isinstance(other, Currency)
        if self._symbol != other._symbol:
            raise ValueError(f"Invalid operation for mismatched currencies '{self._symbol}' and '{other._symbol}'.")
        return

