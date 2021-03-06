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
from fractions import Fraction

from ..utils import fraction_to_decimal


def _unary_magic_calc_op(method):
    def fn(self):
        new = copy(self)
        new._value = getattr(self._value, method)()
        return new
    return fn

def _binary_magic_calc_op(method):
    def fn(self, other):
        self._err_if_same_symbol(other)
        new = copy(self)
        new._value = getattr(self._value, method)(other._value)
        return new
    return fn

def _binary_magic_comparison_op(method):
    def fn(self, other):
        if isinstance(other, Currency):
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
        assert isinstance(symbol, str)
        self._value = Fraction(*args, **kwargs)
        self._symbol = symbol.strip()
        return

    @property
    def value(self):
        return self._value

    @property
    def symbol(self):
        return self._symbol

    def __hash__(self):
        return hash((self._value, self._symbol))

    def __repr__(self):
        s = self._symbol.replace("'", r"\'")
        return f"Currency('{s}', {repr(self._value)})"

    def __str__(self):
        return f"{self._symbol}[{fraction_to_decimal(self._value)}]"

    __lt__ = _binary_magic_comparison_op("__lt__")
    __le__ = _binary_magic_comparison_op("__le__")
    __eq__ = _binary_magic_comparison_op("__eq__")
    __ne__ = _binary_magic_comparison_op("__ne__")
    __gt__ = _binary_magic_comparison_op("__gt__")
    __ge__ = _binary_magic_comparison_op("__ge__")

    __add__      = _binary_magic_calc_op("__add__"     )
    __sub__      = _binary_magic_calc_op("__sub__"     )
    __mul__      = _binary_magic_calc_op("__mul__"     )
    __truediv__  = _binary_magic_calc_op("__truediv__" )
    __floordiv__ = _binary_magic_calc_op("__floordiv__")
    __mod__      = _binary_magic_calc_op("__mod__"     )
    __divmod__   = _binary_magic_calc_op("__divmod__"  )
    __pow__      = _binary_magic_calc_op("__pow__"     )

    __radd__      = _binary_magic_calc_op("__radd__"     )
    __rsub__      = _binary_magic_calc_op("__rsub__"     )
    __rmul__      = _binary_magic_calc_op("__rmul__"     )
    __rtruediv__  = _binary_magic_calc_op("__rtruediv__" )
    __rfloordiv__ = _binary_magic_calc_op("__rfloordiv__")
    __rmod__      = _binary_magic_calc_op("__rmod__"     )
    __rdivmod__   = _binary_magic_calc_op("__rdivmod__"  )
    __rpow__      = _binary_magic_calc_op("__rpow__"     )

    #__iadd__      = _binary_magic_calc_op("__iadd__"     )
    #__isub__      = _binary_magic_calc_op("__isub__"     )
    #__imul__      = _binary_magic_calc_op("__imul__"     )
    #__itruediv__  = _binary_magic_calc_op("__itruediv__" )
    #__ifloordiv__ = _binary_magic_calc_op("__ifloordiv__")
    #__imod__      = _binary_magic_calc_op("__imod__"     )
    #__idivmod__   = _binary_magic_calc_op("__idivmod__"  )
    #__ipow__      = _binary_magic_calc_op("__ipow__"     )

    __neg__ = _unary_magic_calc_op("__neg__")
    __pos__ = _unary_magic_calc_op("__pos__")
    __abs__ = _unary_magic_calc_op("__abs__")

    __complex__ = _unary_magic_calc_op("__complex__")
    __float__   = _unary_magic_calc_op("__float__"  )

    def __round__(self, *args):
        new = copy(self)
        new._value = self._value.__round__(*args)
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

