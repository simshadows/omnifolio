# -*- coding: ascii -*-

"""
Filename: exceptions.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

This file contains all custom exceptions thrown by this codebase.
"""

class NoAPIKeyProvided(Exception):
    """
    Thrown if no API key was provided.
    """
    pass

class MissingData(Exception):
    """
    Thrown if not enough data can be provided (e.g. if a symbol is invalid).
    """
    pass

