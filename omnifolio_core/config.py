# -*- coding: ascii -*-

"""
Filename: config.py
Author:   contact@simshadows.com
License:  GNU Affero General Public License v3 (AGPL-3.0)

Anything to do with reading/writing configuration files.
"""

import os
import logging
from copy import deepcopy

from .utils import fwrite_json, fread_json

_CONFIG_PATH = "config.json"
_BACKUP_PATH = "config.json.backup"

_DEFAULT_CONFIG = {
    "alpha_vantage_api_key": "",
    "iex_cloud_api_key": "",
    "rapidapi_api_key": "",
    "user_data_path": "./_user_data",
    "market_data_store_path": "./_user_data/market_data_store",
    "debugging_path": "./_user_data/debugging",
}

_CONFIG_TYPES = {
    "alpha_vantage_api_key": str,
    "iex_cloud_api_key": str,
    "rapidapi_api_key": str,
    "user_data_path": str,
    "market_data_store_path": str,
    "debugging_path": str,
}

logger = logging.getLogger(__name__)

def get_config():
    if os.path.isfile(_CONFIG_PATH):
        logger.debug("Reading configuration file.")
        data = fread_json(_CONFIG_PATH)
        backup = deepcopy(data)

        if not isinstance(data, dict):
            raise TypeError("Config file root must be a JSON object.")
        
        # We quickly check for missing keys, and if any keys are missing, we update the
        # existing config file.
        updated = False
        for (key, default_value) in _DEFAULT_CONFIG.items():
            assert isinstance(default_value, _CONFIG_TYPES[key])
            if key not in data:
                logger.warning(f"Key '{key}' not found in configuration file. Using default value.")
                data[key] = deepcopy(default_value)
                updated = True
            elif not isinstance(data[key], _CONFIG_TYPES[key]):
                type_name = str(_CONFIG_TYPES[key])
                raise TypeError(f"Value of key '{key}' in configuration file must be of type {type_name}.")

        if updated:
            logger.warning(f"Saving updated configuration file.")
            fwrite_json(_BACKUP_PATH, data=backup)
            fwrite_json(_CONFIG_PATH, data=data)

        return data
    else:
        logger.debug("No configuration file found.")
        logger.debug("Creating a new configuration file.")
        fwrite_json(_CONFIG_PATH, data=_DEFAULT_CONFIG)
        logger.debug("New configuration file saved.")
        return _DEFAULT_CONFIG

