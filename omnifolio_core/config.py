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
    "generated_data_path": "./_generated_data",
    "preferred_currency": "USD",
}

_CONFIG_TYPES = {
    "alpha_vantage_api_key": str,
    "iex_cloud_api_key": str,
    "rapidapi_api_key": str,
    "user_data_path": str,
    "generated_data_path": str,
    "preferred_currency": str,
}

_logger = logging.getLogger(__name__)

def get_config():
    if os.path.isfile(_CONFIG_PATH):
        _logger.debug("Reading configuration file.")
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
                _logger.warning(f"Key '{key}' not found in configuration file. Using default value.")
                data[key] = deepcopy(default_value)
                updated = True
            elif not isinstance(data[key], _CONFIG_TYPES[key]):
                type_name = str(_CONFIG_TYPES[key])
                raise TypeError(f"Value of key '{key}' in configuration file must be of type {type_name}.")

        if updated:
            _logger.warning(f"Saving updated configuration file.")
            fwrite_json(_BACKUP_PATH, data=backup)
            fwrite_json(_CONFIG_PATH, data=data)

        # We now write new config keys, derived from the existing ones.
        data["debugging_path"] = os.path.join(data["generated_data_path"], "debugging")
        data["market_data_overrides_path"] = os.path.join(data["user_data_path"], "market_data_overrides")
        # And we clean up existing ones
        data["preferred_currency"] = data["preferred_currency"].strip()

        return data
    else:
        _logger.debug("No configuration file found.")
        _logger.debug("Creating a new configuration file.")
        fwrite_json(_CONFIG_PATH, data=_DEFAULT_CONFIG)
        _logger.debug("New configuration file saved.")
        return _DEFAULT_CONFIG

