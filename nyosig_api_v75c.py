#!/usr/bin/env python3
"""
NyoSig Analysator API v7.5c launcher.

This launcher executes the existing full nyosig_api.py code, but forces the
core preference order to use nyosig_analysator_core_v7.5c.py first and marks
the web API version as v7.5c-web. It keeps the existing API surface intact.
"""

from __future__ import annotations

import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_API_PATH = os.path.join(_SCRIPT_DIR, "nyosig_api.py")
if not os.path.isfile(_API_PATH):
    raise FileNotFoundError("Missing API module: " + _API_PATH)

with open(_API_PATH, "r", encoding="utf-8") as _f:
    _source = _f.read()

_source = _source.replace(
    'Wraps core v7.5a as REST endpoints for Streamlit dashboard.',
    'Wraps core v7.5c as REST endpoints for Streamlit dashboard.'
)
_source = _source.replace(
    'for name in ["nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:',
    'for name in ["nyosig_analysator_core_v7.5c.py", "nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:'
)
_source = _source.replace('APP_VERSION = "v8.1-web"', 'APP_VERSION = "v7.5c-web"')
_source = _source.replace('APP_VERSION = "v7.5a-web"', 'APP_VERSION = "v7.5c-web"')

# Execute with this file name so relative paths stay correct.
_code = compile(_source, _API_PATH, "exec")
exec(_code, globals(), globals())
