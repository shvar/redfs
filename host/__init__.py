#!/usr/bin/python
"""Host."""

try:
    from host_magic import ENABLE_DBC  # pylint:disable=F0401
except ImportError:
    ENABLE_DBC = False

import contrib.dbc
contrib.dbc.ENABLED = ENABLE_DBC

from common.db import model_tweaks
model_tweaks._FILE_UUID_IS_NULLABLE = False
model_tweaks._HOST_USER_ID_IS_NULLABLE = True
