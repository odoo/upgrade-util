# -*- coding: utf-8 -*-
import atexit
import logging
import os

from .misc import on_CI

_REGISTERED = False

_logger = logging.getLogger(__name__.rpartition(".")[0])


class CriticalHandler(logging.Handler):
    def __init__(self):
        super(CriticalHandler, self).__init__(logging.CRITICAL)

    def emit(self, record):
        global _REGISTERED  # noqa: PLW0603
        if _REGISTERED:
            return

        # force exit with status_code=1 if any critical log is emit during upgrade
        atexit.register(os._exit, 1)
        _REGISTERED = True


if on_CI():  # hopefully temporary restriction
    product = _logger.name.partition(".")[0]
    logging.getLogger(product).addHandler(CriticalHandler())
