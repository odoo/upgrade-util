# -*- coding: utf-8 -*-
import atexit
import logging
import os

from odoo.addons.base.maintenance.migrations import util

_REGISTERED = False


class CriticalHandler(logging.Handler):
    def __init__(self):
        super(CriticalHandler, self).__init__(logging.CRITICAL)

    def emit(self, record):
        global _REGISTERED
        if _REGISTERED:
            return

        # force exit with status_code=1 if any critical log is emit during upgrade
        atexit.register(os._exit, 1)
        _REGISTERED = True


def migrate(cr, version):
    if util.on_CI():  # hopefully temporary restriction
        logging.getLogger("odoo").addHandler(CriticalHandler())
