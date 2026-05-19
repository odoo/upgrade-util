# -*- coding: utf-8 -*-
import atexit
import logging
import os
import sys

from .misc import on_CI

_REGISTERED = False

_logger = logging.getLogger(__name__.rpartition(".")[0])


def getLogger(name=None):
    """
    Return a logger named after the caller.

    If `name` is a dotter module, it is used as-is. Otherwise the logger name is taken
    from the caller's `__name__` when set to a dotted module name (Odoo 16+), or built
    from the last three components of the caller's file path as
    `odoo.upgrade.<module>.<version>.<script>`. `<script>` would be `name` parameter if
    set and non-dotted.
    """
    if name and "." in name:
        return logging.getLogger(name)
    frame = sys._getframe(1)
    script = ""
    if name is None:
        name = frame.f_globals.get("__name__") or ""
    else:
        script = name
    if "." not in name:
        path = frame.f_globals.get("__file__") or ""
        parts = os.path.normpath(path).split(os.sep)
        module, version, script = parts[-3], parts[-2], script or os.path.splitext(parts[-1])[0]
        prefix = __name__.rsplit(".", 2)[0]
        name = ".".join([prefix, module, version, script])
    return logging.getLogger(name)


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
