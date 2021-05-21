# -*- coding: utf-8 -*-
import logging

_logger = logging.getLogger(__name__)


def dbuuid(cr):
    cr.execute(
        """
        SELECT value
          FROM ir_config_parameter
         WHERE key IN ('database.uuid', 'origin.database.uuid')
      ORDER BY key DESC
         LIMIT 1
    """
    )
    return cr.fetchone()[0]


def dispatch_by_dbuuid(cr, version, callbacks):
    """
    Allow to execute a migration script for a specific database only, base on its dbuuid.
    Example:

    >>> def db_yellowbird(cr, version):
            cr.execute("DELETE FROM ir_ui_view WHERE id=837")

    >>> dispatch_by_dbuuid(cr, version, {
            'ef81c07aa90936a89f4e7878e2ebc634a24fcd66': db_yellowbird,
        })
    """
    uuid = dbuuid(cr)
    if uuid in callbacks:
        func = callbacks[uuid]
        _logger.info("calling dbuuid-specific function `%s`", func.__name__)
        func(cr, version)
