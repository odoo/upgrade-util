# -*- coding: utf-8 -*-
# Utility functions for migration scripts


import logging
import sys
from contextlib import contextmanager

from .orm import env
from .pg import get_columns

try:

    from odoo.sql_db import db_connect

except ImportError:

    from openerp.sql_db import db_connect


try:
    from odoo.api import Environment

    manage_env = Environment.manage
except ImportError:
    try:
        from openerp.api import Environment

        manage_env = Environment.manage
    except ImportError:

        @contextmanager
        def manage_env():
            yield


_logger = logging.getLogger(__name__.rpartition(".")[0])


# python3 shims
try:
    basestring
except NameError:
    basestring = unicode = str


def main(func, version=None):
    """a main() function for scripts"""
    # NOTE: this is not recommanded when the func callback use the ORM as the addon-path is
    # incomplete. Please pipe your script into `odoo shell`.
    # Do not forget to commit the cursor at the end.
    if len(sys.argv) != 2:
        sys.exit("Usage: %s <dbname>" % (sys.argv[0],))
    dbname = sys.argv[1]
    with db_connect(dbname).cursor() as cr, manage_env():
        func(cr, version)


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


@contextmanager
def no_fiscal_lock(cr):
    env(cr)["res.company"].invalidate_cache()
    columns = [col for col in get_columns(cr, "res_company")[0] if col.endswith("_lock_date")]
    assert columns
    set_val = ", ".join("{} = NULL".format(col) for col in columns)
    returns = ", ".join("old.{}".format(col) for col in columns)
    cr.execute(
        """
            UPDATE res_company c
               SET {}
              FROM res_company old
             WHERE old.id = c.id
         RETURNING {}, old.id
        """.format(
            set_val, returns
        )
    )
    data = cr.fetchall()
    yield
    set_val = ", ".join("{} = %s".format(col) for col in columns)
    cr.executemany(
        """
            UPDATE res_company
               SET {}
             WHERE id = %s
        """.format(
            set_val
        ),
        data,
    )


__all__ = list(locals())
