# -*- coding: utf-8 -*-
import logging

from .helpers import _validate_table
from .pg import column_exists, table_exists
from .report import add_to_migration_reports

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


def rename_custom_table(cr, table_name, new_table_name, custom_module=None, report_details=""):
    _validate_table(table_name)
    _validate_table(new_table_name)
    if not table_exists(cr, table_name):
        return
    cr.execute('ALTER TABLE "{}" RENAME TO "{}"'.format(table_name, new_table_name))
    module_details = " from module '{}'".format(custom_module) if custom_module else ""  # noqa
    add_to_migration_reports(
        category="Custom tables/columns",
        message="The custom table '{table_name}'{module_details} was renamed to '{new_table_name}'. {report_details}".format(
            **locals()
        ),
    )


def rename_custom_column(cr, table_name, col_name, new_col_name, custom_module=None, report_details=""):
    _validate_table(table_name)
    if not column_exists(cr, table_name, col_name):
        return
    cr.execute('ALTER TABLE "{}" RENAME COLUMN "{}" TO "{}"'.format(table_name, col_name, new_col_name))
    module_details = " from module '{}'".format(custom_module) if custom_module else ""  # noqa
    add_to_migration_reports(
        category="Custom tables/columns",
        message="The custom column '{col_name}' of the table '{table_name}'{module_details} was renamed to '{new_col_name}'."
        " {report_details}".format(**locals()),
    )
