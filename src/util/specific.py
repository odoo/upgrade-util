# -*- coding: utf-8 -*-
import logging

from .helpers import _validate_table
from .misc import _cached
from .models import rename_model
from .modules import rename_module
from .pg import column_exists, rename_table, table_exists
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)


def dbuuid(cr):
    return _dbuuids(cr)[-1]


@_cached
def _dbuuids(cr):
    cr.execute(
        """
        SELECT REPLACE(value, 'upg-neuter-', '')
          FROM ir_config_parameter
         WHERE key IN ('database.uuid', 'origin.database.uuid')
      ORDER BY key
        """
    )
    return [uuid for (uuid,) in cr.fetchall()]


def dispatch_by_dbuuid(cr, version, callbacks):
    """
    Allow to execute a migration script for a specific database only, based on its dbuuid.

    .. example::
        .. code-block:: python
            def db_yellowbird(cr, version):
                cr.execute("DELETE FROM ir_ui_view WHERE id=837")

            util.dispatch_by_dbuuid(cr, version, {
                "ef81c07aa90936a89f4e7878e2ebc634a24fcd66": db_yellowbird,
            })

    :param str version: Odoo version
    :param dict[str, function] callbacks: mapping dbuuids to the functions to run against matching dbs

    .. warning::
        - Only the first match of (icp["database.uuid"], icp["origin.database.uuid"]) in `callbacks` is executed.

    .. tip::
        - If looking to prevent a callback from running against a descendant db, one can use a noop `callback`:
        .. example::
            .. code-block:: python
                noop = lambda *args: None
                util.dispatch_by_dbuuid(cr, version, {
                    "dbuuid": noop,
                    "ancestor's dbuuid": db_yellowbird,
                })
    """
    for uuid in _dbuuids(cr):
        if uuid in callbacks:
            func = callbacks[uuid]
            _logger.info("calling dbuuid-specific function `%s`", func.__name__)
            func(cr, version)
            break


def rename_custom_model(cr, model_name, new_model_name, custom_module=None, report_details=""):
    cr.execute("SELECT 1 FROM ir_model WHERE model = %s", [model_name])
    if not cr.rowcount:
        _logger.warning("Model %r not found: skip renaming", model_name)
        return

    rename_model(cr, model_name, new_model_name, rename_table=True)
    module_details = " from module '{}'".format(custom_module) if custom_module else ""
    add_to_migration_reports(
        category="Custom models",
        message="The custom model '{model_name}'{module_details} was renamed to '{new_model_name}'. {report_details}".format(
            **locals()
        ),
    )


def rename_custom_module(cr, old_module_name, new_module_name, report_details="", author="%"):
    cr.execute("SELECT 1 FROM ir_module_module WHERE name = %s AND author ILIKE %s", [old_module_name, author])
    if not cr.rowcount:
        return

    rename_module(cr, old_module_name, new_module_name)
    _logger.warning("Custom module %r renamed to %r", old_module_name, new_module_name)
    add_to_migration_reports(
        category="Custom modules",
        message="The custom module '{old_module_name}' was renamed to '{new_module_name}'. {report_details}".format(
            **locals()
        ),
    )


def rename_custom_table(
    cr,
    table_name,
    new_table_name,
    custom_module=None,
    report_details="",
):
    if not table_exists(cr, table_name):
        _logger.warning("Table %r not found: skip renaming", table_name)
        return

    rename_table(cr, table_name, new_table_name, remove_constraints=False)

    module_details = " from module '{}'".format(custom_module) if custom_module else ""
    add_to_migration_reports(
        category="Custom tables/columns",
        message="The custom table '{table_name}'{module_details} was renamed to '{new_table_name}'. {report_details}".format(
            **locals()
        ),
    )


def rename_custom_column(cr, table_name, col_name, new_col_name, custom_module=None, report_details=""):
    _validate_table(table_name)
    if not column_exists(cr, table_name, col_name):
        _logger.warning("Column %r not found on table %r: skip renaming", col_name, table_name)
        return
    cr.execute('ALTER TABLE "{}" RENAME COLUMN "{}" TO "{}"'.format(table_name, col_name, new_col_name))
    module_details = " from module '{}'".format(custom_module) if custom_module else ""
    add_to_migration_reports(
        category="Custom tables/columns",
        message="The custom column '{col_name}' of the table '{table_name}'{module_details} was renamed to '{new_col_name}'."
        " {report_details}".format(**locals()),
    )


def reset_cowed_views(cr, xmlid, key=None):
    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

    module, _, name = xmlid.partition(".")
    if not key:
        key = xmlid
    cr.execute(
        """
        UPDATE ir_ui_view u
           SET arch_prev = u.arch_db,
               arch_db = v.arch_db
          FROM ir_ui_view v
          JOIN ir_model_data m
            ON m.res_id = v.id AND m.model = 'ir.ui.view'
         WHERE u.key = %s
           AND m.module = %s
           AND m.name = %s
           AND u.website_id IS NOT NULL
        RETURNING u.id
        """,
        [key, module, name],
    )
    return set(sum(cr.fetchall(), ()))
