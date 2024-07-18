# -*- coding: utf-8 -*-
import logging

from .fields import rename_field
from .helpers import _validate_table
from .misc import _cached
from .models import rename_model
from .modules import rename_module
from .pg import column_exists, rename_table, table_exists
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)


@_cached
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
    -------
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


def rename_custom_field(cr, old_field_name, new_field_name, model, report_details=""):
    cr.execute("SELECT 1 FROM ir_model_fields WHERE name = %s AND model = %s", [old_field_name, model])
    if not cr.rowcount:
        _logger.warning("Field %r not found: skip renaming", old_field_name)
        return

    rename_field(cr, model, old_field_name, new_field_name)
    add_to_migration_reports(
        category="Custom fields",
        message="The custom field '{old_field_name}' was renamed to '{new_field_name}'. {report_details}".format(
            **locals()
        ),
    )


def rename_custom_module(cr, old_module_name, new_module_name, report_details=""):
    cr.execute("SELECT 1 FROM ir_module_module WHERE name = %s", [old_module_name])
    if not cr.rowcount:
        _logger.warning("Module %r not found: skip renaming", old_module_name)
        return

    rename_module(cr, old_module_name, new_module_name)
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
