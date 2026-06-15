"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Migrates the legacy ``wk_odoo_directly_print_reports`` module (direct report
printing via QZ Tray) into its sustainable, Odoo-native replacement
``report_zpl_direct_print``.

Steps performed (only when the legacy module is installed):
  1. Rename the custom models so their tables, m2o relations and stored data
     survive under the new namespace:
       - report.template     -> report.zpl.template
       - wk_printer.printer   -> report.zpl.printer
     The custom ``ir.actions.report`` columns (report_user_action, printer_id,
     use_template, report_template_id, multi_copy_print, default_copies) keep
     the same names, so their data is preserved untouched.
  2. Merge the legacy module into the replacement. ``merge_module`` re-homes the
     registry entry, downstream dependencies (including
     ``product_label_for_zebra_printer``), XML IDs and view keys, then DELETES
     the legacy module record and force-installs the replacement. In other
     words, the old module is uninstalled only AFTER all its data has been
     migrated -- never before, which would drop the data.
  3. Defensive cleanup: should the legacy module somehow still be present after
     the merge, uninstall it explicitly.
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

OLD_MODULE = "wk_odoo_directly_print_reports"
NEW_MODULE = "report_zpl_direct_print"

_MODEL_RENAMES = [
    ("report.template", "report.zpl.template"),
    ("wk_printer.printer", "report.zpl.printer"),
]


def migrate(cr, version):
    if not util.module_installed(cr, OLD_MODULE):
        _logger.info("%r not installed; nothing to migrate.", OLD_MODULE)
        return

    for old_model, new_model in _MODEL_RENAMES:
        if util.table_exists(cr, util.table_of_model(cr, old_model)):
            util.rename_model(cr, old_model, new_model)
            _logger.info("Model renamed: %r -> %r", old_model, new_model)

    util.merge_module(cr, OLD_MODULE, NEW_MODULE)
    _logger.info("Module merged and legacy module retired: %r -> %r", OLD_MODULE, NEW_MODULE)

    if util.module_installed(cr, OLD_MODULE):
        util.uninstall_module(cr, OLD_MODULE)
        _logger.info("Legacy module %r uninstalled after data migration.", OLD_MODULE)
