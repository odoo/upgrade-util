"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Renames custom modules before the upgrade so that Odoo can locate them
under their new names, and so that all related database records
(ir_model_data, ir_module_module_dependency, view keys, etc.) are updated
atomically before any model/view loading takes place.

Renames performed:
  - account_auto_transfer_features → account_transfer_features
  - payment_azul                   → payment_azul_webpages

Affects (for each rename):
  - ir_module_module          — module registry entry
  - ir_module_module_dependency — downstream module dependencies
  - ir_model_data             — XML IDs owned by the module
  - ir_ui_view.key            — view technical keys (module.xmlid prefix)
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

_RENAMES = [
    ("account_auto_transfer_features", "account_transfer_features"),
    ("payment_azul", "payment_azul_webpages"),
]


def migrate(cr, version):
    for old_module, new_module in _RENAMES:
        _rename_module(cr, old_module, new_module)


def _rename_module(cr, old_module, new_module):
    """Rename a custom module in the database.

    util.rename_custom_module checks whether the old module exists in
    ir_module_module before acting, so it is safe to run even on databases
    that were already migrated or that never had the old module installed.
    """
    util.rename_custom_module(cr, old_module, new_module)
    _logger.info(
        "Module renamed: %r → %r",
        old_module,
        new_module,
    )
