"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Renames the custom module `account_auto_transfer_features` to
`account_transfer_features`.  This must run before the upgrade so that
Odoo can locate the module under its new name, and so that all related
database records (ir_model_data, ir_module_module_dependency, view keys,
etc.) are updated atomically before any model/view loading takes place.

Affects:
  - ir_module_module          — module registry entry
  - ir_module_module_dependency — downstream module dependencies
  - ir_model_data             — XML IDs owned by the module
  - ir_ui_view.key            — view technical keys (module.xmlid prefix)
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

_OLD_MODULE = "account_auto_transfer_features"
_NEW_MODULE = "account_transfer_features"


def migrate(cr, version):
    _rename_module(cr)


def _rename_module(cr):
    """Rename account_auto_transfer_features → account_transfer_features.

    util.rename_custom_module checks whether the old module exists in
    ir_module_module before acting, so it is safe to run even on databases
    that were already migrated or that never had the old module installed.
    """
    util.rename_custom_module(cr, _OLD_MODULE, _NEW_MODULE)
    _logger.info(
        "Module renamed: %r → %r",
        _OLD_MODULE,
        _NEW_MODULE,
    )
