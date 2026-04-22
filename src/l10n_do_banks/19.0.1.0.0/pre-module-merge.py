"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Merges custom modules into their replacements before the upgrade so that Odoo
can locate all related database records (ir_model_data,
ir_module_module_dependency, view keys, etc.) under the new module names
before any model/view loading takes place.

Merges performed:
  - account_auto_transfer_features → account_transfer_features
  - payment_azul                   → payment_azul_webpages

Affects (for each merge):
  - ir_module_module          — module registry entry
  - ir_module_module_dependency — downstream module dependencies
  - ir_model_data             — XML IDs owned by the module
  - ir_ui_view.key            — view technical keys (module.xmlid prefix)
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

_MERGES = [
    ("account_auto_transfer_features", "account_transfer_features"),
    ("payment_azul", "payment_azul_webpages"),
]


def migrate(cr, version):
    for old_module, into_module in _MERGES:
        util.merge_module(cr, old_module, into_module)
        _logger.info("Module merged: %r → %r", old_module, into_module)
