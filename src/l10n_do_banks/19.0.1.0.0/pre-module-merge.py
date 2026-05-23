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
from odoo.addons.base.maintenance.migrations import util as mig_util

_logger = logging.getLogger(__name__)

_MERGES = [
    ("account_auto_transfer_features", "account_transfer_features"),
    ("payment_azul", "payment_azul_webpages"),
    ("account_reconcile_payment", "l10n_do_account_withholding_tax")
]


def migrate(cr, version):
    # Seed a fake previous version for l10n_do_account_withholding_tax so that
    # Odoo treats force_install_module as an UPGRADE (not a fresh install).
    # Without this, the module has no latest_version → state = 'to install' →
    # upgrade/19.0.1.0.0/post-migrate.py is skipped entirely.
    cr.execute("""
        INSERT INTO ir_module_module (name, state, latest_version)
        VALUES ('l10n_do_account_withholding_tax', 'installed', '17.0.1.0.0')
        ON CONFLICT (name) DO UPDATE
            SET latest_version = '17.0.1.0.0'
            WHERE ir_module_module.latest_version IS NULL
    """)
    _logger.info(
        "Seeded latest_version=17.0.1.0.0 for l10n_do_account_withholding_tax "
        "so its post-migrate.py runs as an upgrade."
    )

    # Force-install replacement modules BEFORE merging so that
    # account_reconcile_payment is still in 'installed' state for the checks.
    mig_util.force_upgrade_of_fresh_module(
        cr,
        "l10n_do_account_withholding_tax",
        init=True,
    )
    mig_util.force_install_module(
        cr,
        "l10n_do_withholding_certification",
        if_installed=["l10n_do_account_withholding_tax"],
    )

    for old_module, into_module in _MERGES:
        util.merge_module(cr, old_module, into_module)
        _logger.info("Module merged: %r → %r", old_module, into_module)
