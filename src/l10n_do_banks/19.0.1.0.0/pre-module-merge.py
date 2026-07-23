"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Merges custom modules into their replacements before the upgrade so that Odoo
can locate all related database records (ir_model_data,
ir_module_module_dependency, view keys, etc.) under the new module names
before any model/view loading takes place.

Merges performed:
  - account_auto_transfer_features → account_transfer_features
  - payment_azul                   → payment_azul_webpages
  - account_reconcile_payment      → l10n_do_account_withholding_tax
  - hr_employee_relative (OCA)     → l10n_do_hr

Affects (for each merge):
  - ir_module_module          — module registry entry
  - ir_module_module_dependency — downstream module dependencies
  - ir_model_data             — XML IDs owned by the module
  - ir_ui_view.key            — view technical keys (module.xmlid prefix)

Note on hr_employee_relative: in v17 the OCA module `hr_employee_relative`
defined the `hr.employee.relative` model (table, base fields, views) and
`l10n_do_hr` extended it. In v19 `l10n_do_hr` absorbs the model (declares it
with its own `_name`). `merge_module` reassigns the OCA module's ownership of
the model/fields/data to `l10n_do_hr` WITHOUT dropping the shared table (it
never calls `delete_model`), removes the now-redundant OCA base views, and
deletes the OCA module registry entry. This is safer than listing it in
`pre-modules-uninstall.py`: `uninstall_module` would `delete_model` (dropping
the `hr_employee_relative` table and every relative row) if it ran before
`l10n_do_hr` re-declares the model. The leftover OCA employee-form view
(relocated to `l10n_do_hr.hr_employee_view_form`) is removed in
`pre-views-delete.py`.
"""

import logging

from odoo.addons.base.maintenance.migrations import util as mig_util
from odoo.upgrade import util

_logger = logging.getLogger(__name__)

_MERGES = [
    ("account_auto_transfer_features", "account_transfer_features"),
    ("payment_azul", "payment_azul_webpages"),
    ("account_reconcile_payment", "l10n_do_account_withholding_tax"),
    ("hr_employee_relative", "l10n_do_hr"),
]


def migrate(cr, version):

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
