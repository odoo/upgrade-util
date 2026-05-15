"""
Pre-migration script for l10n_do_banks 19.0.1.0.0

Force-installs replacement modules whose functionality has been split out
of legacy custom modules in v19.
"""

import logging

from odoo.addons.base.maintenance.migrations import util as mig_util

_logger = logging.getLogger(__name__)

_INSTALLS = [
    ("account_payment_internal_transfer", ["account_payment_cash_custom_workflow"]),
]


def migrate(cr, version):
    for module, if_installed in _INSTALLS:
        mig_util.force_install_module(cr, module, if_installed=if_installed)
        _logger.info("Module force-installed: %r (if installed: %s)", module, if_installed)
