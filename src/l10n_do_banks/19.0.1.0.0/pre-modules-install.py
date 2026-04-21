import logging

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)


def install_modules(cr):
    util.force_install_module(
        cr,
        "l10n_do_account_withholding_tax",
        if_installed=["account_reconcile_payment"],
    )

def migrate(cr, version):
    install_modules(cr)