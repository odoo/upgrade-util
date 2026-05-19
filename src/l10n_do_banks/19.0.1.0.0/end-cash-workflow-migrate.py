"""End-migration: merge account_payment_cash_custom_workflow into account_payment_internal_transfer."""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

OLD_MODULE = "account_payment_cash_custom_workflow"
NEW_MODULE = "account_payment_internal_transfer"


def migrate(cr, version):
    util.merge_module(cr, OLD_MODULE, NEW_MODULE)
    _logger.info("Merged %s -> %s", OLD_MODULE, NEW_MODULE)
