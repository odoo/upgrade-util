"""
Pre-migration: account_payment_cash_custom_workflow → account_payment_internal_transfer.

Copies data we want to preserve BEFORE the old module is uninstalled in
end-00-finalize-cash-workflow.py.
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

OLD_MODULE = "account_payment_cash_custom_workflow"
NEW_MODULE = "account_payment_internal_transfer"

OLD_M2M_TABLE = "cash_replenishment_payments_rel"
NEW_M2M_TABLE = "account_payment_internal_transfer_linked_rel"

OLD_GROUP_XMLID = "account_payment_cash_custom_workflow.group_account_payment_edit_refund_amount"


def migrate(cr, version):
    if not version:
        return

    if not util.module_installed(cr, OLD_MODULE):
        _logger.info("%s not installed; nothing to migrate", OLD_MODULE)
        return

    util.force_install_module(cr, NEW_MODULE)

    _create_destination_schema(cr)
    _copy_refunded_to_transferred(cr)
    _copy_linked_payments_m2m(cr)
    _stash_old_group_uids(cr)


def _create_destination_schema(cr):
    """Create destination column/table if the new module hasn't been loaded yet."""
    if util.column_exists(cr, "account_payment", "refunded") and not util.column_exists(
        cr, "account_payment", "transferred"
    ):
        util.create_column(cr, "account_payment", "transferred", "boolean")
        _logger.info("Created column account_payment.transferred (defensive)")

    util.create_m2m(
        cr,
        NEW_M2M_TABLE,
        "account_payment",
        "account_payment",
        col1="transfer_payment_id",
        col2="linked_payment_id",
    )


def _copy_refunded_to_transferred(cr):
    if not util.column_exists(cr, "account_payment", "refunded"):
        _logger.info("account_payment.refunded missing; skip transferred copy")
        return

    util.explode_execute(
        cr,
        """
        UPDATE account_payment
           SET transferred = TRUE
         WHERE refunded = TRUE
           AND COALESCE(transferred, FALSE) = FALSE
           AND {parallel_filter}
        """,
        table="account_payment",
    )
    _logger.info("Copied account_payment.refunded -> transferred")


def _copy_linked_payments_m2m(cr):
    if not util.table_exists(cr, OLD_M2M_TABLE):
        _logger.info("%r missing; skip m2m copy", OLD_M2M_TABLE)
        return
    if not util.column_exists(cr, "account_payment", "cash_replenishment"):
        _logger.info("account_payment.cash_replenishment missing; skip m2m copy")
        return

    cr.execute(
        """
        INSERT INTO account_payment_internal_transfer_linked_rel
                    (transfer_payment_id, linked_payment_id)
             SELECT r.payment_id, r.replenishment_payment_id
               FROM cash_replenishment_payments_rel r
               JOIN account_payment p ON p.id = r.payment_id
              WHERE p.cash_replenishment = TRUE
        ON CONFLICT DO NOTHING
        """
    )
    _logger.info("Copied %d m2m rows %s -> %s", cr.rowcount, OLD_M2M_TABLE, NEW_M2M_TABLE)


def _stash_old_group_uids(cr):
    old_group_id = util.ref(cr, OLD_GROUP_XMLID)
    if not old_group_id:
        return

    cr.execute(
        "SELECT array_agg(uid ORDER BY uid) FROM res_groups_users_rel WHERE gid = %s",
        [old_group_id],
    )
    uids = (cr.fetchone() or [None])[0] or []
    util.ENVIRON.setdefault("__cash_workflow_migration", {})["old_group_uids"] = list(uids)
    _logger.info("Stashed %d uids of old refund-amount group for finalize step", len(uids))
