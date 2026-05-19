"""
End-migration: account_payment_cash_custom_workflow → account_payment_internal_transfer.
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

OLD_MODULE = "account_payment_cash_custom_workflow"
NEW_MODULE = "account_payment_internal_transfer"

OLD_M2M_TABLE = "cash_replenishment_payments_rel"
NEW_M2M_TABLE = "account_payment_internal_transfer_linked_rel"

OLD_GROUP_XMLID = f"{OLD_MODULE}.group_account_payment_edit_refund_amount"
NEW_GROUP_XMLID = f"{NEW_MODULE}.group_account_payment_edit_internal_transfer_amount"

OLD_PAYMENT_COLUMNS = ("refunded", "cash_replenishment", "beneficiary_bank_account")


def migrate(cr, version):
    if not util.module_installed(cr, OLD_MODULE):
        return

    _copy_refunded_to_transferred(cr)
    _copy_linked_payments_m2m(cr)
    _migrate_group_membership(cr)

    util.merge_module(cr, OLD_MODULE, NEW_MODULE)
    _logger.info("Merged %s -> %s", OLD_MODULE, NEW_MODULE)

    _drop_orphan_schema(cr)


def _copy_refunded_to_transferred(cr):
    if not util.column_exists(cr, "account_payment", "refunded"):
        _logger.info("account_payment.refunded missing; skip transferred copy")
        return
    if not util.column_exists(cr, "account_payment", "transferred"):
        _logger.warning("account_payment.transferred missing; %s not installed, skip data copy", NEW_MODULE)
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
    if not util.table_exists(cr, NEW_M2M_TABLE):
        _logger.warning("%r missing; %s not installed, skip m2m copy", NEW_M2M_TABLE, NEW_MODULE)
        return

    cr.execute(
        f"""
        INSERT INTO {NEW_M2M_TABLE} (transfer_payment_id, linked_payment_id)
             SELECT r.payment_id, r.replenishment_payment_id
               FROM {OLD_M2M_TABLE} r
               JOIN account_payment p ON p.id = r.payment_id
              WHERE p.cash_replenishment = TRUE
        ON CONFLICT DO NOTHING
        """
    )
    _logger.info("Copied %d m2m rows %s -> %s", cr.rowcount, OLD_M2M_TABLE, NEW_M2M_TABLE)


def _migrate_group_membership(cr):
    old_group_id = util.ref(cr, OLD_GROUP_XMLID)
    if not old_group_id:
        return

    new_group_id = util.ref(cr, NEW_GROUP_XMLID)
    if not new_group_id:
        _logger.warning("Group %r not found; skip group membership migration", NEW_GROUP_XMLID)
        return

    cr.execute(
        """
        INSERT INTO res_groups_users_rel (gid, uid)
             SELECT %s, uid
               FROM res_groups_users_rel
              WHERE gid = %s
        ON CONFLICT DO NOTHING
        """,
        [new_group_id, old_group_id],
    )
    _logger.info("Migrated %d group memberships to %s", cr.rowcount, NEW_GROUP_XMLID)


def _drop_orphan_schema(cr):
    """Drop physical leftovers that merge_module does not touch: legacy
    columns/tables that have no equivalent in the new module."""
    for column in OLD_PAYMENT_COLUMNS:
        if util.column_exists(cr, "account_payment", column):
            util.remove_column(cr, "account_payment", column)
            _logger.info("Dropped orphan column account_payment.%s", column)

    if util.table_exists(cr, OLD_M2M_TABLE):
        cr.execute(f'DROP TABLE "{OLD_M2M_TABLE}" CASCADE')
        _logger.info("Dropped orphan table %s", OLD_M2M_TABLE)
