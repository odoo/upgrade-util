"""
End-migration: finalize cash-workflow -> internal-transfer migration.

At end-time all modules have been loaded, including the new module, so its
group XMLID is now resolvable.
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

OLD_MODULE = "account_payment_cash_custom_workflow"
NEW_GROUP_XMLID = (
    "account_payment_internal_transfer."
    "group_account_payment_edit_internal_transfer_amount"
)


def migrate(cr, version):
    if not version:
        return

    if not util.module_installed(cr, OLD_MODULE):
        return

    _apply_group_membership(cr)
    util.uninstall_module(cr, OLD_MODULE)
    _logger.info("Uninstalled %s", OLD_MODULE)


def _apply_group_membership(cr):
    uids = util.ENVIRON.get("__cash_workflow_migration", {}).get("old_group_uids") or []
    if not uids:
        return

    new_group_id = util.ref(cr, NEW_GROUP_XMLID)
    if not new_group_id:
        _logger.warning(
            "Group %r not found; %d users won't be migrated", NEW_GROUP_XMLID, len(uids)
        )
        return

    cr.execute(
        """
        INSERT INTO res_groups_users_rel (gid, uid)
             SELECT %s, unnest(%s::int[])
        ON CONFLICT DO NOTHING
        """,
        [new_group_id, list(uids)],
    )
    _logger.info(
        "Migrated %d users to group %s", cr.rowcount, NEW_GROUP_XMLID
    )
