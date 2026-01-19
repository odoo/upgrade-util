import logging

from odoo import api, SUPERUSER_ID
from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)


def _update_views(env):
    views = env["ir.ui.view"].search(
        [
            ("model", "=", "stock.picking"),
            ("arch_db", "ilike", "analytic_account_id"),
        ]
    )
    for view in views:
        new_arch = view.arch_db.replace(
            "analytic_account_id", "analytic_distribution"
        )
        if new_arch != view.arch_db:
            view.write({"arch_db": new_arch})
            _logger.info("Updated view %s (%s)", view.name, view.id)


def migrate(cr, version):
    del version

    if not util.module_installed(cr, "stock_analytic"):
        _logger.info("stock_analytic not installed; skipping view fix.")
        return

    env = api.Environment(cr, SUPERUSER_ID, {})
    _update_views(env)
