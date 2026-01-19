import logging

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)


def _update_views(cr):
    cr.execute(
        """
        UPDATE ir_ui_view
           SET arch_db = replace(
               replace(
                   arch_db::text,
                   'analytic_account_id',
                   'analytic_distribution'
               ),
               'analytic_tag_ids',
               'analytic_distribution'
           )::jsonb
         WHERE model = 'stock.picking'
           AND (
               arch_db::text ILIKE '%%analytic_account_id%%'
               OR arch_db::text ILIKE '%%analytic_tag_ids%%'
           )
        """
    )
    _logger.info("Updated %s stock.picking views.", cr.rowcount)


def migrate(cr, version):
    del version

    if not util.module_installed(cr, "stock_analytic"):
        _logger.info("stock_analytic not installed; skipping view fix.")
        return

    _update_views(cr)
