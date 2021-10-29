# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.util.records import _remove_records


def migrate(cr, version):
    xid_date = (
        ", x.date_init" if util.column_exists(cr, "ir_model_data", "date_init") else ""
    )  # field gone in saas-13.4
    cr.execute(
        """
            SELECT array_agg(c.id)
              FROM ir_cron c
              JOIN ir_model_data x ON x.model = 'ir.cron' AND x.res_id = c.id
             WHERE x.module = '__upgrade__'
               AND now() - COALESCE(c.create_date, x.create_date {xid_date}) > interval '1 month'
        """.format(
            xid_date=xid_date
        )
    )
    _remove_records(cr, "ir.cron", cr.fetchone()[0])
