# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    server_act_array = (
        "array_agg(c.ir_actions_server_id)" if util.column_exists(cr, "ir_cron", "ir_actions_server_id") else "NULL"
    )
    xid_date = (
        ", x.date_init" if util.column_exists(cr, "ir_model_data", "date_init") else ""
    )  # field gone in saas-13.4
    cr.execute(
        """
            SELECT array_agg(c.id), {server_act_array}
              FROM ir_cron c
              JOIN ir_model_data x ON x.model = 'ir.cron' AND x.res_id = c.id
             WHERE x.module = '__upgrade__'
               AND now() - COALESCE(c.create_date, x.create_date {xid_date}) > interval '1 month'
        """.format(
            server_act_array=server_act_array,
            xid_date=xid_date,
        )
    )
    cron_ids, server_act_ids = cr.fetchone()
    util.remove_records(cr, "ir.cron", cron_ids)
    util.remove_records(cr, "ir.actions.server", server_act_ids)
