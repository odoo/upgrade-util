# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    # fmt:off
    cr.execute("""
        SELECT concat(table_name, '.', column_name)
          FROM information_schema.columns
         WHERE column_name LIKE '%__moved_'
    """)  # noqa:Q440
    # fmt:on

    util.ENVIRON["moved0"] = set(moved[0] for moved in cr.fetchall())
