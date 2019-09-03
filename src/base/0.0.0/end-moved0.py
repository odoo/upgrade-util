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

    new_moved0 = set(moved[0] for moved in cr.fetchall()) - util.ENVIRON["moved0"]

    if new_moved0:
        raise util.MigrationError(
            "New `moved0` field. It happen when the ORM cannot change a column type by itself.\n%s"
            % "\n".join("\t- " + m for m in sorted(new_moved0))
        )
