# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    # fmt:off
    cr.execute(r"""
        SELECT table_name, column_name
          FROM information_schema.columns
         WHERE column_name LIKE '%\_moved_'
    """)  # noqa:Q440
    # fmt:on

    moved_fields = set(tuple(moved) for moved in cr.fetchall()) - util.ENVIRON["moved0"]

    remaining_fields = []
    moved_ignore = (  # ignore tables created after moved0 to avoir error
        "account_payment_pre_backup",
        "account_bank_statement_line_pre_backup",
    )
    for f in moved_fields:
        if f[0] in moved_ignore:
            continue

        # if the moved column is empty, just remove it
        cr.execute("SELECT EXISTS(SELECT 1 FROM {0} WHERE {1} IS NOT NULL)".format(*f))

        if not cr.fetchone()[0]:
            cr.execute("ALTER TABLE {0} DROP COLUMN {1}".format(*f))
            continue

        remaining_fields.append(f)

    if remaining_fields:
        raise util.MigrationError(
            "New `moved0` field. It happen when the ORM cannot change a column type by itself.\n%s"
            % "\n".join("\t- %s.%s" % m for m in sorted(remaining_fields))
        )
