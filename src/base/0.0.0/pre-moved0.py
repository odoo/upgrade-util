# -*- coding: utf-8 -*-
from psycopg2.extras import Json

from odoo.addons.base.maintenance.migrations import util

KEY = "base.tests.test_moved0.TestMoved0"


def get_moved0_columns(cr):
    cr.execute(
        """
            SELECT table_name, column_name
              FROM information_schema.columns
             WHERE column_name ~ '_moved[0-9]+'
          ORDER BY table_name, column_name
        """
    )
    return cr.fetchall()


def migrate(cr, version):
    if util.version_gte("16.0"):
        # Starting Odoo 16, no more `moved0` columns are created
        # See https://github.com/odoo/odoo/commit/50767ef90eadeca2ed05b9400238af8bdbe77fb3
        return

    if util.table_exists(cr, "upgrade_test_data"):
        cr.execute("SELECT 1 FROM upgrade_test_data WHERE key = %s", [KEY])
        if cr.rowcount:
            # Already ran as test. ignore
            return
    else:
        # Test not run or not a version that support upgrade tests (<= 12)
        cr.execute(
            """
                CREATE TABLE upgrade_test_data (
                    key VARCHAR(255) PRIMARY KEY,
                    value JSONB NOT NULL
                )
            """
        )

    util.ENVIRON["manual_moved0"] = True

    value = get_moved0_columns(cr)
    if value:
        cr.execute(
            "INSERT INTO upgrade_test_data(key, value) VALUES (%s, %s)",
            [KEY, Json(value)],
        )
