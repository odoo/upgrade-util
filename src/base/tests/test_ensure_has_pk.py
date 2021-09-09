import logging

from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.testing import IntegrityCase

_logger = logging.getLogger("odoo.upgrade.base.tests.test_ensure_has_pk")


class TestTablesHavePK(IntegrityCase):
    def invariant(self):
        if not util.version_gte("14.0"):
            # Older versions generated m2m tables without PK
            return

        # Verify that all tables have a PK
        cr = self.env.cr
        query = """
            SELECT c.relname
              FROM pg_class c
              JOIN pg_namespace ns on ns.oid = c.relnamespace
         LEFT JOIN pg_constraint p on p.conrelid = c.oid and p.contype = 'p'
             WHERE c.relkind IN ('r', 'p')
               AND ns.nspname = current_schema
               AND p.oid IS NULL
          ORDER BY c.relname
        """

        cr.execute(query)
        if cr.rowcount:
            tables = "\n".join(" - %s" % t for t, in cr.fetchall())
            msg = "Some tables doesn't have any primary key:\n{}".format(tables)
            _logger.error(msg)  # trigger an error on runbot
            if util.on_CI():
                # and on upgradeci
                raise AssertionError(msg)
