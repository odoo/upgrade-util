# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    if not util.ENVIRON.get("manual_moved0"):
        # let the test verify the invariant.
        return

    pre = util.import_script("base/0.0.0/pre-moved0.py")

    cr.execute("SELECT value FROM upgrade_test_data WHERE key = %s", [pre.KEY])
    expected = [tuple(i) for i in cr.fetchone()[0]] if cr.rowcount else []
    moved_fields = set(pre.get_moved0_columns(cr)) - set(expected)
    if moved_fields:
        raise util.UpgradeError(
            "New `moved0` field. It happen when the ORM cannot change a column type by itself.\n%s"
            % "\n".join("\t- %s.%s" % m for m in sorted(moved_fields))
        )
