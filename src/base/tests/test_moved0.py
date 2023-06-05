from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.testing import IntegrityCase

impl = util.import_script("base/0.0.0/pre-moved0.py")


class TestMoved0(IntegrityCase):
    key = impl.KEY
    message = "New `moved0` field. It happen when the ORM cannot change a column type by itself."

    def invariant(self):
        if util.version_gte("16.0"):
            # See https://github.com/odoo/odoo/commit/50767ef90eadeca2ed05b9400238af8bdbe77fb3
            self.skipTest("Starting Odoo 16, no more `moved0` columns are created")
            return

        return impl.get_moved0_columns(self.env.cr)
