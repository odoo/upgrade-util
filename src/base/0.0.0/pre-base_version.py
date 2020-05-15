# -*- coding: utf-8 -*-
try:
    from odoo.addons.base.maintenance.migrations import util
except ImportError:
    # for symlinked versions
    from openerp.addons.base.maintenance.migrations import util


def migrate(cr, version):
    cr.execute("SELECT latest_version FROM ir_module_module WHERE name='base' AND state='to upgrade'")
    util.ENVIRON["__base_version"] = util.parse_version(cr.fetchone()[0])
