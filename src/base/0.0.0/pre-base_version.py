# -*- coding: utf-8 -*-
try:
    from odoo.addons.base.maintenance.migrations import util
except ImportError:
    # for symlinked versions
    from openerp.addons.base.maintenance.migrations import util


def migrate(cr, version):
    util._get_base_version(cr)
