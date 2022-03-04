# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.util.modules import _trigger_auto_discovery


def migrate(cr, version):
    if util.version_gte("saas~14.5"):
        _trigger_auto_discovery(cr)
