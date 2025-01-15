# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    if util.version_gte("saas~18.2"):
        return
    util.env(cr)["res.groups"]._update_user_groups_view()
