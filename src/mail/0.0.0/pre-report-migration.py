# -*- coding: utf-8 -*-
from openerp import api, models
from openerp.addons.base.maintenance.migrations import util


def migrate(cr, version):
    pass


class MailMessage(models.Model):
    _inherit = 'ir.ui.view'
    _module = 'mail'

    @api.model_cr
    def _register_hook(self):
        if len(util.migration_reports):
            util.announce_migration_report(self.env.cr)
        return super(MailMessage, self)._register_hook()
