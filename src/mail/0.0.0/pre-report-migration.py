# -*- coding: utf-8 -*-
from odoo import models

try:
    from odoo.api import model_cr
except ImportError:
    # v13 shim
    def model_cr(f):
        return f


try:
    from odoo.addons.mail.models.mail_message import Message  # noqa
except ImportError:
    from odoo.addons.mail.models.mail_message import MailMessage

from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    pass


class MailMessage(models.Model):
    _inherit = "mail.message"
    _module = "mail"

    @model_cr
    def _register_hook(self):
        util.announce_release_note(self.env.cr)
        if len(util.migration_reports):
            util.announce_migration_report(self.env.cr)
        return super(MailMessage, self)._register_hook()
