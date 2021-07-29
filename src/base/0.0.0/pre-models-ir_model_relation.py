# -*- coding: utf-8 -*-
from odoo import api, models

try:
    from odoo.addons.base.models import ir_model as _ignore  # noqa
except ImportError:
    # version 10
    from odoo.addons.base.ir import ir_model as _ignore  # noqa


def migrate(cr, version):
    pass


class ModelRelation(models.Model):
    _inherit = "ir.model.relation"
    _module = "base"

    @api.model
    def _register_hook(self):
        super(ModelRelation, self)._register_hook()

        query = """
            DELETE FROM ir_model_relation WHERE id IN (
                SELECT r.id
                  FROM ir_model_relation r
                  JOIN ir_module_module m ON m.id = r.module
             LEFT JOIN information_schema.tables t ON t.table_name = r.name
                 WHERE m.state = 'installed'
                   AND t.table_name IS NULL
            )
        """

        self.env.cr.execute(query)
