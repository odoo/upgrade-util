# -*- coding: utf-8 -*-
import logging
from psycopg2.extras import execute_values

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger("odoo.addons.base.maintenance.base.000.no_respawn")


def migrate(cr, version):
    # Ensure that we didn't `remove_field` that shouldnt'
    cr.execute(
        """
        CREATE TEMPORARY TABLE no_respawn(
            model varchar,
            field varchar
        )
    """
    )
    execute_values(
        cr._obj,
        "INSERT INTO no_respawn(model, field) VALUES %s",
        [
            (model, field)
            for model, fields in util.ENVIRON["__renamed_fields"].items()
            for field in fields
        ],
    )
    cr.execute(
        """
        SELECT m.model, f.name
          FROM ir_model_fields f
          JOIN ir_model m ON m.id = f.model_id
          JOIN no_respawn r ON (m.model = r.model AND f.name = r.field)
    """
    )
    for model, field in cr.fetchall():
        _logger.error("field %s.%s has respawn!", model, field)

    if cr.rowcount:
        raise util.SleepyDeveloperError("Damn!")
