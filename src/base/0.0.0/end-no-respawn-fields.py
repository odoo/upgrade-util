# -*- coding: utf-8 -*-
import logging
import os

from psycopg2.extras import execute_values

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger("odoo.addons.base.maintenance.migrations.base.000.no_respawn")


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
            for field, new_name in fields.items()
            if new_name is None  # means removed :p
        ],
    )
    cr.execute(
        """
        SELECT m.model, f.name, m.transient, f.store
          FROM ir_model_fields f
          JOIN ir_model m ON m.id = f.model_id
          JOIN no_respawn r ON (m.model = r.model AND f.name = r.field)
      ORDER BY m.model, f.name
    """
    )

    key = "field_respawn:"
    ignored_fields_respawn = {
        e[len(key) :]
        for e in os.environ.get("suppress_upgrade_warnings", "").split(",")  # noqa: SIM112
        if e.startswith(key)
    }

    for model, field, transient, store in cr.fetchall():
        qualifier = "field" if store else "non-stored field"
        if transient:
            qualifier = "transient " + qualifier
        lvl = util.NEARLYWARN if transient or not store else logging.CRITICAL
        action = ""

        if "{}/{}".format(model, field) in ignored_fields_respawn:
            lvl = util.NEARLYWARN
            action = "; explicitly ignored"

        _logger.log(lvl, "%s %s/%s has respawn%s.", qualifier, model, field, action)
