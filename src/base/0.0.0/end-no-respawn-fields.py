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
        # fmt:off
        [
            (model, field)
            for model, fields in util.ENVIRON["__renamed_fields"].items()
            for field, new_name in fields.items()
            if new_name is None  # means removed :p
        ],
        # fmt:on
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

    ignored_fields = set()

    for e in os.environ.get("suppress_upgrade_warnings", "").split(","):
        if e.startswith("field:"):
            ignored_fields.add(e[6:])

    for model, field, transient, store in cr.fetchall():
        qualifier = "field" if store else "non-stored field"
        if transient:
            qualifier = "transient " + qualifier
        lvl = util.NEARLYWARN if transient or not store else logging.CRITICAL

        if "{}.{}".format(model, field) in ignored_fields:
            ignore_fields |= field
            util._logger.log(
                util.NEARLYWARN, "Field respawn %s.%s explicitly ignored, skipping", field.model, field.name
            )
            lvl = util.NEARLYWARN

        _logger.log(lvl, "%s %s/%s has respawn!", qualifier, model, field)
