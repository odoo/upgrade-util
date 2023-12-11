# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    # The `commercial_partner_id` field is expected to always be set. Although the column is not marked as `NOT NULL`.
    # Fight the Murphy's Law, and recompute the value on partners with a NULL value.
    cr.execute("SELECT 1 FROM res_partner WHERE commercial_partner_id IS NULL LIMIT 1")
    if cr.rowcount:
        cr.execute(
            """
            WITH RECURSIVE partner AS (
                SELECT id as cid,
                        ARRAY[id] AS path
                  FROM res_partner
                 WHERE parent_id is null
                 GROUP BY parent_id,id

                 UNION ALL

                SELECT child.id as cid,
                        ARRAY_PREPEND(child.id,parent.path) AS path
                  FROM partner AS parent
                  JOIN res_partner child
                    ON child.parent_id = parent.cid
            )
            UPDATE res_partner rp
               SET commercial_partner_id = pat.path[array_length(path, 1)]
              FROM partner pat
             WHERE pat.cid = rp.id
               AND (
                    is_company = 'f' OR is_company IS NULL
                    )
            """
        )
        util.explode_execute(
            cr,
            """
            UPDATE res_partner
               SET commercial_partner_id = id
             WHERE is_company = 't'
            """,
            table="res_partner",
        )
