from odoo.addons.base.maintenance.migrations import util


@util.once("10.0", "20.0")
def migrate(cr, version):
    # The `commercial_partner_id` field is expected to always be set. Although the column is not marked as `NOT NULL`.
    # Fight the Murphy's Law, and recompute the value on partners with a NULL value.
    """Ref: https://github.com/odoo/odoo/blob/4cb3090ee829084c2a9260b6d38095cd818678f5/odoo/addons/base/models/res_partner.py#L515
        if partner.is_company or not partner.parent_id:
           partner.commercial_partner_id = partner
        else:
            partner.commercial_partner_id = partner.parent_id.commercial_partner_id
    The `is_company` check is gone as of saas~19.1, see odoo/odoo@a0ee79e6."""
    query = util.format_query(
        cr,
        """
        WITH RECURSIVE _commercial_partners_to_set AS (
              SELECT p.id,
                     CASE WHEN p.parent_id IS NULL {0} THEN p.id
                          ELSE p.parent_id
                     END AS target_id,
                     p.parent_id IS NULL {0} AS found
                FROM res_partner p
               WHERE p.commercial_partner_id IS NULL
                 AND {{parallel_filter}}

           UNION ALL

              SELECT c.id,
                     CASE WHEN p.commercial_partner_id IS NOT NULL
                          THEN p.commercial_partner_id
                          WHEN p.parent_id IS NULL {0} THEN p.id
                          ELSE p.parent_id
                     END AS target_id,
                     p.commercial_partner_id IS NOT NULL OR p.parent_id IS NULL {0} AS found
                FROM _commercial_partners_to_set c
                JOIN res_partner p
                  ON p.id = c.target_id
               WHERE NOT c.found
        )
        UPDATE res_partner p
           SET commercial_partner_id = c.target_id
          FROM _commercial_partners_to_set c
         WHERE c.id = p.id
           AND c.found
        """,
        util.SQLStr("" if util.version_gte("saas~19.1") else "OR p.is_company IS TRUE"),
    )
    util.explode_execute(cr, query, table="res_partner", alias="p")
