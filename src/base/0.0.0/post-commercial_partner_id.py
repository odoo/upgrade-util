# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util

_logger = util.getLogger(__name__)


def migrate(cr, version):
    cr.execute("SELECT 1 FROM res_partner WHERE commercial_partner_id IS NULL LIMIT 1")
    if cr.rowcount:
        _migrate(cr)


def _migrate(cr):
    # The `commercial_partner_id` field is expected to always be set. Although the column is not marked as `NOT NULL`.
    # Fight the Murphy's Law, and recompute the value on partners with a NULL value.
    query = """
        WITH RECURSIVE top_level_candidates AS (
            SELECT id,
                   commercial_partner_id IS NULL AS is_candidate
              FROM res_partner
             WHERE parent_id IS NULL

             UNION ALL

            SELECT c.id,
                   c.commercial_partner_id IS NULL
              FROM top_level_candidates p
              JOIN res_partner c
                ON c.parent_id = p.id
             WHERE NOT p.is_candidate
        ), top_level_candidates_subtrees AS (
                -- every node is in its own subtree
            SELECT id AS root_id,
                   id
              FROM top_level_candidates
             WHERE is_candidate
             UNION ALL
                -- add nodes to subtree
            SELECT p.root_id,
                   c.id
              FROM top_level_candidates_subtrees p
              JOIN res_partner c
                ON c.parent_id = p.id
        )
        SELECT root_id
          FROM top_level_candidates_subtrees
      GROUP BY root_id
        HAVING COUNT(*) > 10000
    """
    _logger.info("Computing `commercial_partner_id` for big entities")
    util.recompute_fields(
        cr,
        "res.partner",
        ["commercial_partner_id"],
        query=query,
        chunk_size=1,
        strategy="commit",
    )
    _logger.info("Computing `commercial_partner_id` for the rest")
    util.recompute_fields(
        cr,
        "res.partner",
        ["commercial_partner_id"],
        query="SELECT id FROM res_partner WHERE commercial_partner_id IS NULL",
        chunk_size=10000,
        strategy="commit",
    )
