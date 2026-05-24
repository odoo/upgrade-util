"""Preserve v17 cash-workflow / internal-transfer history on account.payment.

Example values stored:
    'reembolsado 2021-03-15'
    'transferencia interna 2023-06-10, reposicion caja chica 2023-06-10'
"""

import logging

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)

_OLD_MODULE = "account_payment_cash_custom_workflow"
_TABLE = "account_payment"
# Manual fields in Odoo must be named with the `x_` prefix, enforced by the
# `ir_model_fields_name_manual_field` CHECK constraint on ir_model_fields.
_FIELD = "x_legacy_cash_workflow_info"
_FIELD_LABEL = "Histórico Flujo Caja v17"

_ROLE_TOKENS = {
    "refunded": "reembolsado",
    "is_internal_transfer": "transferencia interna",
    "cash_replenishment": "reposicion caja chica",
}


def migrate(cr, version):
    present = [col for col in _ROLE_TOKENS if util.column_exists(cr, _TABLE, col)]
    if not present:
        _logger.info("No v17 role columns on %s; nothing to migrate.", _TABLE)
        return

    _logger.info("Migrating legacy markers from: %s", ", ".join(present))

    util.create_column(cr, _TABLE, _FIELD, "text")
    _register_manual_field(cr)
    _backfill_text(cr, present)

    if util.module_installed(cr, _OLD_MODULE):
        _logger.info("Uninstalling %s", _OLD_MODULE)
        util.uninstall_module(cr, _OLD_MODULE)


def _register_manual_field(cr):
    """Expose the column to the ORM as a manual text field on
    account.payment, so it shows up in *Add custom filter*.
    """
    # `field_description` is a translatable jsonb column in v19; store the
    # label under both en_US (Odoo's default fallback) and es_DO so the field
    # name is rendered correctly in both locales without a separate
    # translation pass.
    cr.execute(
        """
        INSERT INTO ir_model_fields
            (model_id, model, name, field_description, ttype,
             state, store, copied, readonly, required, "index")
        SELECT m.id, 'account.payment', %(name)s,
               jsonb_build_object('en_US', %(label)s, 'es_DO', %(label)s),
               'text', 'manual', TRUE, FALSE, TRUE, FALSE, FALSE
          FROM ir_model m
         WHERE m.model = 'account.payment'
           AND NOT EXISTS (
               SELECT 1 FROM ir_model_fields
                WHERE model = 'account.payment' AND name = %(name)s
           )
        """,
        {"name": _FIELD, "label": _FIELD_LABEL},
    )


def _backfill_text(cr, present):
    """Build and run one UPDATE that writes the concatenated tokens.

    For each column in `present`, a CASE/WHEN line emits
    '<token> <payment date>' when the flag is TRUE, otherwise NULL.
    `concat_ws` drops the NULLs and joins the rest with ', '.
    `RTRIM` strips the trailing space when the date is NULL (drafts
    without a move_id).
    """
    case_lines = ",\n".join(
        f"""CASE WHEN p.{col} = TRUE THEN
                RTRIM(concat(%({col})s || ' ',
                             COALESCE(to_char(j.move_date, 'YYYY-MM-DD'), '')))
            END"""
        for col in present
    )
    where_clause = " OR ".join(f"p.{col} = TRUE" for col in present)
    params = {col: _ROLE_TOKENS[col] for col in present}

    cr.execute(
        f"""
        UPDATE {_TABLE} p
           SET {_FIELD} = NULLIF(concat_ws(', ', {case_lines}), '')
          FROM (
              SELECT p2.id AS pid, m.date AS move_date
                FROM {_TABLE} p2
                LEFT JOIN account_move m ON m.id = p2.move_id
          ) AS j
         WHERE j.pid = p.id AND ({where_clause})
        """,
        params,
    )
    _logger.info("Backfilled %s.%s for %d row(s)", _TABLE, _FIELD, cr.rowcount)
