import logging

_logger = logging.getLogger(__name__)


# Models whose ir_model_data records get unlocked before the upgrade runs.
# Clients often modify core records (via Studio or manual XML) which leaves
# them with noupdate=True. That blocks `-u all` from restoring them to the
# expected v19 state, and the upgrade explodes. Flipping noupdate=False here
# lets Odoo overwrite them cleanly so customizations on core records get
# replaced by the version they're supposed to have.
UNLOCK_MODELS = (
    'ir.ui.view',
    'ir.actions.act_window',
    'ir.actions.server',
    'ir.actions.report',
    'ir.ui.menu',
)


def migrate(cr, version):
    if not version:
        return

    cr.execute(
        """
        UPDATE ir_model_data
           SET noupdate = FALSE
         WHERE noupdate = TRUE
           AND model IN %s
        RETURNING module, model
        """,
        (UNLOCK_MODELS,),
    )
    rows = cr.fetchall()

    total = len(rows)
    _logger.info("Unlocked %s ir.model.data records (noupdate -> False)", total)

    if not total:
        return

    by_model = {}
    for _module, model in rows:
        by_model[model] = by_model.get(model, 0) + 1
    for model, count in sorted(by_model.items()):
        _logger.info("  %s: %s records", model, count)

    by_module = {}
    for module, _model in rows:
        by_module[module] = by_module.get(module, 0) + 1
    top = sorted(by_module.items(), key=lambda x: x[1], reverse=True)[:15]
    _logger.info("Top modules with unlocked records:")
    for module, count in top:
        _logger.info("  %s: %s", module, count)
