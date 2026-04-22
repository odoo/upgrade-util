import logging

from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)

_COMPENSATION_COLUMNS = [
    ("res_company", "compensation_currency_date_reference", "VARCHAR", "'invoice_date'"),
    ("res_company", "compensation_default_percentage_sale_base", "NUMERIC", "1"),
    ("res_company", "compensation_default_profit_percentage_sale_base", "NUMERIC", "1"),
]


def ensure_compensation_columns(cr):
    for table, column, col_type, default in _COMPENSATION_COLUMNS:
        if not util.column_exists(cr, table, column):
            _logger.info("Adding missing column %s.%s", table, column)
            cr.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {col_type} DEFAULT {default}"
            )


def uninstall_modules(cr):
    """Uninstall modules that are no longer needed or compatible with version 19.0."""
    modules_to_uninstall = [
        "l10n_do_hr_course",
        "l10n_do_hr_recurrent_news",
        "l10n_do_ecf_status_check",
        "l10n_do_sign_to_xml",
        "hr_course",
        "advanced_web_domain_widget",
        "hr_employee_relative",
        "account_payment_cash_custom_workflow",
        "looker_connector",
    ]

    _logger.info("Starting uninstall process for %d modules.", len(modules_to_uninstall))

    uninstalled_count = 0
    not_installed_count = 0
    failed_count = 0

    for module_name in modules_to_uninstall:
        try:
            if util.module_installed(cr, module_name):
                _logger.info("Attempting to uninstall module: %s", module_name)
                util.uninstall_module(cr, module_name)
                uninstalled_count += 1
                _logger.info("Successfully uninstalled module: %s.", module_name)
            else:
                not_installed_count += 1
                _logger.debug("Module %s is not installed, skipping.", module_name)
        except Exception:
            failed_count += 1
            _logger.error("Failed to uninstall module %s.", module_name, exc_info=True)

    _logger.info(
        "Uninstall process completed. Uninstalled: %d, Not installed: %d, Failed: %d",
        uninstalled_count,
        not_installed_count,
        failed_count,
    )

def migrate(cr, version):
    ensure_compensation_columns(cr)
    uninstall_modules(cr)