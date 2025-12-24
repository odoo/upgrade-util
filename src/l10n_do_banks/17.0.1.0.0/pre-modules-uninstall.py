from odoo.addons.base.maintenance.migrations import util
from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)


def _cleanup_qztray_data(cr):
    """Remove qztray printer records that block keypair deletion.

    Some databases have a NOT NULL constraint on ``qztray_printer.keypair_id``
    while the foreign key uses ``ON DELETE SET NULL``. When uninstalling the
    qztray modules, the upgrade utility tries to delete records from
    ``qztray_keypair``, which triggers ``SET NULL`` on ``qztray_printer`` and
    crashes on the NOT NULL constraint.

    We proactively delete the printer records that reference any keypair so the
    module uninstall can proceed.
    """
    # Solo ejecutamos esta limpieza si el módulo qztray_printer (o qztray)
    # está instalado en la base de datos.
    if not (
        util.module_installed(cr, "qztray_printer")
        or util.module_installed(cr, "qztray")
    ):
        _logger.debug(
            "qztray_printer/qztray no instalados; se omite la limpieza previa."
        )
        return

    try:
        cr.execute(
            """
                DELETE FROM qztray_printer
                WHERE keypair_id IN (SELECT id FROM qztray_keypair)
            """
        )
        _logger.info("Deleted qztray_printer records referencing qztray_keypair.")
    except Exception:
        # If tables don't exist or deletion fails, ignore and let normal
        # uninstall logic handle it.
        _logger.debug(
            "qztray_printer/qztray_keypair tables not found or deletion failed, "
            "continuing with module uninstall.",
            exc_info=True,
        )


def uninstall_modules(cr):
    """
    Script to uninstall modules that are no longer needed or compatible with version 17.0.

    Args:
        cr (cursor): Database cursor.
    """
    modules_to_uninstall = [
        'partner_phone_extension',
        'whatsapp_connector_send_stock',
        'whatsapp_connector_send_purchase',
        'whatsapp_connector_send_account',
        'whatsapp_connector_send_crm',
        'whatsapp_connector_template_base',
        'whatsapp_connector_admin',
        'account_multi_contact_followup',
        'account_payment_reconcile_features',
        'dgii_reports_enterprise',
        'res_partner_phone_search',
        'datamodel',
        'website_sale_require_login',
        'product_pricelist_direct_print',
        'web_m2x_options_manager',
        'l10n_do_electronic_stamp_amount',
        'neutralized_automated_action',
        'import_lot_serial_no',
        'unique_fields_support',
        'repair_financial_risk_features',
        'repair_financial_risk',
        'product_analytic',
        'sh_product_multi_barcode_mod',
        'sh_product_multi_barcode',
        'eq_send_payslip_email',
        'l10n_do_duplicate_fiscal_number',
        'l10n_do_hr_payroll_holidays',
        'odoo_cheque_bank_no_image',
        'whatsapp_connector_facebook',
        'whatsapp_connector_menu_analysis',
        'whatsapp_connector_menu_analysis_sale',
        'base_rest',
        'base_rest_auth_api_key',
        'base_rest_datamodel',
        'l10n_do_ecf_consult',
        'account_reconcile_exchange_difference',
        'account_followup_features',
        'l10n_do_account_followup',
        'setu_advance_inventory_reports',
        'account_state_report_kl',
        'sale_mrp_link',
        'bim',
        'component',
        'dgii_ir3',
        'hr_payroll_inputs_dynamic_tree',
        'l10n_do_check_print',
        'l10n_do_hr_expense_invoice',
        'ncf_invoice_template',
        'ncf_manager',
        'ncf_purchase',
        'sh_payslip_send_email',
        'incocegla_product_label_zebra_printer',
        'l10n_do_ecf_xsd_bypass',
        'l10n_do_ecf_document_type_enable',
        'l10n_do_ecf_certificate',
        'l10n_do_ecf_cashier',
        'sale_pos_backend_invoice_template',
        'l10n_do_partner',
        'web_gantt_view',
        'product_quantity_update_force_inventory',
        'account_move_line_auto_reconcile_hook',
        'pos_reprint',
        'l10n_do_purchase',
        'base_partner_fields',
        'stock_account_product_cost_security',
        'product_code_unique',
        'account_ecf_auto_post',
        'qztray_base',
        'professional_templates',
        'stock_available_unreserved',
        'product_warehouse_quantity',
        'sale_discount_limit',
        'sales_product_warehouse_quantity',
        'account_invoice_migration_scripts',
        'alan_customize',
        'config_interface',
        'database_cleanup',
        'dev_sale_product_stock_restrict',
        'interface_invoicing',
        'ncf_sale',
        'negative_stock_sale',
        'non_moving_product_ept',
        'payment_backend_refund',
        'product_hide_sale_cost_price',
        'protocol_message',
        'qztray',
        'qztray_base',
        'qztray_location_labels',
        'qztray_partner_labels',
        'qztray_product_inventory',
        'qztray_product_labels',
        'qztray_product_purchase',
        'required_requested_date',
        'stock_inventory_chatter'
        'cecomsa_account_followup',
    ]

    for module_name in modules_to_uninstall:
        if util.module_installed(cr, module_name):
            util.uninstall_module(cr, module_name)
            _logger.info(f'Successfuly module uninstalled {module_name}.')


def migrate(cr, version):
    _cleanup_qztray_data(cr)
    uninstall_modules(cr)
