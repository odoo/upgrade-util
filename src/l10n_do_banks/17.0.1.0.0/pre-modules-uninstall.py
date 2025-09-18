from odoo.addons.base.maintenance.migrations import util
from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)


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
        'whatsapp_connector_send_sale',
        'whatsapp_connector_send_crm',
        'whatsapp_connector_template_base',
        'whatsapp_connector_admin',
        'account_multi_contact_followup',
        'account_payment_reconcile_features',
        'whatsapp_connector_chatter',
        'dgii_reports_enterprise',
        'res_partner_phone_search',
        'whatsapp_connector',
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
        'whatsapp_connector_sale',
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
    ]

    for module_name in modules_to_uninstall:
        if util.module_installed(cr, module_name):
            util.uninstall_module(cr, module_name)
            _logger.info(f'Successfuly module uninstalled {module_name}.')


def migrate(cr, version):
    uninstall_modules(cr)
