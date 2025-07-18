import logging
from odoo import api, SUPERUSER_ID
from odoo.upgrade import util

_logger = logging.getLogger(__name__)


def activate_database_views(cr, xml_ids):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for xml_id in xml_ids:
        try:
            view = env.ref(xml_id)
            if view:
                try:
                    view.write({'active': True})
                    _logger.info(f"Successfully activated view {xml_id}")
                except Exception as e:
                    _logger.info(f"Error activating view {xml_id}: {e}")
                    # Only delete view if it has no inherited views
                    if not env['ir.ui.view'].search([('inherit_id', '=', view.id)]):
                        delete_database_views(cr, [xml_id])
        except Exception as e:
            _logger.info(f"Error finding view {xml_id}: {e}")

def delete_database_views(cr, xml_ids):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for xml_id in xml_ids:
        try:
            view = env.ref(xml_id)
            if view:
                # First deactivate and delete any inherited views
                inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view.id)])
                for inherited in inherited_views:
                    inherited.write({'active': False})
                    inherited.unlink()
                # Then delete the main view
                view.unlink()
        except Exception as e:
            _logger.info(f"Error deleting view {xml_id}: {e}")


def migrate(cr, version):
    activate_views_list = [
        'account_auto_transfer_features.res_config_settings_view_form',
        'account_bank_charge_import_base.account_journal_dashboard_kanban_view_inherited',
        'account_invoice_rate.res_config_settings_view_form',
        'account_multi_journal_payment.res_config_settings_view_form',
        'account_partner_fields.res_partner_view_search_inherit',
        'account_partner_fields.view_move_form',
        'account_payment_authorization_code.res_config_settings_view_form',
        'account_payment_compensation.res_config_settings_view_form',
        'cecomsa_account_commission.cecomsa_payments_report_tree_view',
        'cecomsa_crm_feature.crm_lead_line_view_tree',
        'cecomsa_crm_feature.crm_lead_line_view_form',
        'cecomsa_crm_feature.crm_lead_view_form',
        'crm_helpdesk_custom.crm_helpdesk_crm_lead_view_form',
        'crm_helpdesk_custom.crm_helpdesk_custom_res_config_settings_view_form',
        'delivery_buenvio.res_config_settings_view_form',
        'helpdesk_sale_custom.helpdesk_sale_custom_res_config_settings_view_form',
        'l10n_do_accounting.view_account_move_reversal_inherited',
        'l10n_do_accounting.res_config_settings_view_form',
        'l10n_do_accounting.report_invoice_with_payments',
        'l10n_do_hr_payroll.hr_payslip_run_form',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_hr_payslip_form',
        'l10n_do_hr_recruitment.hr_applicant_view_form',
        'l10n_do_hr_recruitment.hr_job_survey',
        'l10n_do_report_cecomsa.report_invoice_with_payments',
        'l10n_do_rnc_validation.res_config_settings_view_form_inherited',
        'l10n_do_sale_pos_backend.res_config_settings_view_form_credit_note',
        'l10n_do_withholding_certification.res_config_settings_view_form',
        'payment_azul.acquirer_form_azul',
        'payment_bhd.payment_acquirer_form',
        'product_part_number.view_stock_move_line_operation_inherited',
        'purchase_foreign_cost_update.res_config_settings_view_form_inherited',
        'purchase_order_rate.res_config_settings_view_form_purchase',
        'purchase_request_features.res_config_settings_view_form',
        'recurring_sale_order_app_features.recurring_order_view_form',
        'repair_location_settings.res_config_settings_view_form_stock',
        'repair_warranty_extra_info.view_repair_order_form_inherit_features',
        'sale_pos_backend.sale_advance_payment_inv_cashier',
        'sale_pos_backend.view_sale_cashier_session_form',
        'sale_pos_backend.view_sale_cashier_form',
        'sale_pos_backend.view_sale_cashier_form_inherit',
        'sale_pos_backend.view_sale_cashier_authorization_code_form',
        'sale_pos_backend.view_sale_cashier_inherit_form',
        'sale_pos_backend.view_sale_cashier_rate_form',
        'sale_pos_backend_advance_payment.view_sale_cashier_session_inherit_form',
        'sale_pos_backend_advance_payment.view_sale_cashier_multi_journal_form',
        'sale_pos_backend_card_bin_promotion_payments.view_sale_cashier_authorization_code_form',
        'sale_pos_backend_journal_control.view_sale_cashier_session_journal_control_form',
        'sale_pos_backend_journal_control.view_sale_cashier_inherit_form',
        'sale_pos_backend_multi_journal_payment.view_sale_cashier_multi_journal_form',
        'sale_pos_session_link.view_sale_cashier_session_inherit_form',
        'sale_pos_session_link.view_sale_cashier_session_return_form',
        'sale_pos_session_link.view_sale_cashier_inherit_form',
        'sale_stock_qty_date_widgets.sale_stock_qty_date_widgets_view',
        'sale_stock_product_price_widget.sale_price_widgets_view',
        'sale_stock_restriction.res_config_settings_view_form_inherit_sale_management',
        'stock_inventory_forecasted_report.res_config_settings_view_form',
        'stock_landed_costs_features.stock_landed_cost_run_form',
        'stock_landed_costs_file.stock_landed_cost_run_form_inherited',
        'tss_report.tss_report_form_wizard',
        'tss_report.view_hr_payslip_form',
        'website_stock_availability.res_config_settings_view_form_inherit',
        'bi_warranty_registration.warranty_case_claims_form_view1',
    ]

    activate_database_views(cr, activate_views_list)