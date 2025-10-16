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
                    _logger.warning(f"Error activating view {xml_id}: {e}")
                    # Try to delete the view if activation fails
                    # First check if there are any inherited views
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view.id)])
                    if inherited_views:
                        _logger.info(f"View {xml_id} has {len(inherited_views)} inherited views, attempting to delete them first")
                        # Use the improved delete function that handles inherited views properly
                        delete_database_views(cr, [xml_id])
                    else:
                        _logger.info(f"View {xml_id} has no inherited views, attempting to delete it")
                        delete_database_views(cr, [xml_id])
        except Exception as e:
            _logger.warning(f"Error finding view {xml_id}: {e}")

def delete_database_views(cr, xml_ids):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for xml_id in xml_ids:
        try:
            view = env.ref(xml_id)
            if view:
                # First, recursively find and delete all inherited views
                def delete_inherited_views_recursive(view_id):
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view_id)])
                    for inherited in inherited_views:
                        # Recursively delete any views that inherit from this inherited view
                        delete_inherited_views_recursive(inherited.id)
                        # Deactivate and delete the inherited view
                        try:
                            inherited.write({'active': False})
                            inherited.unlink()
                            _logger.info(f"Successfully deleted inherited view {inherited.xml_id}")
                        except Exception as e:
                            _logger.warning(f"Error deleting inherited view {inherited.xml_id}: {e}")
                
                # Delete all inherited views first
                delete_inherited_views_recursive(view.id)
                
                # Then delete the main view
                try:
                    view.write({'active': False})
                    view.unlink()
                    _logger.info(f"Successfully deleted view {xml_id}")
                except Exception as e:
                    _logger.warning(f"Error deleting main view {xml_id}: {e}")
        except Exception as e:
            _logger.warning(f"Error finding view {xml_id}: {e}")

def remove_inherited_views(cr, refs):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for ref in refs:
        try:
            view = env.ref(ref)
            if view:
                # Recursively find and delete all inherited views
                def delete_inherited_views_recursive(view_id):
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view_id)])
                    for inherited in inherited_views:
                        # Recursively delete any views that inherit from this inherited view
                        delete_inherited_views_recursive(inherited.id)
                        # Deactivate and delete the inherited view
                        try:
                            inherited.write({'active': False})
                            inherited.unlink()
                            _logger.info(f"Successfully deleted inherited view {inherited.xml_id}")
                        except Exception as e:
                            _logger.warning(f"Error deleting inherited view {inherited.xml_id}: {e}")
                
                # Delete all inherited views
                delete_inherited_views_recursive(view.id)
                _logger.info(f"Successfully removed inherited views for {ref}")
        except Exception as e:
            _logger.warning(f"Error removing inherited views for {ref}: {e}")

def migrate(cr, version):
    activate_views_list = [
        'account_auto_transfer_features.res_config_settings_view_form',
        'account_bank_charge_import_base.account_journal_dashboard_kanban_view_inherited',
        'account_invoice_rate.res_config_settings_view_form',
        'account_multi_journal_payment.res_config_settings_view_form',
        'account_partner_fields.res_partner_view_search_inherit',
        'account_partner_fields.view_move_form',
        'account_payment_authorization_code.res_config_settings_view_form',
        'account_payment_cash_custom_workflow.view_account_payment_form',
        'account_payment_compensation.account_compensation_batch_report_view_search',
        'account_payment_compensation.account_compensation_receipt_search',
        'account_payment_compensation.res_config_settings_view_form',
        'cecomsa_account_commission.cecomsa_payments_report_tree_view',
        'cecomsa_crm_feature.crm_lead_line_view_tree',
        'cecomsa_crm_feature.crm_lead_line_view_form',
        'cecomsa_crm_feature.crm_lead_view_form',
        'crm_helpdesk_custom.crm_helpdesk_crm_lead_view_form',
        'crm_helpdesk_custom.crm_helpdesk_custom_res_config_settings_view_form',
        'delivery_buenvio.res_config_settings_view_form',
        'helpdesk_sale_custom.helpdesk_sale_custom_res_config_settings_view_form',
        'l10n_do_account_batch_payment_base.l10n_do_account_batch_payment_wizard_form',
        'l10n_do_account_batch_payment_bpd.l10n_do_account_batch_bpd',
        'l10n_do_account_batch_payment_ee.l10n_do_account_batch_payment_wizard_form_inherited',
        'l10n_do_account_batch_payment_ee.view_batch_payment_form_inherited',
        'l10n_do_accounting.report_invoice_with_payments',
        'l10n_do_accounting.res_config_settings_view_form',
        'l10n_do_accounting.view_account_move_reversal_inherited',
        'l10n_do_accounting.view_move_form',
        'l10n_do_ecf_invoicing.view_move_form_inherited',
        'l10n_do_ecf_invoicing.res_config_settings_view_form',
        'l10n_do_hr.view_hr_job_form',
        'l10n_do_hr_payroll.hr_payslip_run_form',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_hr_payslip_form',
        'l10n_do_hr_recruitment.hr_applicant_view_form',
        'l10n_do_hr_recruitment.hr_job_survey',
        'l10n_do_report_cecomsa.report_invoice_with_payments',
        'l10n_do_rnc_validation.res_config_settings_view_form_inherited',
        'l10n_do_sale_pos_backend.res_config_settings_view_form_credit_note',
        'l10n_do_withholding_certification.res_config_settings_view_form',
        'odoo_cheque_features.inherit_view_move_form',
        'odoo_cheque_management.inherit_view_move_form',
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
        'sale_pos_backend.view_order_sales_pos_backend_form',
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
        'stock_landed_costs_file.view_move_form_inherited',
        'tss_report.tss_report_form_wizard',
        'tss_report.view_hr_payslip_form',
        'website_stock_availability.res_config_settings_view_form_inherit',
        'operating_unit.view_user_form',
        'sale_discount_display_amount.sale_order_view_form_display_discount',
    ]

    remove_inherit_views_list = [
        'bi_warranty_registration.warranty_case_claims_form_view1',
    ]
    delete_views_list = [
        'warranty_registration_extra_features.warranty_case_claims_form_view1_inherit',
        'studio_customization.odoo_studio_warranty_5e101e7c-e388-4589-b9b8-628b03ca43f1',
        'studio_customization.odoo_studio_warranty_64540003-0f78-4e15-8bba-6d3475418fed',
        'product_product_price_widget.product_product_tree_view_inherit_widget',
    ]

    activate_database_views(cr, activate_views_list)
    delete_database_views(cr, delete_views_list)
    remove_inherited_views(cr, remove_inherit_views_list)