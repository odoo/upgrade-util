import logging
from odoo import api, SUPERUSER_ID
from odoo.upgrade import util

_logger = logging.getLogger(__name__)


def delete_deprecated_views(cr, xml_ids):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for xml_id in xml_ids:
        try:
            view = env.ref(xml_id)
            if view:
                try:
                    view.write({'active': False})
                    view.unlink()
                    _logger.info(f"Successfully deleted deprecated view {xml_id}")
                except Exception as e:
                    _logger.warning(f"Error deleting deprecated view {xml_id}: {e}")
        except Exception as e:
            _logger.warning(f"Error finding deprecated view {xml_id}: {e}")


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
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view.id)])
                    if inherited_views:
                        _logger.info(f"View {xml_id} has {len(inherited_views)} inherited views, attempting to delete them first")
                        delete_database_views(cr, [xml_id])
                    else:
                        _logger.info(f"View {xml_id} has no inherited views, attempting to delete it")
                        delete_database_views(cr, [xml_id])
        except Exception as e:
            _logger.warning(f"Error finding view {xml_id}: {e}")


def delete_database_views(cr, refs):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for ref in refs:
        try:
            view = env.ref(ref)
            if view:
                def delete_inherited_views_recursive(view_id):
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view_id)])
                    for inherited in inherited_views:
                        delete_inherited_views_recursive(inherited.id)
                        try:
                            inherited.write({'active': False})
                            inherited.unlink()
                            _logger.info(f"Successfully deleted inherited view {inherited.xml_id}")
                        except Exception as e:
                            _logger.warning(f"Error deleting inherited view {inherited.xml_id}: {e}")

                delete_inherited_views_recursive(view.id)

                try:
                    view.write({'active': False})
                    view.unlink()
                    _logger.info(f"Successfully deleted view {ref}")
                except Exception as e:
                    _logger.warning(f"Error deleting main view {ref}: {e}")
        except Exception as e:
            _logger.warning(f"Error finding view {ref}: {e}")


def remove_inherited_views(cr, refs):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for ref in refs:
        try:
            view = env.ref(ref)
            if view:
                def delete_inherited_views_recursive(view_id):
                    inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view_id)])
                    for inherited in inherited_views:
                        delete_inherited_views_recursive(inherited.id)
                        try:
                            inherited.write({'active': False})
                            inherited.unlink()
                            _logger.info(f"Successfully deleted inherited view {inherited.xml_id}")
                        except Exception as e:
                            _logger.warning(f"Error deleting inherited view {inherited.xml_id}: {e}")

                delete_inherited_views_recursive(view.id)
                _logger.info(f"Successfully removed inherited views for {ref}")
        except Exception as e:
            _logger.warning(f"Error removing inherited views for {ref}: {e}")


def migrate(cr, version):
    activate_views_list = [
        'account_payment_compensation.account_compensation_batch_report_view_search',
        'account_payment_compensation.account_compensation_receipt_search',
        'account_bank_charge_import_base.account_journal_dashboard_kanban_view_inherited',
        'l10n_do_hr_recruitment.l10n_do_hr_vacancy_application_view_form',
        'tss_report.tss_report_form_wizard',
        'tss_report.view_hr_payslip_form',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_hr_payslip_form',
        'l10n_do_hr_payroll.hr_payslip_run_form',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_view_employee_form',
        'l10n_do_account_batch_payment_base.l10n_do_account_batch_payment_wizard_form',
        'l10n_do_account_batch_payment_bpd.l10n_do_account_batch_bpd',
        'l10n_do_account_batch_payment_ee.l10n_do_account_batch_payment_wizard_form_inherited',
        'l10n_do_accounting.view_account_move_reversal_inherited',
        'l10n_do_accounting.view_move_form',
        'l10n_do_accounting.external_layout_striped_inherited',
        'l10n_do_accounting.report_invoice_document_inherited',
        'l10n_do_withholding_certification.view_account_form_inherited',
        'l10n_do_hr.view_employee_form',
        'l10n_do_hr.view_hr_job_form',
        'l10n_do_ecf_invoicing.view_move_form_inherited',
        'l10n_do_document_pools.view_move_form_inherited',
        'sale_order_time_total.view_order_form_inherited',
    ]

    activate_database_views(cr, activate_views_list)

    # Views present in v17 but removed in v19.
    # Modules marked [DELETED] no longer exist in v19 — views may still hang if
    # the module was dropped from the addons path without a formal uninstall.
    deprecated_views_list = [
        # account_auto_transfer_features [DELETED in v19]
        'account_auto_transfer_features.res_config_settings_view_form',
        'account_auto_transfer_features.view_transfer_model_form',
        # account_followup_extra_features
        'account_followup_extra_features.customer_statements_form_view_inherit',
        # account_followup_multi_partner [DELETED in v19]
        'account_followup_multi_partner.followup_multi_partner_line_tree_view',
        'account_followup_multi_partner.followup_multi_partner_wizard_form_view',
        # account_invoice_rate [DELETED in v19]
        'account_invoice_rate.res_config_settings_view_form',
        'account_invoice_rate.view_move_form_inherited',
        # account_payment_authorization_code
        'account_payment_authorization_code.view_account_journal_form',
        # account_payment_compensation_pos [DELETED in v19]
        'account_payment_compensation_pos.account_compensation_receipt_form_pos',
        'account_payment_compensation_pos.account_compensation_receipt_line_tree_pos',
        # account_reconcile_payment [DELETED in v19]
        'account_reconcile_payment.view_account_journal_form_inherited',
        'account_reconcile_payment.view_account_payment_form_multi_inherited',
        'account_reconcile_payment.view_tax_form_inherited',
        # auto_backup_sh
        'auto_backup_sh.view_backup_sh_config_tree',
        # crm_helpdesk_custom
        'crm_helpdesk_custom.helpdesk_ticket_crm_lead_view_form_helpdesk_invoicing',
        # dgii_reports
        'dgii_reports.dgii_cancel_report_line_tree',
        'dgii_reports.dgii_exterior_report_line_tree',
        'dgii_reports.dgii_report_purchase_line_tree',
        'dgii_reports.dgii_report_sale_line_tree',
        'dgii_reports.dgii_report_tree_view',
        'dgii_reports.invoice_tree_inherited',
        # fleet_product_rules
        'fleet_product_rules.fleet_vehicle_state_view_form',
        'fleet_product_rules.fleet_vehicle_state_view_tree_inherit',
        'fleet_product_rules.product_normal_form_view_inherit',
        # fleet_product_rules_renting [DELETED in v19]
        'fleet_product_rules_renting.product_normal_form_view_inherit',
        'fleet_product_rules_renting.product_template_form_view_rental_inherit',
        # helpdesk_sale_custom
        'helpdesk_sale_custom.helpdesk_ticket_sale_order_view_form',
        # l10n_do_accounting
        'l10n_do_accounting.view_account_move_filter',
        'l10n_do_accounting.view_invoice_tree',
        # l10n_do_ecf_invoicing
        'l10n_do_ecf_invoicing.l10n_do_account_journal_document_type_view_tree',
        'l10n_do_ecf_invoicing.view_invoice_tree',
        # l10n_do_ecf_status_check [DELETED in v19]
        'l10n_do_ecf_status_check.view_move_form_inherited_status_check',
        # l10n_do_hr
        'l10n_do_hr.hr_employee_relative_view_tree',
        'l10n_do_hr.l10n_do_hr_employee_study_field_view_tree',
        'l10n_do_hr.l10n_do_hr_employee_visa_type_view_tree',
        'l10n_do_hr.l10n_do_hr_employee_visa_view_tree',
        'l10n_do_hr.l10n_do_illness_allergy_view_tree',
        # l10n_do_hr_bonus_legal [DELETED in v19]
        'l10n_do_hr_bonus_legal.hr_bonus_legal_view_form',
        'l10n_do_hr_bonus_legal.hr_bonus_legal_view_tree',
        # l10n_do_hr_expense
        'l10n_do_hr_expense.view_hr_expense_tree_l10n_do',
        # l10n_do_hr_news
        'l10n_do_hr_news.l10n_do_hr_news_type_tree',
        'l10n_do_hr_news.l10n_do_hr_news_view_tree',
        # l10n_do_hr_payroll
        'l10n_do_hr_payroll.l10n_do_hr_payroll_days_division_form',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_days_division_tree',
        'l10n_do_hr_payroll.l10n_do_hr_payroll_hr_contract_view_form',
        'l10n_do_hr_payroll.l10n_do_hr_quotation_calculation_tree',
        # l10n_do_hr_payroll_news
        'l10n_do_hr_payroll_news.l10n_do_hr_news_type_form',
        'l10n_do_hr_payroll_news.l10n_do_hr_news_type_tree',
        'l10n_do_hr_payroll_news.l10n_do_hr_news_view_form',
        # l10n_do_hr_recruitment
        'l10n_do_hr_recruitment.l10n_do_hr_vacancy_application_view_tree',
        # l10n_do_payroll_file_base
        'l10n_do_payroll_file_base.hr_payslip_run_form_inherited',
        # l10n_do_pos_features [DELETED in v19]
        # (no views found in diff — module deleted cleanly)
        # payment_azul [DELETED in v19]
        'payment_azul.payment_provider_form',
        # payment_salesperson
        'payment_salesperson.view_account_payment_tree_inh',
        # payroll_dynamic_xls_report [DELETED in v19]
        'payroll_dynamic_xls_report.view_hr_salary_report',
        'payroll_dynamic_xls_report.view_hr_salary_report_tree',
        # pos_cardnet [DELETED in v19]
        'pos_cardnet.pos_payment_method_view_form_inherit_cardnet',
        # pos_hr_minimal_rights [DELETED in v19]
        'pos_hr_minimal_rights.pos_config_form_view_inherit_minimal',
        'pos_hr_minimal_rights.res_config_settings_view_form_minimal',
        # product_foreign_cost_price
        'product_foreign_cost_price.product_product_form_view_inherited',
        # product_label_layout [DELETED in v19]
        'product_label_layout.product_template_form_view_inherited',
        # product_price_history
        'product_price_history.product_listprice_history_tree',
        # purchase_order_rate
        'purchase_order_rate.res_config_settings_view_form_purchase',
        # recurring_sale_order_app [DELETED in v19]
        'recurring_sale_order_app.recurring_order_view_filter',
        'recurring_sale_order_app.recurring_order_view_form',
        'recurring_sale_order_app.recurring_order_view_tree',
        'recurring_sale_order_app.sale_recurring_view',
        # recurring_sale_order_app_features [DELETED in v19]
        'recurring_sale_order_app_features.recurring_order_view_form',
        # repair_no_negative_allow
        'repair_no_negative_allow.repair_order_form_view_inherit',
        # repair_services
        'repair_services.view_repair_service_line_tree',
        # sale_order_rate
        'sale_order_rate.res_config_settings_view_form',
        # sale_stock_features [DELETED in v19]
        'sale_stock_features.view_picking_form_salesperson',
        'sale_stock_features.view_picking_search_salesperson',
        # sale_subscription_draft_invoice [DELETED in v19]
        'sale_subscription_draft_invoice.sale_subscription_plan_view_form_inherited',
        # serial_number_report [DELETED in v19]
        'serial_number_report.product_normal_form_view_inherit_serial_report',
        'serial_number_report.product_template_form_view_inherit_serial_report',
        'serial_number_report.res_config_settings_view_form_inherit',
        # stock_landed_costs_features
        'stock_landed_costs_features.stock_landed_cost_run_tree',
        'stock_landed_costs_features.stock_valuation_adjustment_lines_tree',
        'stock_landed_costs_features.stock_valuation_adjustment_summary_tree',
        # stock_landed_costs_file
        'stock_landed_costs_file.stock_landed_costs_file_tree',
        # website_currency_convertion [DELETED in v19]
        'website_currency_convertion.res_config_settings_view_form_inherit_currency_conversion',
        # website_store_pickup [DELETED in v19]
        'website_store_pickup.sale_order_form_view_inherit',
        'website_store_pickup.stock_route_form_view_inherited',
    ]

    delete_deprecated_views(cr, deprecated_views_list)
