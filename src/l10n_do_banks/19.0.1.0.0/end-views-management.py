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
