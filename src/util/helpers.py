# -*- coding: utf-8 -*-
import logging

import lxml

from .exceptions import SleepyDeveloperError
from .misc import splitlines, version_gte

_logger = logging.getLogger(__name__.rpartition(".")[0])

# python3 shims
try:
    unicode
except NameError:
    unicode = str


def table_of_model(cr, model):
    exceptions = dict(
        line.split()
        for line in splitlines(
            """
        ir.actions.actions          ir_actions
        ir.actions.act_url          ir_act_url
        ir.actions.act_window       ir_act_window
        ir.actions.act_window_close ir_actions
        ir.actions.act_window.view  ir_act_window_view
        ir.actions.client           ir_act_client
        ir.actions.report.xml       ir_act_report_xml
        ir.actions.report           ir_act_report_xml
        ir.actions.server           ir_act_server
        ir.actions.wizard           ir_act_wizard

        stock.picking.in  stock_picking
        stock.picking.out stock_picking

        workflow            wkf
        workflow.activity   wkf_activity
        workflow.instance   wkf_instance
        workflow.transition wkf_transition
        workflow.triggers   wkf_triggers
        workflow.workitem   wkf_workitem

        # mass_mailing
        mail.mass_mailing.list_contact_rel mail_mass_mailing_contact_list_rel
        mailing.contact.subscription       mailing_contact_list_rel

        # `mail.notification` was a "normal" model in versions <9.0
        # and a named m2m in >=saas~13
        # and renamed as a "normal" model table in >= saas~14.3
        {gte_saas13_lte_saas14_3} mail.notification mail_message_res_partner_needaction_rel

        project.task.stage.personal project_task_user_rel
    """.format(
                gte_saas13_lte_saas14_3="" if version_gte("9.saas~13") and not version_gte("saas~14.3") else "#"
            )
        )
    )
    return exceptions.get(model, model.replace(".", "_"))


def model_of_table(cr, table):
    exceptions = dict(
        line.split()
        for line in splitlines(
            """
        # can also be act_window_close, but there are chances it wont be usefull for anyone...
        ir_actions         ir.actions.actions
        ir_act_url         ir.actions.act_url
        ir_act_window      ir.actions.act_window
        ir_act_window_view ir.actions.act_window.view
        ir_act_client      ir.actions.client
        ir_act_report_xml  {action_report_model}
        ir_act_server      ir.actions.server
        ir_act_wizard      ir.actions.wizard

        wkf            workflow
        wkf_activity   workflow.activity
        wkf_instance   workflow.instance
        wkf_transition workflow.transition
        wkf_triggers   workflow.triggers
        wkf_workitem   workflow.workitem

        ir_config_parameter  ir.config_parameter

        documents_request_wizard documents.request_wizard

        hr_payslip_worked_days hr.payslip.worked_days
        stock_package_level stock.package_level

        survey_user_input survey.user_input
        survey_user_input_line survey.user_input_line

        mail_mass_mailing_contact_list_rel mail.mass_mailing.list_contact_rel
        mailing_contact_list_rel           mailing.contact.subscription
        # Not a real model until saas~13
        {gte_saas13_lte_saas14_3} mail_message_res_partner_needaction_rel mail.notification

        data_merge_rule     data_merge.rule
        data_merge_model    data_merge.model
        data_merge_group    data_merge.group
        data_merge_record   data_merge.record

        project_task_user_rel project.task.stage.personal
    """.format(
                action_report_model="ir.actions.report" if version_gte("10.saas~17") else "ir.actions.report.xml",
                gte_saas13_lte_saas14_3="" if version_gte("9.saas~13") and not version_gte("saas~14.3") else "#",
            )
        )
    )
    return exceptions.get(table, table.replace("_", "."))


def _validate_model(model):
    exceptions = ["_unknown", "website_pricelist", "ir_actions_account_report_download"]
    if "_" in model and "." not in model and not model.startswith("x_") and model not in exceptions:
        raise SleepyDeveloperError("`{}` seems to be a table name instead of model name".format(model))
    return model


def _validate_table(table):
    if "." in table:
        raise SleepyDeveloperError("`{}` seems to be a model name instead of table name".format(table))
    return table


def _ir_values_value(cr):
    # returns the casting from bytea to text needed in saas~17 for column `value` of `ir_values`
    # returns tuple(column_read, cast_write)
    result = getattr(_ir_values_value, "result", None)

    if result is None:
        from .pg import column_type

        if column_type(cr, "ir_values", "value") == "bytea":
            cr.execute("SELECT character_set_name FROM information_schema.character_sets")
            (charset,) = cr.fetchone()
            column_read = "convert_from(value, '%s')" % charset
            cast_write = "convert_to(%%s, '%s')" % charset
        else:
            column_read = "value"
            cast_write = "%s"
        _ir_values_value.result = result = (column_read, cast_write)

    return result


def _dashboard_actions(cr, arch_match, *models):
    """Yield (dashboard_id, action) of dashboards that match `arch_match` and apply on `models` (if specified)"""

    q = """
        SELECT id, arch
          FROM ir_ui_view_custom
         WHERE arch ~ %s
    """
    cr.execute(q, [arch_match])
    for dash_id, arch in cr.fetchall():
        try:
            if isinstance(arch, unicode):
                arch = arch.encode("utf-8")
            dash = lxml.etree.fromstring(arch)
        except lxml.etree.XMLSyntaxError:
            _logger.error("Cannot parse dashboard %s", dash_id)
            continue
        for act in dash.xpath("//action"):
            if models:
                try:
                    act_id = int(act.get("name", "FAIL"))
                except ValueError:
                    continue

                cr.execute("SELECT res_model FROM ir_act_window WHERE id = %s", [act_id])
                [act_model] = cr.fetchone() or [None]
                if act_model not in models:
                    continue
            yield dash_id, act

        cr.execute(
            "UPDATE ir_ui_view_custom SET arch = %s WHERE id = %s",
            [lxml.etree.tostring(dash, encoding="unicode"), dash_id],
        )
