# -*- coding: utf-8 -*-
import collections

from .helpers import model_of_table, table_of_model
from .pg import SQLStr, column_exists, table_exists


class IndirectReference(
    collections.namedtuple(
        "IndirectReference", "table res_model res_id res_model_id set_unknown company_dependent_comodel"
    )
):
    def model_filter(self, prefix="", placeholder="%s"):
        if prefix and prefix[-1] != ".":
            prefix += "."
        if self.res_model_id:
            placeholder = "(SELECT id FROM ir_model WHERE model={})".format(placeholder)
            column = self.res_model_id
        else:
            column = self.res_model

        if column is None:
            # `model` is not set when `company_dependent_comodel` is.
            return SQLStr("(false AND {} IS NULL)".format(placeholder))

        return SQLStr('{}"{}"={}'.format(prefix, column, placeholder))


# By default, there is no `res_id`, no `res_model_id` and it is deleted when the linked model is removed
# warning: defaults are from the last fields in the namedtuple
IndirectReference.__new__.__defaults__ = (None, None, False, None)  # https://stackoverflow.com/a/18348004


def indirect_references(cr, bound_only=False):
    IR = IndirectReference
    each = [
        IR("ir_attachment", "res_model", "res_id"),
        IR("ir_cron", "model", None, set_unknown=True),
        IR("ir_act_report_xml", "model", None, set_unknown=True),
        IR("ir_act_window", "res_model", "res_id"),
        IR("ir_act_window", "res_model", None),
        IR("ir_act_window", "src_model", None),
        IR("ir_act_server", "wkf_model_name", None),
        IR("ir_act_server", "crud_model_name", None),
        IR("ir_act_server", "model_name", None, "model_id", set_unknown=True),
        IR("ir_act_client", "res_model", None, set_unknown=True),
        IR("ir_embedded_actions", "parent_res_model", "parent_res_id"),
        IR("ir_model", "model", None),
        IR("ir_model_fields", "model", None),
        IR("ir_model_fields", "relation", None),  # destination of a relation field
        IR("ir_model_data", "model", "res_id"),
        IR("ir_filters", "model_id", None, set_unknown=True),  # YUCK!, not an id
        # duplicated for versions where the `res_id` column does not exists
        IR("ir_filters", "model_id", "embedded_parent_res_id"),
        IR("ir_exports", "resource", None),
        IR("ir_ui_view", "model", None, set_unknown=True),
        IR("ir_values", "model", "res_id"),
        IR("wkf_transition", "trigger_model", None),
        IR("wkf_triggers", "model", None),
        IR("ir_model_fields_anonymization", "model_name", None),
        IR("ir_model_fields_anonymization_migration_fix", "model_name", None),
        IR("base_import_mapping", "res_model", None),
        IR("calendar_event", "res_model", "res_id"),  # new in saas~18
        IR("data_cleaning_model", "res_model_name", None),
        IR("data_cleaning_record", "res_model_name", "res_id"),
        IR("data_cleaning_rule", "res_model_name", None),
        IR("data_merge_group", "res_model_name", None),
        IR("data_merge_model", "res_model_name", None),
        IR("data_merge_record", "res_model_name", "res_id"),
        IR("documents_document", "res_model", "res_id"),
        IR("email_template", "model", None, set_unknown=True),  # stored related
        IR("iap_extracted_words", "res_model", "res_id"),
        IR("mail_template", "model", None, set_unknown=True),  # model renamed in saas~6
        IR("mail_activity", "res_model", "res_id", "res_model_id"),
        IR("mail_activity_type", "res_model", None),
        IR("mail_alias", None, "alias_force_thread_id", "alias_model_id"),
        IR("mail_alias", None, "alias_parent_thread_id", "alias_parent_model_id"),
        IR("mail_followers", "res_model", "res_id"),
        IR("mail_message_subtype", "res_model", None),
        IR("mail_message", "model", "res_id"),
        IR("mail_compose_message", "model", "res_id"),
        IR("mail_wizard_invite", "res_model", "res_id"),
        IR("mail_mail_statistics", "model", "res_id"),
        IR("mailing_trace", "model", "res_id"),
        IR("mail_mass_mailing", "mailing_model", None, "mailing_model_id", set_unknown=True),
        IR("mailing_mailing", None, None, "mailing_model_id", set_unknown=True),
        IR("marketing_campaign", "model_name", None, set_unknown=True),  # stored related
        IR("marketing_participant", "model_name", "res_id", "model_id", set_unknown=True),
        IR("payment_transaction", None, "callback_res_id", "callback_model_id"),
        IR("project_project", "alias_model", None, set_unknown=True),
        # IR("pos_blackbox_be_log", "model_name", None),  # ACTUALLY NOT. We need to keep records intact, even when renaming a model  # noqa: ERA001
        IR("quality_point", "worksheet_model_name", None),
        IR("rating_rating", "res_model", "res_id", "res_model_id"),
        IR("rating_rating", "parent_res_model", "parent_res_id", "parent_res_model_id"),
        IR("snailmail_letter", "model", "res_id", set_unknown=True),
        IR("sms_template", "model", None),
        IR("studio_approval_rule", "model_name", None),
        IR("spreadsheet_revision", "res_model", "res_id"),
        IR("studio_approval_entry", "model", "res_id"),
        IR("timer_timer", "res_model", "res_id"),
        IR("timer_timer", "parent_res_model", "parent_res_id"),
        IR("worksheet_template", "res_model", None),
    ]

    for ir in each:
        if bound_only and not ir.res_id:
            continue
        if ir.res_id and not column_exists(cr, ir.table, ir.res_id):
            continue

        # some `res_model/res_model_id` combination may change between
        # versions (i.e. rating_rating.res_model_id was added in saas~15).
        # we need to verify existence of columns before using them.
        if ir.res_model and not column_exists(cr, ir.table, ir.res_model):
            ir = ir._replace(res_model=None)  # noqa: PLW2901
        if ir.res_model_id and not column_exists(cr, ir.table, ir.res_model_id):
            ir = ir._replace(res_model_id=None)  # noqa: PLW2901
        if not ir.res_model and not ir.res_model_id:
            continue

        yield ir

    if column_exists(cr, "ir_model_fields", "company_dependent"):
        cr.execute(
            """
            SELECT model, name, relation
              FROM ir_model_fields
             WHERE company_dependent IS TRUE
               AND ttype = 'many2one'
            """,
        )
        for model_name, column_name, comodel_name in cr.fetchall():
            yield IR(table_of_model(cr, model_name), None, column_name, company_dependent_comodel=comodel_name)

    # XXX Once we will get the model field of `many2one_reference` fields in the database, we should get them also
    # (and filter the one already hardcoded)


def generate_indirect_reference_cleaning_queries(cr, ir):
    """Yield queries to clean an `IndirectReference`."""
    assert not ir.company_dependent_comodel  # not supported for now
    if ir.res_model:
        query = """
            SELECT {ir.res_model}
              FROM {ir.table}
             WHERE {ir.res_model} IS NOT NULL
          GROUP BY {ir.res_model}
        """
    else:
        query = """
            SELECT m.model
              FROM {ir.table} t
              JOIN ir_model m ON m.id = t.{ir.res_model_id}
          GROUP BY m.model
        """
    cr.execute(query.format(ir=ir))
    for (model,) in cr.fetchall():
        res_table = table_of_model(cr, model)
        if table_exists(cr, res_table):
            cond = "NOT EXISTS (SELECT 1 FROM {res_table} r WHERE r.id = t.{ir.res_id})".format(**locals())
        else:
            cond = "true"

        model_filter = ir.model_filter()
        yield cr.mogrify(
            "DELETE FROM {ir.table} t WHERE {model_filter} AND {cond}".format(**locals()), [model]
        ).decode()


def res_model_res_id(cr, filtered=True):
    for ir in indirect_references(cr):
        if ir.res_model:
            yield model_of_table(cr, ir.table), ir.res_model, ir.res_id
