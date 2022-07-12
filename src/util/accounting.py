# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager

from .fields import remove_field
from .helpers import table_of_model
from .modules import module_installed
from .orm import env, invalidate
from .pg import create_column, explode_query_range, get_columns, parallel_execute
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)


@contextmanager
def no_fiscal_lock(cr):
    invalidate(env(cr)["res.company"])
    columns = [col for col in get_columns(cr, "res_company") if col.endswith("_lock_date")]
    assert columns
    set_val = ", ".join("{} = NULL".format(col) for col in columns)
    returns = ", ".join("old.{}".format(col) for col in columns)
    cr.execute(
        """
            UPDATE res_company c
               SET {}
              FROM res_company old
             WHERE old.id = c.id
         RETURNING {}, old.id
        """.format(
            set_val, returns
        )
    )
    data = cr.fetchall()
    yield
    set_val = ", ".join("{} = %s".format(col) for col in columns)
    cr.executemany(
        """
            UPDATE res_company
               SET {}
             WHERE id = %s
        """.format(
            set_val
        ),
        data,
    )


@contextmanager
def skip_failing_python_taxes(env, skipped=None):
    if module_installed(env.cr, "account_tax_python"):
        origin_compute_amount = env.registry["account.tax"]._compute_amount

        def _compute_amount(self, *args, **kwargs):
            if self.amount_type != "code":
                return origin_compute_amount(self, *args, **kwargs)
            try:
                return origin_compute_amount(self, *args, **kwargs)
            except ValueError as e:
                if skipped is not None:
                    skipped[self.id] = (self.name, e.args[0])
                return 0

        env.registry["account.tax"]._compute_amount = _compute_amount
        yield
        env.registry["account.tax"]._compute_amount = origin_compute_amount
    else:
        yield


def upgrade_tax_tags(cr, env, company, chart_template_id, upgraded_tax_xmlid=None):
    """
    This function updates the tags on the account.tax based on the templates
    It first constructs a mapping: account.tax.template.id -> account.tax.id, based on ir.model.data
    Then, it loops over the account.tax.template and adds the new tags to the matching account.tax.
    If no matching account.tax is found or if it was modified by the user, create a new clean tax instead.
    Upgraded_tax_xmlid is a list containing the xmlid of all the taxes expected at the end of the migration (see Note3).

    This helper has been tested successfully for Odoo 14.0.

    Note1: in the case where we update the tags on the existing tax, we add the new tags and keep the old ones.
    Note2: at the end of the migration, _process_end will call unlink on the old account.tax.report.line(s), which will then
        cause the removal of the tags only used by the report line being unlinked. Thus, the old tags will be removed, except if
        they are not attached to an old report line.
    Note3: if the account.tax.template is updated (e.g. removing a tax), this is not directly reflected in the ir.model.data table
        thus, to avoid recreating this tax here, we make sure its xmlid is in the upgraded_tax_xmlid list
    """
    template_to_tax = get_template_to_tax_mapping(cr, company)
    if not template_to_tax:
        return  # no mapping could be constructed

    ChartTemplate = env["account.chart.template"].with_context(default_company_id=company.id)

    for template in env["account.tax.template"].search([("chart_template_id", "=", chart_template_id)]):
        # see Note3
        _module, _, xmlid = template.get_xml_id()[template.id].partition(".")
        if upgraded_tax_xmlid and xmlid not in upgraded_tax_xmlid:
            _logger.warning(
                "The template tax '%s' is not present in 'upgraded_tax_xmlid' and is thus ignored by upgrade_tax_tags",
                template.name,
            )
            continue
        # if the tax template is not present in the mapping, it means the corresponding tax was removed from the db,
        # or no xmlid exists for the tax (in the case of old db) -> create a new tax
        if template.id not in template_to_tax:
            _logger.info("The tax '%s' does not exist, it will now be created as well as its xmlid", template.name)
            vals = template._get_tax_vals_complete(company)
            # 'create' is overriden in upgrade s.t. if a record already exists with the same fields, it is returned rather than
            # created. Renaming the existing, identical, taxes if they are linked to aml prevents this retrieving.
            domain = [
                ("name", "=", vals["name"]),
                ("type_tax_use", "=", vals["type_tax_use"]),
                ("company_id", "=", vals["company_id"]),
                ("tax_scope", "=", vals["tax_scope"]),
            ]
            for tax in env["account.tax"].search(domain):
                if used_by_amls(cr, tax):
                    tax.name += " (old)"
            ChartTemplate.create_record_with_xmlid(company, template, "account.tax", vals)
            continue

        tax = env["account.tax"].browse(template_to_tax[template.id])
        sort_line_func = lambda r: (
            r.repartition_type,
            r.factor_percent,
            "" if not r.account_id.code else r.account_id.code,
        )
        # The sort is handling the case when a user re-sequenced the repartition lines (to avoid comparing different lines)
        tax_rep_lines_sorted = tax.invoice_repartition_line_ids.sorted(
            key=sort_line_func
        ) + tax.refund_repartition_line_ids.sorted(key=sort_line_func)
        template_rep_lines_sorted = template.invoice_repartition_line_ids.sorted(
            key=sort_line_func
        ) + template.refund_repartition_line_ids.sorted(key=sort_line_func)
        if is_same_tax_and_template_lines(tax, template, tax_rep_lines_sorted, template_rep_lines_sorted):
            # Only update the tags: add the new ones, keep the existing ones
            _logger.info("Updating the tags for tax '%s' with id %d", tax.name, tax.id)
            for tax_line, template_line in zip(tax_rep_lines_sorted, template_rep_lines_sorted):
                tags_to_add = template_line._get_tags_to_add()
                tax_line.sudo().write({"tag_ids": [(4, tag.id) for tag in tags_to_add]})
        else:
            # Create a new tax based on the tax template and drop the xmlid of the old tax
            _logger.info("The xmlid of tax: '%s' will be removed and a new tax will be created", tax.name)
            if used_by_amls(cr, tax):
                tax.name += " (old)"
            xml_id = tax.get_xml_id().get(tax.id)
            if xml_id:
                remove_xml_id(cr, xml_id)
            vals = template._get_tax_vals_complete(company)
            ChartTemplate.create_record_with_xmlid(company, template, "account.tax", vals)

    add_to_migration_reports(
        message="Taxes and tags from company %r were upgraded based on the latest tax report available." % company.name,
        category="Accounting",
    )


def is_same_tax_and_template_lines(tax, template, tax_rep_lines_sorted, template_rep_lines_sorted):
    """
    This function compares an account.tax's and an account.tax.template's repartition lines
    (1) if they have a different number of repartition lines OR their factor_percent and repartition_type
    is different -> returns 'False'
    (2) if several lines exists with the same factor_percent and repartition_type, compare the account_id.code,
    if it is different -> returns 'False'
    (3) else -> returns 'True'

    Note for (2): for instance, this will handle the case when a tax has 2 repartition lines with repartition_type='tax'
    and factor_percent=50% but the account_id on these lines differ from the tax template.
    """
    if len(tax.invoice_repartition_line_ids) != len(template.invoice_repartition_line_ids) or len(
        tax.refund_repartition_line_ids
    ) != len(template.refund_repartition_line_ids):
        return False
    # the repartition.lines for the tax and the tax.template were sorted by repartition_type, factor_percent and account_id.code
    preceding_factor_percent = None
    preceding_repartition_type = None
    for rep_line, rep_line_template in zip(tax_rep_lines_sorted, template_rep_lines_sorted):
        if (rep_line.factor_percent != rep_line_template.factor_percent) or (
            rep_line.repartition_type != rep_line_template.repartition_type
        ):
            return False
        # the template code gives a prefix, template might be 1000 and account 100000
        if (
            (preceding_repartition_type == rep_line.repartition_type)
            and (preceding_factor_percent == rep_line.factor_percent)
            and (
                not rep_line.account_id
                or not rep_line_template.account_id
                or (
                    rep_line.account_id.code[: len(rep_line_template.account_id.code)]
                    != rep_line_template.account_id.code
                )
            )
        ):
            return False
        preceding_repartition_type = rep_line.repartition_type
        preceding_factor_percent = rep_line.factor_percent
    return True


def get_template_to_tax_mapping(cr, company):
    """
    This function uses the table ir_model_data to return a mapping between the tax templates and the taxes, using their xmlid
    :returns {
        account.tax.template.id: account.tax.id
        }
    """
    cr.execute(
        """
        SELECT tmplt.res_id AS tmplt_res_id,
               tax.res_id AS tax_res_id
          FROM ir_model_data tax
          JOIN ir_model_data tmplt
            ON tmplt.name = substr(tax.name, strpos(tax.name, '_') + 1)
         WHERE tax.model = 'account.tax'
           AND tax.name LIKE %s
         -- tax.name is of the form: {company_id}_{account.tax.template.name}
        """,
        [r"%s\_%%" % company.id],
    )
    tuples = cr.fetchall()
    template_to_tax = dict(tuples)
    return template_to_tax


def remove_xml_id(cr, xml_id):
    module, _, name = xml_id.partition(".")
    cr.execute(
        """
        DELETE
          FROM ir_model_data
         WHERE module = %s
           AND name = %s
    """,
        [module, name],
    )


def used_by_amls(cr, tax):
    cr.execute(
        """
        SELECT 1
          FROM account_move_line_account_tax_rel
         WHERE account_tax_id = %s
         LIMIT 1
    """,
        [tax.id],
    )
    return bool(cr.rowcount)


def upgrade_analytic_distribution(cr, model, tag_table=None, account_field=None, tag_field=None):
    table = table_of_model(cr, model)
    tag_table = tag_table or "account_analytic_tag_{table}_rel".format(table=table)
    account_field = account_field or "analytic_account_id"
    tag_field = tag_field or "analytic_tag_ids"
    query = """
        WITH _items AS (
            SELECT id, {account_field} FROM {table} item WHERE {{parallel_filter}}
        ),
        table_union AS (
            SELECT item.id AS line_id,
                   distribution.account_id AS account_id,
                   distribution.percentage AS percentage
              FROM _items item
              JOIN {tag_table} analytic_rel ON analytic_rel.{table}_id = item.id
              JOIN account_analytic_distribution distribution ON analytic_rel.account_analytic_tag_id = distribution.tag_id
            UNION ALL
            SELECT item.id AS line_id,
                   item.{account_field} AS account_id,
                   100 AS percentage
              FROM _items item
             WHERE item.{account_field} IS NOT NULL
        ),
        summed_union AS (
            SELECT line_id,
                   account_id,
                   SUM(percentage) AS percentage
              FROM table_union
          GROUP BY line_id, account_id
        ),
        distribution AS (
            SELECT line_id,
                   json_object_agg(account_id, percentage) AS distribution
              FROM summed_union
          GROUP BY line_id
        )
        UPDATE {table} item
           SET analytic_distribution = distribution.distribution
          FROM distribution
         WHERE item.id = distribution.line_id
    """.format(
        account_field=account_field,
        table=table,
        tag_table=tag_table,
    )

    create_column(cr, table, "analytic_distribution", "jsonb")
    parallel_execute(cr, explode_query_range(cr, query, table=table, alias="item"))
    remove_field(cr, model, account_field)
    remove_field(cr, model, tag_field)
