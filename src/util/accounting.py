# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager

from .fields import remove_field
from .helpers import table_of_model
from .modules import module_installed
from .orm import env, invalidate
from .pg import create_column, explode_query_range, get_columns, parallel_execute

_logger = logging.getLogger(__name__)


@contextmanager
def no_deprecated_accounts(cr):
    cr.execute(
        """
        UPDATE account_account
           SET deprecated = false
         WHERE deprecated = true
     RETURNING id
        """
    )
    ids = tuple(r for (r,) in cr.fetchall())
    yield
    if ids:
        cr.execute(
            """
            UPDATE account_account
               SET deprecated = true
             WHERE id IN %s
            """,
            [ids],
        )


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
        """.format(set_val, returns)
    )
    data = cr.fetchall()
    yield
    set_val = ", ".join("{} = %s".format(col) for col in columns)
    cr.executemany(
        """
            UPDATE res_company
               SET {}
             WHERE id = %s
        """.format(set_val),
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
