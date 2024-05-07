# -*- coding: utf-8 -*-
import logging
import os
from textwrap import dedent

from psycopg2.extensions import quote_ident
from psycopg2.extras import Json

from odoo.tools.misc import str2bool

from .helpers import _validate_model, table_of_model
from .misc import chunks
from .pg import get_value_or_en_translation
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)

INCLUDE_ARCHIVED_PRODUCTS = str2bool(
    os.environ.get("ODOO_MIG_DO_NOT_IGNORE_ARCHIVED_PRODUCTS_FOR_UOM_INCONSISTENCIES"),
    default=False,
)
FIX_PRODUCT_UOM = str2bool(os.environ.get("ODOO_MIG_FIX_ALL_UOM_INCONSISTENCIES"), default=False)


def verify_companies(
    cr, model, field_name, logger=_logger, model_company_field="company_id", comodel_company_field="company_id"
):
    _validate_model(model)
    cr.execute(
        """
            SELECT ttype, relation, relation_table, column1, column2
              FROM ir_model_fields
             WHERE name = %s
               AND model = %s
               AND store IS TRUE
               AND ttype IN ('many2one', 'many2many')
    """,
        [field_name, model],
    )

    field_values = cr.dictfetchone()

    if not field_values:
        _logger.warning("Field %s not found on model %s.", field_name, model)
        return

    table = table_of_model(cr, model)
    comodel = field_values["relation"]
    cotable = table_of_model(cr, comodel)

    limit = 15

    if field_values["ttype"] == "many2one":
        query = """
            SELECT a.id, a.{model_company_field}, b.id, b.{comodel_company_field}, count(*) OVER ()
              FROM {table} a
              JOIN {cotable} b ON b.id = a.{field_name}
             WHERE a.{model_company_field} IS NOT NULL
               AND b.{comodel_company_field} IS NOT NULL
               AND a.{model_company_field} != b.{comodel_company_field}
             LIMIT {limit}
        """.format(**locals())
    else:  # many2many
        m2m_relation = field_values["relation_table"]
        f1, f2 = field_values["column1"], field_values["column2"]
        query = """
            SELECT a.id, a.{model_company_field}, b.id, b.{comodel_company_field}, count(*) OVER ()
              FROM {m2m_relation} m
              JOIN {table} a ON a.id = m.{f1}
              JOIN {cotable} b ON b.id = m.{f2}
             WHERE a.{model_company_field} IS NOT NULL
               AND b.{comodel_company_field} IS NOT NULL
               AND a.{model_company_field} != b.{comodel_company_field}
             LIMIT {limit}
        """.format(**locals())

    cr.execute(query)
    if cr.rowcount:
        logger.warning(
            "Company field %s/%s is not consistent with %s/%s for %d records (through %s relation %s)",
            model,
            model_company_field,
            comodel,
            comodel_company_field,
            cr.rowcount,
            field_values["ttype"],
            field_name,
        )

        bad_rows = cr.fetchall()
        total = bad_rows[-1][-1]
        lis = "\n".join("<li>record #%s (company=%s) -&gt; record #%s (company=%s)</li>" % bad[:-1] for bad in bad_rows)

        add_to_migration_reports(
            message="""\
            <details>
              <summary>
                Some inconsistencies have been found on field {model}/{field_name} ({total} records affected; show top {limit})
              </summary>
              <ul>
                {lis}
              </ul>
            </details>
        """.format(**locals()),
            category="Multi-company inconsistencies",
            format="html",
        )


def verify_uoms(cr, model, uom_field="product_uom_id", product_field="product_id", ids=None):
    """
    Check if the category of uom  on `model` is the same as the category of uom on `product.template`.

    When `ids` is not provided, every ids would be verified.

    Returns list of ids if inconsistencies found, else []
    """
    _validate_model(model)
    table = table_of_model(cr, model)

    q = lambda s: quote_ident(s, cr._cnx)

    query = """
        SELECT t.id line_id,
               t.{uom_column} line_uom_id,
               tu.{uom_name} line_uom_name,
               tuc.{category_name} line_uom_categ_name,
               pt.uom_id product_uom_id,
               ptu.{uom_name} product_uom_name,
               pt.id product_template_id,
               pt.{product_template_name} product_template_name,
               ptuc.{category_name} product_uom_categ_name
          FROM {table} t
          JOIN uom_uom tu ON t.{uom_column} = tu.id
          JOIN uom_category tuc ON tu.category_id = tuc.id
          JOIN product_product pp ON t.{product_column} = pp.id
          JOIN product_template pt ON pp.product_tmpl_id = pt.id
          JOIN uom_uom ptu ON pt.uom_id = ptu.id
          JOIN uom_category ptuc ON ptu.category_id = ptuc.id
         WHERE tu.category_id != ptu.category_id
               {ids}
               {active}
    """.format(
        table=q(table),
        uom_column=q(uom_field),
        product_column=q(product_field),
        uom_name=get_value_or_en_translation(cr, "uom_uom", "name"),
        category_name=get_value_or_en_translation(cr, "uom_category", "name"),
        product_template_name=get_value_or_en_translation(cr, "product_template", "name"),
        ids=" AND t.id IN %s" if ids else "",
        active=" AND pp.active" if not INCLUDE_ARCHIVED_PRODUCTS else "",
    )

    rows = []
    if ids is None:
        cr.execute(query)
        rows = cr.fetchall()
    elif ids:
        ids_chunks = chunks(ids, size=cr.IN_MAX, fmt=tuple)
        for chunk in ids_chunks:
            cr.execute(query, [chunk])
            rows.extend(cr.fetchall())

    if not rows:
        return []

    title = model.replace(".", " ").title()

    if FIX_PRODUCT_UOM:
        line_new_ids = {line_id: prod_uom_id for line_id, _, _, _, prod_uom_id, _, _, _, _ in rows}
        cr.execute(
            """
            UPDATE {table} t
            SET {uom_column} = (%s::jsonb->t.id::text)::int
            WHERE t.id IN %s
            """.format(
                table=q(table),
                uom_column=q(uom_field),
            ),
            [
                Json(line_new_ids),
                tuple(line_new_ids),
            ],
        )

        msg = dedent(
            """
        Upon your request, we have automatically fixed the faulty UoMs by picking it from
        the Product Template and setting it on the {title}s.

        Please, take the time to check that the following {title}s inconsistencies have
        been updated to the right UoM:\n\n{updated_uoms}
        """
        ).format(
            title=title,
            updated_uoms="\n".join(
                "         * {}(id={}): Updated UoM from `{}`(id={}, category: `{}`) to `{}`(id={}, category: `{}`) for Product Template `{}`(id={})".format(
                    title,
                    line_id,
                    line_uom,
                    line_uom_id,
                    line_uom_categ,
                    prod_uom,
                    prod_uom_id,
                    prod_uom_categ,
                    prod_temp,
                    prod_temp_id,
                )
                for line_id, line_uom_id, line_uom, line_uom_categ, prod_uom_id, prod_uom, prod_temp_id, prod_temp, prod_uom_categ in rows
            ),
        )
        faulty_ids = []

    else:
        msg = """
        There is a UoM mismatch in some {title}s. The category of the UoM defined on the
        {title} is different from that defined on the Product Template and must be the same to
        avoid errors. We allowed the upgrade to continue, but these inconsistencies may cause error
        during the upgrade or issues on the upgraded database.

        To avoid any issue, here are the options to consider:

         * fix these inconsistencies manually (below, the details of the affected records)
         * let this script automatically fix the affected records by setting the environment variable
           ODOO_MIG_FIX_ALL_UOM_INCONSISTENCIES to 1. It will take the UoM from the Product Template
           and set it on the faulty {title}s.

        You can also take the archived products into account for listing or fixing faulty lines by setting the
        environment variable ODOO_MIG_DO_NOT_IGNORE_ARCHIVED_PRODUCTS_FOR_UOM_INCONSISTENCIES to 1

        These {title}s have UoM inconsistencies:\n\n{uom_inconsistencies}
        """.format(
            title=title,
            uom_inconsistencies="\n".join(
                "         * {}(id={}) has UoM `{}`(id={}, category: `{}`), Product Template `{}`(id={}) has UoM `{}`(id={}, category: `{}`)".format(
                    title,
                    line_id,
                    line_uom,
                    line_uom_id,
                    line_uom_categ,
                    prod_temp,
                    prod_temp_id,
                    prod_uom,
                    prod_uom_id,
                    prod_uom_categ,
                )
                for line_id, line_uom_id, line_uom, line_uom_categ, prod_uom_id, prod_uom, prod_temp_id, prod_temp, prod_uom_categ in rows
            ),
        )
        faulty_ids = [r[0] for r in rows]

    _logger.warning("\n%s\n", msg)
    add_to_migration_reports(category=title + " UoM Inconsistencies", message=msg, format="md")
    return faulty_ids


def verify_products(
    cr,
    model,
    foreign_model,
    foreign_model_reference_field,
    model_product_field="product_id",
    foreign_model_product_field="product_id",
    ids=None,
):
    """
    Check if the product on the `foreign_model` is the same as the product on the `model`.

    When `ids` is not provided, every ids would be verified.

    The `foreign_model` should be the one that have a reference to the `model` using this
    schema:
        >>> `foreign_model`.`foreign_reference_field` = `model`.id

    In case where the model/foreign model own a specific product field (different than `product_id`),
    you NEED to provide it using model_product_field/foreign_model_product_field

    As a function example, if you want to check if the product defined on the `account_move_line`
    is the same as the product defined on the `purchase_order_line` using `purchase_line_id`
    as reference, you should call this function in this way:
        >>> verify_products(cr, "purchase.order.line", "account.move.line", "purchase_line_id", ids=ids)

    Returns list of ids if inconsistencies found, else [].
    """
    _validate_model(model)
    _validate_model(foreign_model)
    table = table_of_model(cr, model)
    foreign_table = table_of_model(cr, foreign_model)

    q = lambda s: quote_ident(s, cr._cnx)
    query = """
        SELECT f.id,
               f.{foreign_model_product_field},
               fpt.{name},
               t.id,
               t.{model_product_field},
               tpt.{name}
          FROM {table} t
          JOIN {foreign_table} f ON f.{foreign_model_reference_field} = t.id
          JOIN product_product tpp ON t.{model_product_field} = tpp.id
          JOIN product_template tpt ON tpp.product_tmpl_id = tpt.id
          JOIN product_product fpp ON f.{foreign_model_product_field} = fpp.id
          JOIN product_template fpt ON fpp.product_tmpl_id = fpt.id
         WHERE f.{foreign_model_product_field} != t.{model_product_field}
               {ids}
               {active}
    """.format(
        name=get_value_or_en_translation(cr, "product_template", "name"),
        table=q(table),
        foreign_table=q(foreign_table),
        foreign_model_reference_field=q(foreign_model_reference_field),
        model_product_field=q(model_product_field),
        foreign_model_product_field=q(foreign_model_product_field),
        ids=" AND t.id IN %s" if ids else "",
        active=" AND tpp.active" if not INCLUDE_ARCHIVED_PRODUCTS else "",
    )

    rows = []
    if ids is None:
        cr.execute(query)
        rows = cr.fetchall()
    elif ids:
        ids_chunks = chunks(ids, size=cr.IN_MAX, fmt=tuple)
        for chunk in ids_chunks:
            cr.execute(query, [chunk])
            rows.extend(cr.fetchall())

    if not rows:
        return []

    title = model.replace(".", " ").title()
    foreign_title = foreign_model.replace(".", " ").title()
    msg = """
    There is a product mismatch in some {foreign_title}. The product defined on the {foreign_title}
    is different from that defined on the {title}. To allow the upgrade to continue, the product
    on the {foreign_title} and on the {title} must be the same.
    These {foreign_title} have inconsistencies:
    """.format(**locals())
    msg += "\n".join(
        "     * {}(id={}) has Product `{}`(id={}), {}(id={}) has Product `{}`(id={})".format(
            foreign_title, fline_id, fline_product, fline_product_id, title, line_id, line_product, line_product_id
        )
        for fline_id, fline_product_id, fline_product, line_id, line_product_id, line_product in rows
    )

    add_to_migration_reports(
        category=title + " - " + foreign_title + " Products Inconsistencies",
        message=msg,
        format="md",
    )
    _logger.warning("\n%s\n", msg)
    return [r[0] for r in rows]
