# -*- coding: utf-8 -*-
import base64
import json
import logging
import re
import warnings

import psycopg2
from psycopg2 import sql

try:
    from odoo import release
    from odoo.osv import expression
    from odoo.tools.misc import mute_logger
    from odoo.tools.safe_eval import safe_eval
except ImportError:
    from openerp import release
    from openerp.osv import expression
    from openerp.tools.misc import mute_logger
    from openerp.tools.safe_eval import safe_eval

try:
    from odoo.tools.sql import make_index_name
except ImportError:

    def make_index_name(table_name, column_name):
        return "%s_%s_index" % (table_name, column_name)


from .const import ENVIRON
from .domains import _adapt_one_domain, _valid_path_to, adapt_domains
from .exceptions import SleepyDeveloperError
from .helpers import _dashboard_actions, _validate_model, table_of_model
from .inherit import for_each_inherit
from .misc import SelfPrintEvalContext, log_progress, version_gte
from .orm import env, invalidate
from .pg import (
    alter_column_type,
    column_exists,
    column_type,
    explode_query_range,
    format_query,
    get_value_or_en_translation,
    parallel_execute,
    pg_text2html,
    remove_column,
    savepoint,
    table_exists,
)
from .report import add_to_migration_reports, get_anchor_link_to_record

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = unicode = str


_logger = logging.getLogger(__name__)
IMD_FIELD_PATTERN = "field_%s__%s" if version_gte("saas~11.2") else "field_%s_%s"

_CONTEXT_KEYS_TO_CLEAN = (
    "group_by",
    "pivot_measures",
    "pivot_column_groupby",
    "pivot_row_groupby",
    "graph_groupbys",
    "orderedBy",
)


def ensure_m2o_func_field_data(cr, src_table, column, dst_table):
    """
    Fix broken m2o relations.
    If any `column` not present in `dst_table`, remove column from `src_table` in
    order to force recomputation of the function field

    WARN: only call this method on m2o function/related fields!!
    """
    if not column_exists(cr, src_table, column):
        return
    cr.execute(
        """
            SELECT count(1)
              FROM "{src_table}"
             WHERE "{column}" NOT IN (SELECT id FROM "{dst_table}")
        """.format(src_table=src_table, column=column, dst_table=dst_table)
    )
    if cr.fetchone()[0]:
        remove_column(cr, src_table, column, cascade=True)


def remove_field(cr, model, fieldname, cascade=False, drop_column=True, skip_inherit=()):
    _validate_model(model)

    ENVIRON["__renamed_fields"][model][fieldname] = None

    def filter_value(key, value):
        if key == "orderedBy" and isinstance(value, dict):
            res = {k: (filter_value(None, v) if k == "name" else v) for k, v in value.items()}
            # return if name didn't match fieldname
            return res if "name" not in res or res["name"] is not None else None
        if not isinstance(value, basestring):
            # if not a string, ignore it
            return value
        if value.split(":")[0] != fieldname:
            # only return if not matching fieldname
            return value
        return None  # value filtered out

    def clean_context(context):
        if not isinstance(context, dict):
            return False

        changed = False
        for key in _CONTEXT_KEYS_TO_CLEAN:
            if context.get(key):
                context_part = [filter_value(key, e) for e in context[key]]
                changed |= context_part != context[key]
                context[key] = [e for e in context_part if e is not None]

        for vt in ["pivot", "graph", "cohort"]:
            key = "{}_measure".format(vt)
            if key in context:
                new_value = filter_value(key, context[key])
                changed |= context[key] != new_value
                context[key] = new_value if new_value is not None else "id"

            if vt in context:
                changed |= clean_context(context[vt])

        return changed

    # clean dashboard's contexts
    for id_, action in _dashboard_actions(cr, r"\y{}\y".format(fieldname), model):
        context = safe_eval(action.get("context", "{}"), SelfPrintEvalContext(), nocopy=True)
        changed = clean_context(context)
        action.set("context", unicode(context))
        if changed:
            add_to_migration_reports(
                ("ir.ui.view.custom", id_, action.get("string", "ir.ui.view.custom")), "Filters/Dashboards"
            )

    # clean filter's contexts
    cr.execute(
        "SELECT id, name, context FROM ir_filters WHERE model_id = %s AND context ~ %s",
        [model, r"\y{}\y".format(fieldname)],
    )
    for id_, name, context_s in cr.fetchall():
        context = safe_eval(context_s or "{}", SelfPrintEvalContext(), nocopy=True)
        changed = clean_context(context)
        cr.execute("UPDATE ir_filters SET context = %s WHERE id = %s", [unicode(context), id_])
        if changed:
            add_to_migration_reports(("ir.filters", id_, name), "Filters/Dashboards")

    def adapter(leaf, is_or, negated):
        # replace by TRUE_LEAF, unless negated or in a OR operation but not negated
        if is_or ^ negated:
            return [expression.FALSE_LEAF]
        return [expression.TRUE_LEAF]

    # clean domains
    adapt_domains(cr, model, fieldname, "ignored", adapter=adapter, skip_inherit=skip_inherit, force_adapt=True)

    if table_exists(cr, "ir_server_object_lines"):
        cr.execute(
            """
            DELETE FROM ir_server_object_lines
                  WHERE col1 IN (SELECT id
                                   FROM ir_model_fields
                                  WHERE model = %s
                                    AND name = %s)
            """,
            [model, fieldname],
        )

    # update tracking values
    if column_exists(cr, "mail_tracking_value", "field_info"):
        cr.execute(
            """
                SELECT id, field_description, name, ttype
                  FROM ir_model_fields
                 WHERE model=%s
                   AND name=%s
            """,
            (model, fieldname),
        )
        if cr.rowcount:
            (field_id, field_desc_w_translation, name, ttype) = cr.fetchone()
            field_desc = field_desc_w_translation.get("en_US", next(iter(field_desc_w_translation.values())))
            fields_info = {
                "desc": field_desc,
                "name": name,
                "type": ttype,
            }
            cr.execute(
                """
                UPDATE mail_tracking_value
                   SET field_info = %s
                 WHERE field_id = %s
                """,
                (psycopg2.extras.Json(fields_info), field_id),
            )

    # remove this field from dependencies of other fields
    if column_exists(cr, "ir_model_fields", "depends"):
        cr.execute(
            "SELECT id,model,depends FROM ir_model_fields WHERE state='manual' AND depends ~ %s",
            [r"\m{}\M".format(fieldname)],
        )
        for id, from_model, deps in cr.fetchall():
            parts = []
            for part in deps.split(","):
                path = part.strip().split(".")
                if not any(
                    path[i] == fieldname and _valid_path_to(cr, path[:i], from_model, model) for i in range(len(path))
                ):
                    parts.append(part)
            if len(parts) != len(deps.split(",")):
                cr.execute("UPDATE ir_model_fields SET depends=%s WHERE id=%s", [", ".join(parts) or None, id])

    # drop m2m table if needed
    if drop_column and column_exists(cr, "ir_model_fields", "relation_table"):  # appears in version 9.0
        # verify that there aren't any other m2m pointing to the relation table
        cr.execute(
            """
                SELECT relation_table
                  FROM ir_model_fields
                 WHERE relation_table = (
                        SELECT relation_table
                          FROM ir_model_fields
                         WHERE model = %s
                           AND name = %s
                           AND ttype = 'many2many'
                       )
                   AND ttype = 'many2many'
              GROUP BY relation_table
                HAVING count(*) = 1
            """,
            [model, fieldname],
        )
        for (m2m_rel,) in cr.fetchall():
            cr.execute('DROP TABLE IF EXISTS "{}" CASCADE'.format(m2m_rel))
            cr.execute(
                "DELETE FROM ir_model_relation r USING ir_model m WHERE m.id = r.model AND r.name = %s", [m2m_rel]
            )
            ENVIRON.setdefault("_gone_m2m", {})[m2m_rel] = "%s:%s" % (model, fieldname)

    # remove the ir.model.fields entry (and its xmlid)
    cr.execute(
        """
            WITH del AS (
                DELETE FROM ir_model_fields WHERE model=%s AND name=%s RETURNING id
            )
            DELETE FROM ir_model_data
                  USING del
                  WHERE model = 'ir.model.fields'
                    AND res_id = del.id
        """,
        [model, fieldname],
    )

    # cleanup translations
    if table_exists(cr, "ir_translation"):
        cr.execute(
            """
           DELETE FROM ir_translation
            WHERE name=%s
              AND type in ('field', 'help', 'model', 'model_terms', 'selection')   -- ignore wizard_* translations
        """,
            ["%s,%s" % (model, fieldname)],
        )

    # remove default values set for aliases
    if column_exists(cr, "mail_alias", "alias_defaults"):
        cr.execute(
            """
            SELECT a.id, a.alias_defaults
              FROM mail_alias a
              JOIN ir_model m ON m.id = a.alias_model_id
             WHERE m.model = %s
               AND a.alias_defaults ~ %s
        """,
            [model, r"\y%s\y" % (fieldname,)],
        )
        for alias_id, defaults_s in cr.fetchall():
            try:
                defaults = dict(safe_eval(defaults_s))  # XXX literal_eval should works.
            except Exception:
                continue
            defaults.pop(fieldname, None)
            cr.execute("UPDATE mail_alias SET alias_defaults = %s WHERE id = %s", [repr(defaults), alias_id])

    # if field was a binary field stored as attachment, clean them...
    if column_exists(cr, "ir_attachment", "res_field"):
        parallel_execute(
            cr,
            explode_query_range(
                cr,
                cr.mogrify(
                    "DELETE FROM ir_attachment WHERE res_model = %s AND res_field = %s", [model, fieldname]
                ).decode(),
                table="ir_attachment",
            ),
        )

    table = table_of_model(cr, model)
    # NOTE table_exists is needed to avoid altering views
    if drop_column and table_exists(cr, table) and column_exists(cr, table, fieldname):
        remove_column(cr, table, fieldname, cascade=cascade)

    # remove field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        remove_field(cr, inh.model, fieldname, cascade=cascade, drop_column=drop_column, skip_inherit=skip_inherit)


def remove_field_metadata(cr, model, fieldname, skip_inherit=()):
    """
    Due to a bug of the ORM [1], mixins doesn't create/register xmlids for fields created in children models
    Thus, when a field is no more defined in a child model, their xmlids should be removed explicitly to
    avoid the fields to be considered as missing and being removed at the end of the upgrade.

    [1] https://github.com/odoo/odoo/issues/49354
    """
    _validate_model(model)

    cr.execute(
        """
            DELETE FROM ir_model_data
                  WHERE model = 'ir.model.fields'
                    AND res_id IN (SELECT id FROM ir_model_fields WHERE model=%s AND name=%s)
        """,
        [model, fieldname],
    )
    for inh in for_each_inherit(cr, model, skip_inherit):
        remove_field_metadata(cr, inh.model, fieldname, skip_inherit=skip_inherit)


def move_field_to_module(cr, model, fieldname, old_module, new_module, skip_inherit=()):
    _validate_model(model)
    name = IMD_FIELD_PATTERN % (model.replace(".", "_"), fieldname)
    try:
        with savepoint(cr), mute_logger("openerp.sql_db", "odoo.sql_db"):
            cr.execute(
                """
                   UPDATE ir_model_data
                      SET module = %s
                    WHERE model = 'ir.model.fields'
                      AND name = %s
                      AND module = %s
            """,
                [new_module, name, old_module],
            )
    except psycopg2.IntegrityError:
        cr.execute(
            "DELETE FROM ir_model_data WHERE model = 'ir.model.fields' AND name = %s AND module = %s",
            [name, old_module],
        )
    # move field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        move_field_to_module(cr, inh.model, fieldname, old_module, new_module, skip_inherit=skip_inherit)


def rename_field(cr, model, old, new, update_references=True, domain_adapter=None, skip_inherit=()):
    _validate_model(model)

    rf = ENVIRON["__renamed_fields"][model]
    rf[new] = rf.pop(old, old)

    try:
        with savepoint(cr):
            cr.execute("UPDATE ir_model_fields SET name=%s WHERE model=%s AND name=%s RETURNING id", (new, model, old))
            [fid] = cr.fetchone() or [None]
    except psycopg2.IntegrityError:
        # If a field with the same name already exists for this model (e.g. due to a custom module),
        # rename it to avoid clashing and warn the customer
        custom_name = new + "_custom"
        rename_field(cr, model, new, custom_name, update_references, domain_adapter=None, skip_inherit=skip_inherit)
        cr.execute("UPDATE ir_model_fields SET name=%s WHERE model=%s AND name=%s RETURNING id", (new, model, old))
        [fid] = cr.fetchone() or [None]
        msg = (
            "The field %r from the model %r is now a standard Odoo field, but it already existed in the database "
            "(coming from a non-standard module) and thus has been renamed to %r" % (new, model, custom_name)
        )
        add_to_migration_reports(msg, "Non-standard fields")

    if fid:
        name = IMD_FIELD_PATTERN % (model.replace(".", "_"), new)
        # In some cases the same field may be present on ir_model_data with both the double __ and single _ name
        # version. To avoid conflicts (module, name) on the UPDATE below we keep only the double __ version
        cr.execute(
            """
             DELETE FROM ir_model_data
                   WHERE id IN (SELECT unnest((array_agg(id ORDER BY id))[2:count(id)])
                                  FROM ir_model_data
                                 WHERE model = 'ir.model.fields'
                                   AND res_id = %s
                                 GROUP BY module)
            """,
            [fid],
        )
        try:
            with savepoint(cr):
                cr.execute("UPDATE ir_model_data SET name=%s WHERE model='ir.model.fields' AND res_id=%s", [name, fid])
        except psycopg2.IntegrityError:
            # duplicate key. May happen du to conflict between
            # some_model.sub_id and some_model_sub.id
            # (before saas~11.2, where pattern changed)
            name = "%s_%s" % (name, fid)
            cr.execute("UPDATE ir_model_data SET name=%s WHERE model='ir.model.fields' AND res_id=%s", [name, fid])
        cr.execute("UPDATE ir_property SET name=%s WHERE fields_id=%s", [new, fid])
    # Updates custom field relation name to match the renamed standard field during upgrade.
    cr.execute(
        """
        UPDATE ir_model_fields
           SET relation_field = %s
         WHERE relation = %s
           AND relation_field = %s
           AND state = 'manual'
    """,
        [new, model, old],
    )

    if table_exists(cr, "ir_translation"):
        cr.execute(
            """
           UPDATE ir_translation
              SET name=%s
            WHERE name=%s
              AND type in ('field', 'help', 'model', 'model_terms', 'selection')   -- ignore wizard_* translations
        """,
            ["%s,%s" % (model, new), "%s,%s" % (model, old)],
        )

    if column_exists(cr, "ir_attachment", "res_field"):
        cr.execute(
            """
            UPDATE ir_attachment
               SET res_field = %s
             WHERE res_model = %s
               AND res_field = %s
        """,
            [new, model, old],
        )

    if table_exists(cr, "ir_values"):
        cr.execute(
            """
            UPDATE ir_values
               SET name = %s
             WHERE model = %s
               AND name = %s
               AND key = 'default'
        """,
            [new, model, old],
        )

    if column_type(cr, "mail_tracking_value", "field") == "varchar":
        # From saas~13.1, column `field` is a m2o to the `ir.model.fields`
        cr.execute(
            """
            UPDATE mail_tracking_value v
               SET field = %s
              FROM mail_message m
             WHERE v.mail_message_id = m.id
               AND m.model = %s
               AND v.field = %s
          """,
            [new, model, old],
        )

    table = table_of_model(cr, model)
    # NOTE table_exists is needed to avoid altering views
    if table_exists(cr, table) and column_exists(cr, table, old):
        cr.execute('ALTER TABLE "{0}" RENAME COLUMN "{1}" TO "{2}"'.format(table, old, new))
        # Rename corresponding index
        new_index_name = make_index_name(table, new)
        old_index_name = make_index_name(table, old)
        cr.execute('ALTER INDEX IF EXISTS "{0}" RENAME TO "{1}"'.format(new_index_name, old_index_name))

    if update_references:
        # skip all inherit, they will be handled by the resursive call
        update_field_usage(cr, model, old, new, domain_adapter=domain_adapter, skip_inherit="*")

    # rename field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        rename_field(cr, inh.model, old, new, update_references=update_references, skip_inherit=skip_inherit)


def convert_field_to_html(cr, model, field, skip_inherit=()):
    _validate_model(model)
    table = table_of_model(cr, model)
    if column_exists(cr, table, field):  # only exists for list-based inherits (not for dict-based)
        jsonb_column = column_type(cr, table, field) == "jsonb"
        if jsonb_column:
            query = """
                WITH html_values AS (
                    SELECT id, jsonb_object_agg(t.key, {2}) AS value
                      FROM "{0}", jsonb_each_text("{1}") AS t
                     WHERE {{parallel_filter}}
                  GROUP BY id
                )
                UPDATE "{0}"
                   SET "{1}" = h.value
                  FROM html_values AS h
                 WHERE "{0}".id = h.id
            """.format(table, field, pg_text2html("t.value"))
        else:
            query = 'UPDATE "{0}" SET "{1}" = {2} WHERE "{1}" IS NOT NULL'.format(table, field, pg_text2html(field))
        parallel_execute(cr, explode_query_range(cr, query, table=table))

    # Update translations
    if table_exists(cr, "ir_translation"):
        wrap = None if version_gte("saas~11.5") else "p"
        ttype = "model_terms" if version_gte("saas~11.5") else "model"
        cr.execute(
            r"""
            UPDATE ir_translation
               SET src = {}, value = {}, type = %s
             WHERE type = 'model'
               AND name = %s
            """.format(pg_text2html("src", wrap=wrap), pg_text2html("value", wrap=wrap)),
            [ttype, "%s,%s" % (model, field)],
        )
    cr.execute(
        """
        UPDATE ir_model_fields AS imf
           SET ttype = 'html'
          FROM ir_model_fields AS f
         WHERE imf.related_field_id = f.id
           AND f.name = %s
           AND f.model = %s
           AND imf.state = 'manual'
        """,
        [field, model],
    )
    for inh in for_each_inherit(cr, model, skip_inherit):
        convert_field_to_html(cr, inh.model, field, skip_inherit=skip_inherit)


def convert_field_to_property(
    cr, model, field, type, target_model=None, default_value=None, default_value_ref=None, company_field="company_id"
):
    """
    Notes:
        `target_model` is only use when `type` is "many2one".
        The `company_field` can be an sql expression.
            You may use `t` to refer the model's table.
    """
    _validate_model(model)
    if target_model:
        _validate_model(target_model)
    type2field = {
        "char": "value_text",
        "float": "value_float",
        "boolean": "value_integer",
        "integer": "value_integer",
        "text": "value_text",
        "binary": "value_binary",
        "many2one": "value_reference",
        "date": "value_datetime",
        "datetime": "value_datetime",
        "selection": "value_text",
    }

    assert type in type2field
    value_field = type2field[type]

    table = table_of_model(cr, model)

    cr.execute("SELECT id FROM ir_model_fields WHERE model=%s AND name=%s", (model, field))
    if not cr.rowcount:
        # no ir_model_fields, no ir_property
        remove_column(cr, table, field, cascade=True)
        return
    [fields_id] = cr.fetchone()

    if default_value is None:
        where_clause = "{field} IS NOT NULL".format(field=field)
    else:
        where_clause = "{field} != %(default_value)s".format(field=field)

    if type == "boolean":
        value_select = "%s::integer" % field
    elif type != "many2one":
        value_select = field
    else:
        # for m2o, the store value is a refrence field, so in format `model,id`
        value_select = "CONCAT('{target_model},', {field})".format(**locals())

    if is_field_anonymized(cr, model, field):
        # if field is anonymized, we need to create a property for each record
        where_clause = "true"
        # and we need to unanonymize its values
        ano_default_value = cr.mogrify("%s", [default_value])
        if type != "many2one":  # noqa: SIM108
            ano_value_select = "%(value)s"
        else:
            ano_value_select = "CONCAT('{0},', %(value)s)".format(target_model)

        register_unanonymization_query(
            cr,
            model,
            field,
            """
            UPDATE ir_property
               SET {value_field} = CASE WHEN %(value)s IS NULL THEN {ano_default_value}
                                        ELSE {ano_value_select} END
             WHERE res_id = CONCAT('{model},', %(id)s)
               AND name='{field}'
               AND type='{type}'
               AND fields_id={fields_id}
            """.format(**locals()),
        )

    cr.execute(
        """
        WITH cte AS (
            SELECT CONCAT('{model},', id) as res_id, {value_select} as value,
                   ({company_field})::integer as company
              FROM {table} t
             WHERE {where_clause}
        )
        INSERT INTO ir_property(name, type, fields_id, company_id, res_id, {value_field})
            SELECT %(field)s, %(type)s, %(fields_id)s, cte.company, cte.res_id, cte.value
              FROM cte
             WHERE NOT EXISTS(SELECT 1
                                FROM ir_property
                               WHERE fields_id=%(fields_id)s
                                 AND company_id IS NOT DISTINCT FROM cte.company
                                 AND res_id=cte.res_id)
    """.format(**locals()),
        locals(),
    )
    # default property
    if default_value:
        cr.execute(
            """
                INSERT INTO ir_property(name, type, fields_id, {value_field})
                     VALUES (%s, %s, %s, %s)
                  RETURNING id
            """.format(value_field=value_field),
            (field, type, fields_id, default_value),
        )
        [prop_id] = cr.fetchone()
        if default_value_ref:
            module, _, xid = default_value_ref.partition(".")
            cr.execute(
                """
                    INSERT INTO ir_model_data(module, name, model, res_id, noupdate)
                         VALUES (%s, %s, %s, %s, %s)
            """,
                [module, xid, "ir.property", prop_id, True],
            )

    remove_column(cr, table, field, cascade=True)


# alias with a name related to the new API to declare property fields (company_dependent=True attribute)
make_field_company_dependent = convert_field_to_property


def convert_binary_field_to_attachment(cr, model, field, encoded=True, name_field=None):
    _validate_model(model)
    table = table_of_model(cr, model)
    if not column_exists(cr, table, field):
        return
    name_query = "COALESCE({0}, '{1}('|| id || ').{2}')".format(
        name_field if name_field else "NULL",
        model.title().replace(".", ""),
        field,
    )

    cr.execute(format_query(cr, "SELECT count(*) FROM {} WHERE {} IS NOT NULL", table, field))
    [count] = cr.fetchone()

    A = env(cr)["ir.attachment"]
    iter_cur = cr._cnx.cursor("fetch_binary")
    iter_cur.itersize = 1
    iter_cur.execute(
        format_query(
            cr,
            "SELECT id, {field}, {name_query} FROM {table} WHERE {field} IS NOT NULL",
            field=field,
            name_query=sql.SQL(name_query),
            table=table,
        )
    )
    logger = _logger.getChild("convert_binary_field_to_attachment")
    for rid, data, name in log_progress(iter_cur, logger=logger, qualifier="rows", size=count):
        # we can't save create the attachment with res_model & res_id as it will fail computing
        # `res_name` field for non-loaded models. Store it naked and change it via SQL after.
        data = bytes(data)  # noqa: PLW2901
        if re.match(b"^\\d+ (bytes|[KMG]b)$", data, re.I):
            # badly saved data, no need to create an attachment.
            continue
        if not encoded:
            data = base64.b64encode(data)  # noqa: PLW2901
        att = A.create({"name": name, "datas": data, "type": "binary"})
        cr.execute(
            """
               UPDATE ir_attachment
                  SET res_model = %s,
                      res_id = %s,
                      res_field = %s
                WHERE id = %s
            """,
            [model, rid, field, att.id],
        )
        invalidate(att)

    iter_cur.close()
    # free PG space
    remove_column(cr, table, field)


if version_gte("16.0"):

    def convert_field_to_translatable(cr, model, field):
        table = table_of_model(cr, model)
        if column_type(cr, table, field) == "jsonb":
            return
        alter_column_type(cr, table, field, "jsonb", "jsonb_build_object('en_US', {0})")

    def convert_field_to_untranslatable(cr, model, field, type="varchar"):
        assert type in ("varchar", "text")
        table = table_of_model(cr, model)
        if column_type(cr, table, field) != "jsonb":
            return

        alter_column_type(cr, table, field, type, "{0}->>'en_US'")

else:
    # Older versions, these functions are no-op
    def convert_field_to_translatable(cr, model, field):
        pass

    def convert_field_to_untranslatable(cr, model, field, type="varchar"):
        pass


def change_field_selection_values(cr, model, field, mapping, skip_inherit=()):
    _validate_model(model)
    if not mapping:
        return
    table = table_of_model(cr, model)

    if column_exists(cr, table, field):
        query = "UPDATE {table} SET {field}= %s::jsonb->>{field} WHERE {field} IN %s".format(table=table, field=field)
        queries = [
            cr.mogrify(q, [json.dumps(mapping), tuple(mapping)]).decode()
            for q in explode_query_range(cr, query, table=table)
        ]
        parallel_execute(cr, queries)

    cr.execute(
        """
        DELETE FROM ir_model_fields_selection s
              USING ir_model_fields f
              WHERE f.id = s.field_id
                AND f.model = %s
                AND f.name = %s
                AND s.value IN %s
        """,
        [model, field, tuple(mapping)],
    )

    def adapter(leaf, _or, _neg):
        left, op, right = leaf
        if isinstance(right, (tuple, list)):  # noqa: SIM108
            right = [mapping.get(r, r) for r in right]
        else:
            right = mapping.get(right, right)
        return [(left, op, right)]

    # skip all inherit, they will be handled by the resursive call
    adapt_domains(cr, model, field, field, adapter=adapter, skip_inherit="*")

    # rename field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        change_field_selection_values(cr, inh.model, field, mapping=mapping, skip_inherit=skip_inherit)


def is_field_anonymized(cr, model, field):
    from .modules import module_installed

    _validate_model(model)
    if not module_installed(cr, "anonymization"):
        return False
    cr.execute(
        """
            SELECT id
              FROM ir_model_fields_anonymization
             WHERE model_name = %s
               AND field_name = %s
               AND state = 'anonymized'
    """,
        [model, field],
    )
    return bool(cr.rowcount)


def register_unanonymization_query(cr, model, field, query, query_type="sql", sequence=10):
    _validate_model(model)
    cr.execute(
        """
            INSERT INTO ir_model_fields_anonymization_migration_fix(
                    target_version, sequence, query_type, model_name, field_name, query
            ) VALUES (%s, %s, %s, %s, %s, %s)
    """,
        [release.major_version, sequence, query_type, model, field, query],
    )


def update_field_usage(cr, model, old, new, domain_adapter=None, skip_inherit=()):
    """
    Replace all references to field `old` to `new` in:
        - ir_filters
        - ir_exports_line
        - ir_act_server
        - mail_alias
        - ir_ui_view_custom (dashboard)
        - domains (using `domain_adapter`)
        - related fields

    """
    return _update_field_usage_multi(cr, [model], old, new, domain_adapter=domain_adapter, skip_inherit=skip_inherit)


def update_field_references(cr, old, new, only_models=None, domain_adapter=None, skip_inherit=()):
    warnings.warn(
        "The `update_field_references` function is deprecated. Use `update_field_usage` instead.",
        DeprecationWarning,
        stacklevel=1,
    )
    models = "*" if only_models is None else only_models
    return _update_field_usage_multi(cr, models, old, new, domain_adapter=domain_adapter, skip_inherit=skip_inherit)


def _update_field_usage_multi(cr, models, old, new, domain_adapter=None, skip_inherit=()):
    assert models
    only_models = None if models == "*" else tuple(models)

    if only_models:
        for model in only_models:
            _validate_model(model)

    p = {
        "old": r"\y%s\y" % (re.escape(old),),
        "old_pattern": r"""[.'"]{0}\y""".format(re.escape(old)),
        "new": new,
        "def_old": r"\ydefault_%s\y" % (re.escape(old),),
        "def_new": "default_%s" % (new,),
        "models": tuple(only_models) if only_models else (),
    }

    # ir.action.server
    # Modifying server action is dangerous.
    # The search pattern can be anywhere in the code, leading to invalid codes.
    # Moreover, limiting to some models will ignore some SA that should be modified.
    # Just search for the potential SA that need update.
    col_prefix = ""
    if not column_exists(cr, "ir_act_server", "condition"):
        col_prefix = "--"  # sql comment the line

    q = """
        SELECT id, {name}
          FROM ir_act_server
         WHERE state = 'code'
           AND (code ~ %(old_pattern)s
                {col_prefix} OR condition ~ %(old)s
               )
    """

    cr.execute(q.format(col_prefix=col_prefix, name=get_value_or_en_translation(cr, "ir_act_server", "name")), p)
    if cr.rowcount:
        li = "".join(
            "<li>{}</li>".format(get_anchor_link_to_record("ir.actions.server", aid, aname))
            for aid, aname in cr.fetchall()
        )
        model_text = "All models"
        if only_models:
            model_text = "Models " + ", ".join("<kbd>{}</kbd>".format(m) for m in only_models)
        add_to_migration_reports(
            """
<details>
  <summary>
    {model_text}: the field <kbd>{old}</kbd> has been renamed to <kbd>{new}</kbd>. The following server actions may need update.
  </summary>
  <ul>{li}</ul>
</details>
            """.format(**locals()),
            category="Fields renamed",
            format="html",
        )

    # if we stay on the same model. (no usage of dotted-path) (only works for domains and related)
    if "." not in old and "." not in new:
        # ir.filters
        col_prefix = ""
        if not column_exists(cr, "ir_filters", "sort"):
            col_prefix = "--"  # sql comment the line
        q = """
            UPDATE ir_filters
               SET {col_prefix} sort = regexp_replace(sort, %(old)s, %(new)s, 'g'),
                   context = regexp_replace(regexp_replace(context,
                                                           %(old)s, %(new)s, 'g'),
                                                           %(def_old)s, %(def_new)s, 'g')
        """

        if only_models:
            q += " WHERE model_id IN %(models)s AND "
        else:
            q += " WHERE "
        q += """
            (
                context ~ %(old)s
                OR context ~ %(def_old)s
                {col_prefix} OR sort ~ %(old)s
            )
        """
        cr.execute(q.format(col_prefix=col_prefix), p)

        # ir.exports.line
        q = """
            UPDATE ir_exports_line l
               SET name = regexp_replace(l.name, %(old)s, %(new)s, 'g')
        """
        if only_models:
            q += """
              FROM ir_exports e
             WHERE e.id = l.export_id
               AND e.resource IN %(models)s
               AND
            """
        else:
            q += "WHERE "
        q += "l.name ~ %(old)s"
        cr.execute(q, p)

        # mail.alias
        if column_exists(cr, "mail_alias", "alias_defaults"):
            q = """
                UPDATE mail_alias a
                   SET alias_defaults = regexp_replace(a.alias_defaults, %(old)s, %(new)s, 'g')
            """
            if only_models:
                q += """
                  FROM ir_model m
                 WHERE m.id = a.alias_model_id
                   AND m.model IN %(models)s
                   AND
                """
            else:
                q += "WHERE "
            q += "a.alias_defaults ~ %(old)s"
            cr.execute(q, p)

        # ir.ui.view.custom
        # adapt the context. The domain will be done by `adapt_domain`
        eval_context = SelfPrintEvalContext()
        def_old = "default_{}".format(old)
        def_new = "default_{}".format(new)
        match = "{0[old]}|{0[def_old]}".format(p)

        def adapt_value(key, value):
            if key == "orderedBy" and isinstance(value, dict):
                # only adapt the "name" key
                return {k: (adapt_value(None, v) if k == "name" else v) for k, v in value.items()}

            if not isinstance(value, basestring):
                # ignore if not a string
                return value

            parts = value.split(":", 1)
            if parts[0] != old:
                # if not match old, leave it
                return value
            # change to new, and return it
            parts[0] = new
            return ":".join(parts)

        def adapt_dict(d):
            # adapt (in place) dictionary values
            if not isinstance(d, dict):
                return

            for key in _CONTEXT_KEYS_TO_CLEAN:
                if d.get(key):
                    d[key] = [adapt_value(key, e) for e in d[key]]

            for vt in ["pivot", "graph", "cohort"]:
                key = "{}_measure".format(vt)
                if key in d:
                    d[key] = adapt_value(key, d[key])

                if vt in d:
                    adapt_dict(d[vt])

        for _, act in _dashboard_actions(cr, match, *only_models or ()):
            context = safe_eval(act.get("context", "{}"), eval_context, nocopy=True)
            adapt_dict(context)

            if def_old in context:
                context[def_new] = context.pop(def_old)
            act.set("context", unicode(context))

    # domains, related and inhited models
    if only_models:
        for model in only_models:
            # skip all inherit, they will be handled by the resursive call
            adapt_domains(cr, model, old, new, adapter=domain_adapter, skip_inherit="*", force_adapt=True)
            adapt_related(cr, model, old, new, skip_inherit="*")
            adapt_depends(cr, model, old, new, skip_inherit="*")

        inherited_models = tuple(
            inh.model for model in only_models for inh in for_each_inherit(cr, model, skip_inherit)
        )
        if inherited_models:
            _update_field_usage_multi(
                cr, inherited_models, old, new, domain_adapter=domain_adapter, skip_inherit=skip_inherit
            )


def adapt_depends(cr, model, old, new, skip_inherit=()):
    # adapt depends for custom compute fields only. Standard fields will be updated by the ORM.
    _validate_model(model)

    if not column_exists(cr, "ir_model_fields", "depends"):
        # this field only appears in 9.0
        return

    target_model = model

    match_old = r"\y{}\y".format(re.escape(old))
    cr.execute(
        """
        SELECT id, model, depends
          FROM ir_model_fields
         WHERE state = 'manual'
           AND depends ~ %s
        """,
        [match_old],
    )
    for id_, model, depends in cr.fetchall():
        temp_depends = depends.split(",")
        for i in range(len(temp_depends)):
            domain = _adapt_one_domain(
                cr, target_model, old, new, model, [(temp_depends[i], "=", "depends")], force_adapt=True
            )
            if domain:
                temp_depends[i] = domain[0][0]
        new_depends = ",".join(temp_depends)
        if new_depends != depends:
            cr.execute("UPDATE ir_model_fields SET depends = %s WHERE id = %s", [new_depends, id_])

    # down on inherits
    for inh in for_each_inherit(cr, target_model, skip_inherit):
        adapt_depends(cr, inh.model, old, new, skip_inherit=skip_inherit)


def adapt_related(cr, model, old, new, skip_inherit=()):
    _validate_model(model)

    if not column_exists(cr, "ir_model_fields", "related"):
        # this field only appears in 9.0
        return

    target_model = model

    match_old = r"\y{}\y".format(re.escape(old))
    cr.execute(
        """
        SELECT id, model, related
          FROM ir_model_fields
         WHERE related ~ %s
        """,
        [match_old],
    )
    for id_, model, related in cr.fetchall():
        domain = _adapt_one_domain(cr, target_model, old, new, model, [(related, "=", "related")], force_adapt=True)
        if domain:
            cr.execute("UPDATE ir_model_fields SET related = %s WHERE id = %s", [domain[0][0], id_])

    # TODO adapt paths in email templates?

    # down on inherits
    for inh in for_each_inherit(cr, target_model, skip_inherit):
        adapt_related(cr, inh.model, old, new, skip_inherit=skip_inherit)


def update_server_actions_fields(cr, src_model, dst_model=None, fields_mapping=None):
    """
    When some fields of `src_model` have ben copied to `dst_model` and/or have
    been copied to fields with another name, some references have to be moved.

    For server actions, this function starts by updating `ir_server_object_lines`
    to refer to new fields. If no `fields_mapping` is provided, all references to
    fields that exist in both models (source and destination) are moved. If no `dst_model`
    is given, the `src_model` is used as `dst_model`.
    Then, if `dst_model` is set, `ir_act_server` referred by modified `ir_server_object_lines`
    are also updated. A chatter message informs the customer about this modification.
    """
    if dst_model is None and fields_mapping is None:
        raise SleepyDeveloperError(
            "at least dst_model or fields_mapping must be given to the move_field_references function."
        )

    _dst_model = dst_model if dst_model is not None else src_model

    # update ir_server_object_lines to point to new fields
    if fields_mapping is None:
        cr.execute(
            """
            WITH field_ids AS (
                SELECT mf1.id as old_field_id, mf2.id as new_field_id
                  FROM ir_model_fields mf1
                  JOIN ir_model_fields mf2 ON mf2.name = mf1.name
                 WHERE mf1.model = %s
                   AND mf2.model = %s
            )
               UPDATE ir_server_object_lines
                  SET col1 = f.new_field_id
                 FROM field_ids f
                WHERE col1 = f.old_field_id
            RETURNING server_id
            """,
            [src_model, _dst_model],
        )
    else:
        psycopg2.extras.execute_values(
            cr._obj,
            """
            WITH field_ids AS (
                SELECT mf1.id as old_field_id, mf2.id as new_field_id
                  FROM (VALUES %s) AS mapping(src_model, dst_model, old_name, new_name)
                  JOIN ir_model_fields mf1 ON mf1.name = mapping.old_name AND mf1.model = mapping.src_model
                  JOIN ir_model_fields mf2 ON mf2.name = mapping.new_name AND mf2.model = mapping.dst_model
            )
               UPDATE ir_server_object_lines
                  SET col1 = f.new_field_id
                 FROM field_ids f
                WHERE col1 = f.old_field_id
            RETURNING server_id
            """,
            [(src_model, _dst_model, fm[0], fm[1]) for fm in fields_mapping],
        )

    # update ir_act_server records to point to the right model if set
    if dst_model is not None and src_model != dst_model and cr.rowcount > 0:
        action_ids = tuple({row[0] for row in cr.fetchall()})  # uniquify ids

        cr.execute(
            """
               UPDATE ir_act_server
                  SET model_name = %s, model_id = ir_model.id
                 FROM ir_model
                WHERE ir_model.model = %s
                  AND ir_act_server.id IN %s
            RETURNING ir_act_server.name
            """,
            [dst_model, dst_model, action_ids],
        )

        action_names = [row[0] for row in cr.fetchall()]

        # inform the customer through the chatter about this modification
        msg = (
            "The following server actions have been updated due to moving "
            "fields from '%(src_model)s' to '%(dst_model)s' model and need "
            "a checking from your side: %(actions)s"
        ) % {"src_model": src_model, "dst_model": dst_model, "actions": ", ".join(action_names)}

        add_to_migration_reports(message=msg, category="Server Actions")
