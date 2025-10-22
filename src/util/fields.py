# -*- coding: utf-8 -*-
"""
Utility functions for modifying model fields.

Field operations are best done in `pre-` script of the involved modules. In some cases a
preliminary operation could be done in pre and finished in post. A common example is to
remove a field in pre- keeping its column, later used in post when the column is finally
dropped.
"""

import base64
import logging
import re
import warnings
from ast import literal_eval

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

try:
    from odoo import modules
except ImportError:
    from openerp import modules

try:
    from odoo import release
except ImportError:
    from openerp import release

from .domains import FALSE_LEAF, TRUE_LEAF

try:
    from odoo.tools.sql import make_index_name
except ImportError:

    def make_index_name(table_name, column_name):
        return "%s_%s_index" % (table_name, column_name)


try:
    from odoo.tools import pickle
except ImportError:
    import pickle


from . import json
from .const import ENVIRON
from .domains import _adapt_one_domain, _replace_path, _valid_path_to, adapt_domains
from .exceptions import SleepyDeveloperError, UpgradeError
from .helpers import _dashboard_actions, _validate_model, resolve_model_fields_path, table_of_model
from .inherit import for_each_inherit
from .misc import AUTO, log_progress, safe_eval, version_gte
from .orm import env, invalidate
from .pg import (
    PGRegexp,
    SQLStr,
    alter_column_type,
    column_exists,
    column_type,
    create_m2m,
    explode_execute,
    explode_query_range,
    format_query,
    get_columns,
    get_value_or_en_translation,
    parallel_execute,
    pg_replace,
    pg_text2html,
    remove_column,
    table_exists,
    target_of,
)
from .records import _remove_import_export_paths
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

    :meta private: exclude from online docs
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


def _remove_field_from_filters(cr, model, field):
    cr.execute(
        "SELECT id, name, context FROM ir_filters WHERE model_id = %s AND context ~ %s",
        [model, r"\y{}\y".format(field)],
    )
    for id_, name, context_s in cr.fetchall():
        context = safe_eval(context_s or "{}")
        changed = _remove_field_from_context(context, field)
        cr.execute("UPDATE ir_filters SET context = %s WHERE id = %s", [unicode(context), id_])
        if changed:
            add_to_migration_reports(("ir.filters", id_, name), "Filters/Dashboards")

    if column_exists(cr, "ir_filters", "sort"):
        cr.execute(
            """
               WITH to_update AS (
                   SELECT f.id,
                          COALESCE(ARRAY_TO_JSON(ARRAY_AGG(s.sort_item ORDER BY rn) filter (WHERE s.sort_item not in %s)), '[]') AS sort
                     FROM ir_filters f
                 JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(f.sort::jsonb)
                       WITH ORDINALITY AS s(sort_item, rn)
                          ON true
                       WHERE f.model_id = %s
                         AND f.sort ~ %s
                  GROUP BY id
               )
               UPDATE ir_filters f
                  SET sort = t.sort
                 FROM to_update t
                WHERE f.id = t.id
            """,
            [(field, field + " desc"), model, r"\y{}\y".format(field)],
        )


def _remove_field_from_context(context, fieldname):
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
            changed |= _remove_field_from_context(context[vt], fieldname)

    return changed


def remove_field(cr, model, fieldname, cascade=False, drop_column=True, skip_inherit=(), keep_as_attachments=False):
    """
    Remove a field and its references from the database.

    This function also removes the field from inheriting models, unless exceptions are
    specified in `skip_inherit`. When the field is stored we can choose to not drop the
    column.

    :param str model: model name of the field to remove
    :param str fieldname: name of the field to remove
    :param bool cascade: whether the field column(s) are removed in `CASCADE` mode
    :param bool drop_column: whether the field's column is dropped
    :param list(str) or str skip_inherit: list of inheriting models to skip the removal
                                          of the field, use `"*"` to skip all
    :param bool keep_as_attachments: for binary fields, whether the data should be kept
                                     as attachments
    """
    _validate_model(model)

    ENVIRON["__renamed_fields"][model][fieldname] = None

    # clean dashboard's contexts
    for id_, action in _dashboard_actions(cr, r"\y{}\y".format(fieldname), model):
        context = safe_eval(action.get("context", "{}"))
        changed = _remove_field_from_context(context, fieldname)
        action.set("context", unicode(context))
        if changed:
            add_to_migration_reports(
                ("ir.ui.view.custom", id_, action.get("string", "ir.ui.view.custom")), "Filters/Dashboards"
            )

    _remove_field_from_filters(cr, model, fieldname)

    _remove_import_export_paths(cr, model, fieldname)

    def adapter(leaf, is_or, negated):
        # replace by TRUE_LEAF, unless negated or in a OR operation but not negated
        if is_or ^ negated:
            return [FALSE_LEAF]
        return [TRUE_LEAF]

    related = None
    if column_exists(cr, "ir_model_fields", "related"):
        cr.execute("SELECT related FROM ir_model_fields WHERE model=%s AND name=%s", [model, fieldname])
        if cr.rowcount:
            related = cr.fetchone()[0]

    if related:
        update_field_usage(cr, model, fieldname, related, skip_inherit=skip_inherit)
    else:
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
            "SELECT id,model,depends,COALESCE(compute,''),name FROM ir_model_fields WHERE state='manual' AND depends ~ %s",
            [r"\m{}\M".format(fieldname)],
        )
        for id, from_model, deps, compute_code, dep_field_name in cr.fetchall():
            parts = []
            for part in deps.split(","):
                path = part.strip().split(".")
                if not any(
                    path[i] == fieldname and _valid_path_to(cr, path[:i], from_model, model) for i in range(len(path))
                ):
                    parts.append(part)
                elif fieldname in compute_code:
                    _logger.warning(
                        "Field %s.%s depends on removed field %s.%s and its compute code references it, "
                        "this may lead to errors.",
                        from_model,
                        dep_field_name,
                        model,
                        fieldname,
                    )

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
                defaults = dict(literal_eval(defaults_s))
            except Exception:
                continue
            defaults.pop(fieldname, None)
            cr.execute("UPDATE mail_alias SET alias_defaults = %s WHERE id = %s", [repr(defaults), alias_id])

    table = table_of_model(cr, model)
    # NOTE table_exists is needed to avoid altering views
    stored = table_exists(cr, table) and column_exists(cr, table, fieldname)

    if keep_as_attachments:
        if stored and column_type(cr, table, fieldname) == "bytea":
            _extract_data_as_attachment(cr, model, fieldname, set_res_field=False)
        if column_exists(cr, "ir_attachment", "res_field"):
            query = cr.mogrify(
                "UPDATE ir_attachment SET res_field = NULL WHERE res_model = %s AND res_field = %s", [model, fieldname]
            ).decode()
            explode_execute(cr, query, table="ir_attachment")

    elif column_exists(cr, "ir_attachment", "res_field"):
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
    if drop_column and stored:
        remove_column(cr, table, fieldname, cascade=cascade)

    # relation_field_id is a FK with ON DELETE CASCADE
    if column_exists(cr, "ir_model_fields", "relation_field_id"):
        cr.execute(
            """
            SELECT d.model,
                   d.name
              FROM ir_model_fields d
              JOIN ir_model_fields f
                ON d.relation_field_id = f.id
             WHERE f.model = %s
               AND f.name = %s
               AND d.ttype = 'one2many'
               AND d.state = 'manual'
            """,
            [model, fieldname],
        )
        for rel_model, rel_field in cr.fetchall():
            _logger.info("Cascade removing one2many field %s.%s", rel_model, rel_field)
            remove_field(cr, rel_model, rel_field)

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

    # remove field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        remove_field(
            cr,
            inh.model,
            fieldname,
            cascade=cascade,
            drop_column=drop_column,
            skip_inherit=skip_inherit,
            keep_as_attachments=keep_as_attachments,
        )


def make_field_non_stored(cr, model, field, selectable=False):
    """
    Convert field to non-stored.

    :param str model: model name of the field to convert
    :param str fieldname: name of the field to convert
    :param bool selectable: whether the field is still selectable, if True custom `ir.filters` are not updated
    """
    _validate_model(model)

    remove_column(cr, table_of_model(cr, model), field)
    if not selectable:
        _remove_field_from_filters(cr, model, field)


def remove_field_metadata(cr, model, fieldname, skip_inherit=()):
    """
    Remove metadata of a field.

    Due to a bug of the ORM [1], mixins doesn't create/register xmlids for fields created in children models
    Thus, when a field is no more defined in a child model, their xmlids should be removed explicitly to
    avoid the fields to be considered as missing and being removed at the end of the upgrade.

    [1] https://github.com/odoo/odoo/issues/49354

    :meta private: exclude from online docs
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
    """
    Move a field from one module to another.

    This functions updates all references to a specific field, switching from the source
    module to the destination module. It avoid data removal after the registry is fully
    loaded. The field in inheriting models are also moved unless skipped.

    :param str model: name of the owner model of the field to move
    :param str fieldname: name of the field to move
    :param str old_module: source module name from which the field is moved
    :param str new_module: target module name into which the field is moved
    :param list(str) or str skip_inherit: list of inheriting models for which the field is
                                          not to be moved, use `"*"` to skip all
    """
    _validate_model(model)
    name = IMD_FIELD_PATTERN % (model.replace(".", "_"), fieldname)
    columns = get_columns(cr, "ir_model_data", ignore=["id", "module"])
    query = format_query(
        cr,
        """
            INSERT INTO ir_model_data({0}, module)
                 SELECT {0}, %s
                   FROM ir_model_data
                  WHERE model = 'ir.model.fields'
                    AND name = %s
                    AND module = %s
            ON CONFLICT DO NOTHING
        """,
        columns,
    )
    cr.execute(query, [new_module, name, old_module])
    cr.execute(
        "DELETE FROM ir_model_data WHERE model = 'ir.model.fields' AND name = %s AND module = %s",
        [name, old_module],
    )
    # move field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        move_field_to_module(cr, inh.model, fieldname, old_module, new_module, skip_inherit=skip_inherit)


def rename_field(cr, model, old, new, update_references=True, domain_adapter=None, skip_inherit=()):
    """
    Rename a field and its references from `old` to `new` on the given `model`.

    The field is updated in all inheriting models, except for models specified in
    `skip_inherit`.

    This functions also updates references, indirect or direct ones, including filters,
    server actions, related fields, emails, dashboards, domains, and many more. See
    :func:`~odoo.upgrade.util.fields.update_field_usage`

    For the update of domains a special adapter function can be used. The default adapter
    just replaces `old` by `new` in each domain leaf. Refer to
    :func:`~odoo.upgrade.util.domains.adapt_domains` for information about domain adapters.

    :param str model: model name of the field to rename
    :param str old: current name of the field to rename
    :param str new: new name of the field to rename
    :param bool update_references: whether to update all references
    :param function domain_adapter: adapter to use for domains, see
                                    :func:`~odoo.upgrade.util.domains.adapt_domains`
    :param list(str) or str skip_inherit: models to skip when renaming the field in
                                          inheriting models, use `"*"` to skip all
    """
    _validate_model(model)

    rf = ENVIRON["__renamed_fields"][model]
    rf[new] = rf.pop(old, old)

    if update_references:
        # skip all inherit, they will be handled by the recursive call
        update_field_usage(cr, model, old, new, domain_adapter=domain_adapter, skip_inherit="*")

    # search for an existing custom field.
    cr.execute("SELECT 1 FROM ir_model_fields WHERE model = %s AND name IN %s", [model, (old, new)])
    if cr.rowcount == 2:
        # If a field with the same name already exists for this model (e.g. due to a custom module),
        # rename it to avoid clashing and warn the customer
        custom_name = new + "_custom"
        rename_field(cr, model, new, custom_name, update_references, domain_adapter=None, skip_inherit=skip_inherit)
        msg = (
            "The field %r from the model %r is now a standard Odoo field, but it already existed in the database "
            "(coming from a non-standard module) and thus has been renamed to %r" % (new, model, custom_name)
        )
        add_to_migration_reports(msg, "Non-standard fields")

    cr.execute("UPDATE ir_model_fields SET name=%s WHERE model=%s AND name=%s RETURNING id", (new, model, old))
    [fid] = cr.fetchone() or [None]

    if fid:
        # recreate the xmlids
        name = IMD_FIELD_PATTERN % (model.replace(".", "_"), new)

        cr.execute(
            """
             DELETE FROM ir_model_data
                   WHERE model = 'ir.model.fields'
                     AND res_id = %s
                     AND module != '__export__'
               RETURNING module
            """,
            [fid],
        )
        if cr.rowcount:
            modules = {mod for (mod,) in cr.fetchall()}

            # match noupdate value with implementation which changed with commit
            # https://github.com/odoo/odoo/commit/a99e04dfeeef11a781b35574dd625e4007ab5654
            noupdate = False if version_gte("saas~11.5") else None

            cr.execute(
                """
                INSERT INTO ir_model_data(module, name, model, res_id, noupdate)
                     SELECT unnest(%s), %s, 'ir.model.fields', %s, %s
                ON CONFLICT DO NOTHING
                  RETURNING module
                """,
                [list(modules), name, fid, noupdate],
            )
            conflicted = modules - {mod for (mod,) in cr.fetchall()}
            if conflicted:
                # Duplicated key. May happen due to some_model.sub_id and some_model_sub.id
                # both leading to some_model_sub_id xmlid before saas~11.2 where the pattern
                # was fixed to be some_module__sub_id and some_model_sub__id respectively.
                on_conflict_name = "{}_{}".format(name, fid)
                cr.execute(
                    """
                    INSERT INTO ir_model_data(module, name, model, res_id, noupdate)
                         SELECT unnest(%s), %s, 'ir.model.fields', %s, %s
                    """,
                    [list(conflicted), on_conflict_name, fid, noupdate],
                )

        if table_exists(cr, "ir_property"):
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
        cr.execute('ALTER INDEX IF EXISTS "{0}" RENAME TO "{1}"'.format(old_index_name, new_index_name))

    # rename field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        rename_field(cr, inh.model, old, new, update_references=update_references, skip_inherit=skip_inherit)


def invert_boolean_field(cr, model, old, new, skip_inherit=()):
    """Rename a boolean field and invert its value."""
    _validate_model(model)
    table = table_of_model(cr, model)
    if column_type(cr, table, old) != "bool":
        raise ValueError("Field {!r} of model {!r} is not a stored boolean field".format(old, model))

    def _adapter(leaf, _or, _neg):
        return ["!", leaf]

    if old == new:
        # same name but inverted value, just adapt domains
        adapt_domains(cr, model, old, new, adapter=_adapter, skip_inherit=skip_inherit, force_adapt=True)
    else:
        rename_field(cr, model, old, new, update_references=True, domain_adapter=_adapter, skip_inherit=skip_inherit)

    query = format_query(cr, "UPDATE {t} SET {c} = {c} IS NOT TRUE", t=table, c=new)
    explode_execute(cr, query, table=table)


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

    # Update ir.default
    if table_exists(cr, "ir_default"):
        json_value_html = pg_text2html("json_value::jsonb->>0")
        query = """
                UPDATE ir_default AS d
                   SET json_value = to_json({})::text
                  FROM ir_model_fields AS imf
                 WHERE imf.name = %s
                   AND imf.model = %s
                   AND imf.id = d.field_id
                """
        cr.execute(format_query(cr, query, sql.SQL(json_value_html)), [field, model])
    for inh in for_each_inherit(cr, model, skip_inherit):
        convert_field_to_html(cr, inh.model, field, skip_inherit=skip_inherit)


def convert_m2o_field_to_m2m(cr, model, field, new_name=None, m2m_table=None, col1=None, col2=None):
    """
    Convert a Many2one field to a Many2many.

    It creates the relation table and fill it.

    :param str model: model name of the field to convert
    :param str field: current name of the field to convert
    :param str new_name: new name of the field to convert.
                         If not provide, it's `field` with a trailing "s".
    :param str m2m_table: name of the relation table. Automatically generated if not provided.
    :param str col1: name of the column referencing `model`.
    :param str col2: name of the column referencing the target of `field`.
    """
    _validate_model(model)
    if new_name is None:
        if not field.endswith("_id"):
            raise ValueError("Please specify the new name explicitly")
        new_name = field + "s"

    table1 = table_of_model(cr, model)
    table2, _, _ = target_of(cr, table1, field)

    if col1 is None:
        col1 = "{}_id".format(table1)
    if col2 is None:
        col2 = "{}_id".format(table2)

    m2m_table = create_m2m(cr, m2m_table or AUTO, table1, table2, col1, col2)

    dedup = SQLStr("ON CONFLICT DO NOTHING")
    if cr._cnx.server_version < 90500:
        # Approximate version for older PG versions
        dedup = format_query(
            cr,
            """
                EXCEPT
                SELECT {col1}, {col2}
                  FROM {m2m}
            """,
            m2m=m2m_table,
            col1=col1,
            col2=col2,
        )

    fill_query = format_query(
        cr,
        """
            INSERT INTO {m2m}({col1}, {col2})
                 SELECT id, {field}
                   FROM {table}
                  WHERE {field} IS NOT NULL
                {dedup}
        """,
        m2m=m2m_table,
        col1=col1,
        col2=col2,
        field=field,
        table=table1,
        dedup=dedup,
    )
    cr.execute(fill_query)
    remove_column(cr, table1, field)
    rename_field(cr, model, field, new_name)


# Because we can.
m2o2m2m = convert_m2o_field_to_m2m


def _convert_field_to_company_dependent(
    cr, model, field, type, target_model=None, default_value=None, default_value_ref=None, company_field="company_id"
):
    _validate_model(model)
    if target_model:
        _validate_model(target_model)

    type2field = {"char", "float", "boolean", "integer", "text", "many2one", "date", "datetime", "selection", "html"}
    assert type in type2field

    table = table_of_model(cr, model)

    # update "ir_model_fields"."company_dependent" as an identifier for indirect reference
    cr.execute(
        """
        UPDATE ir_model_fields
           SET company_dependent = TRUE
         WHERE model = %s
           AND name = %s
     RETURNING id
        """,
        (model, field),
    )
    if not cr.rowcount:
        # no ir_model_fields, no column
        remove_column(cr, table, field, cascade=True)
        return
    [field_id] = cr.fetchone()

    if default_value is None:  # noqa: SIM108
        where_condition = "{0} IS NOT NULL"
    else:
        where_condition = cr.mogrify("{0} != %s", [default_value]).decode()

    if company_field:
        where_condition += format_query(cr, " AND {} IS NOT NULL", company_field)
        using = format_query(cr, "jsonb_build_object({}, {{0}})", company_field)
    else:
        using = format_query(
            cr,
            "(SELECT jsonb_object_agg(c.id::text, {}.{{0}}) FROM res_company c)",
            table,
        )
    alter_column_type(cr, table, field, "jsonb", using=using, where=where_condition)

    # delete all old default
    cr.execute("DELETE FROM ir_default WHERE field_id = %s", [field_id])

    # add fallback
    if default_value:
        cr.execute(
            "INSERT INTO ir_default(field_id, json_value) VALUES (%s, %s) RETURNING id",
            (field_id, json.dumps(default_value)),
        )
        [default_id] = cr.fetchone()
        if default_value_ref:
            module, _, xid = default_value_ref.partition(".")
            cr.execute(
                """
                INSERT INTO ir_model_data(module, name, model, res_id, noupdate)
                     VALUES (%s, %s, 'ir.default', %s, True)
                """,
                [module, xid, default_id],
            )


def _convert_field_to_property(
    cr, model, field, type, target_model=None, default_value=None, default_value_ref=None, company_field="company_id"
):
    """
    Convert a field to a property field.

    Notes
    -----
        `target_model` is only use when `type` is "many2one".
        The `company_field` can be an sql expression.
            You may use `t` to refer the model's table.

    :meta private: exclude from online docs

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
        # for m2o, the store value is a reference field, so in format `model,id`
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
if version_gte("saas~17.5"):
    make_field_company_dependent = _convert_field_to_company_dependent
else:
    make_field_company_dependent = _convert_field_to_property

# retro-compatibility
convert_field_to_property = make_field_company_dependent


def convert_binary_field_to_attachment(cr, model, field, encoded=True, name_field=None):
    _extract_data_as_attachment(cr, model, field, encoded, name_field)
    # free PG space
    table = table_of_model(cr, model)
    remove_column(cr, table, field)


def _extract_data_as_attachment(cr, model, field, encoded=True, name_field=None, set_res_field=True):
    _validate_model(model)
    table = table_of_model(cr, model)
    if not column_exists(cr, table, field):
        return
    name_query = cr.mogrify(
        format_query(cr, "COALESCE({}, CONCAT(%s, '(', id, ').', %s))", name_field if name_field else SQLStr("NULL")),
        [model.title().replace(".", ""), field],
    ).decode()

    if not column_exists(cr, "ir_attachment", "res_field"):
        res_field_query = SQLStr("")
    elif set_res_field:
        res_field_query = SQLStr(cr.mogrify(", res_field = %s", [field]).decode())
    else:
        res_field_query = SQLStr(", res_field = NULL")

    update_query = format_query(
        cr,
        """
           UPDATE ir_attachment
              SET res_model = %s,
                  res_id = %s
                  {}
            WHERE id = %s
        """,
        res_field_query,
    )

    cr.execute(format_query(cr, "SELECT count(*) FROM {} WHERE {} IS NOT NULL", table, field))
    [count] = cr.fetchone()
    if count == 0:
        return

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

        cr.execute(update_query, [model, rid, att.id])
        invalidate(att)

    iter_cur.close()


if version_gte("16.0"):

    def convert_field_to_translatable(cr, model, field):
        table = table_of_model(cr, model)
        ctype = column_type(cr, table, field)
        if not ctype or ctype == "jsonb":
            return
        alter_column_type(cr, table, field, "jsonb", "jsonb_build_object('en_US', {0})")

    def convert_field_to_untranslatable(cr, model, field, type="varchar"):
        assert type in ("varchar", "text")
        table = table_of_model(cr, model)
        ctype = column_type(cr, table, field)
        if not ctype or ctype != "jsonb":
            return
        alter_column_type(cr, table, field, type, "{0}->>'en_US'")

else:
    # Older versions, these functions are no-op
    def convert_field_to_translatable(cr, model, field):
        pass

    def convert_field_to_untranslatable(cr, model, field, type="varchar"):
        pass


def change_field_selection_values(cr, model, field, mapping, skip_inherit=()):
    """
    Replace references of values of a selection field.

    This function replaces all references to selection values according to a mapping.
    Domains are also updated.

    :param str model: model name of the selection field to update
    :param str field: name of the selection field to update
    :param dict mapping: selection values to update, key values are replaced by
                                            their corresponding values in the mapping
    :param list(str) or str skip_inherit: list of inheriting models to skip in the update
                                          of the selection values, use `"*"` to skip all
    """
    _validate_model(model)
    if not mapping:
        return
    table = table_of_model(cr, model)

    if column_exists(cr, table, field):
        ctype = column_type(cr, table, field)
        if ctype == "varchar":
            query = "UPDATE {table} t SET {column} = %(json)s::jsonb->>t.{column} WHERE t.{column} = ANY(%(keys)s)"
        elif ctype == "jsonb":
            # company dependent selection field
            query = """
                WITH upd AS (
                     SELECT t.id,
                            jsonb_object_agg(v.key, COALESCE(%(json)s::jsonb->>v.value, v.value)) AS value
                       FROM {table} t
                       JOIN LATERAL jsonb_each_text(t.{column}) v
                         ON true
                      WHERE jsonb_path_query_array(t.{column}, '$.*') ?| %(keys)s
                        AND {{parallel_filter}}
                      GROUP BY t.id
                )
                UPDATE {table} t
                   SET {column} = upd.value
                  FROM upd
                 WHERE upd.id = t.id
            """
        else:
            raise UpgradeError("unsupported column type for selection field: {}".format(ctype))

        data = {
            "keys": list(mapping),
            "json": json.dumps(mapping),
        }
        queries = [
            cr.mogrify(q, data).decode()
            for q in explode_query_range(cr, format_query(cr, query, table=table, column=field), table=table, alias="t")
        ]
        parallel_execute(cr, queries)

    elif table_exists(cr, "ir_property"):
        cr.execute("SELECT id FROM ir_model_fields WHERE model=%s AND name=%s", [model, field])
        if cr.rowcount:
            fields_id = cr.fetchone()[0]
            query = """
                UPDATE ir_property
                   SET value_text = %(json)s::jsonb->>value_text
                 WHERE value_text IN %(keys)s
                   AND fields_id = %(fields_id)s
            """
            data = {
                "keys": tuple(mapping),
                "json": json.dumps(mapping),
                "fields_id": fields_id,
            }
            queries = [cr.mogrify(q, data).decode() for q in explode_query_range(cr, query, table="ir_property")]
            parallel_execute(cr, queries)

    if table_exists(cr, "ir_model_fields_selection"):
        cr.execute(
            """
            DELETE FROM ir_model_fields_selection s
                  USING ir_model_fields f
                  WHERE f.id = s.field_id
                    AND f.model = %s
                    AND f.name = %s
                    AND s.value = ANY(%s)
            """,
            [model, field, [k for k in mapping if k not in mapping.values()]],
        )

    if table_exists(cr, "ir_default"):
        query = """
            UPDATE ir_default d
               SET json_value = (%(json)s::jsonb->>d.json_value)
              FROM ir_model_fields f
             WHERE d.field_id = f.id
               AND f.model = %(model)s
               AND f.name = %(name)s
               AND d.json_value IN %(keys)s
        """
        dumped_map = {json.dumps(k): json.dumps(v) for k, v in mapping.items()}
    else:
        query = """
            UPDATE ir_values
               SET value = %(json)s::jsonb->>value
             WHERE model = %(model)s
               AND name = %(name)s
               AND key = 'default'
               AND value IN %(keys)s
        """
        dumped_map = {pickle.dumps(k): pickle.dumps(v) for k, v in mapping.items()}

    data = {
        "keys": tuple(dumped_map),
        "json": json.dumps(dumped_map),
        "model": model,
        "name": field,
    }
    cr.execute(query, data)

    def adapter(leaf, _or, _neg):
        left, op, right = leaf
        if isinstance(right, (tuple, list)):  # noqa: SIM108
            right = [mapping.get(r, r) for r in right]
        else:
            right = mapping.get(right, right)
        return [(left, op, right)]

    # skip all inherit, they will be handled by the recursive call
    adapt_domains(cr, model, field, field, adapter=adapter, skip_inherit="*")

    # rename field on inherits
    for inh in for_each_inherit(cr, model, skip_inherit):
        change_field_selection_values(cr, inh.model, field, mapping=mapping, skip_inherit=skip_inherit)


def is_field_anonymized(cr, model, field):
    from .modules import module_installed  # noqa: PLC0415

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
    Replace all references to the field `old` by `new` in different places.

    Search in:
        - ir_filters
        - ir_exports_line
        - ir_act_server
        - mail_alias
        - ir_ui_view_custom (dashboard)
        - domains (using `domain_adapter`)
        - related fields

    This function can be used to replace the usage of a field by another. Domains are
    updated using the `domain_adapter`. By default the domain adapter just replaces `old`
    by `new` in domain leaves. See :func:`~odoo.upgrade.util.domains.adapt_domains` for
    more information about domain adapters.

    :param str model: model name of the field
    :param str old: source name of the field to replace
    :param str new: target name of the field to set
    :param function domain_adapter: adapter to use for domains, see
                                    :func:`~odoo.upgrade.util.domains.adapt_domains`
    :param list(str) or str skip_inherit: models to skip when renaming the field in
                                          inheriting models, use `"*"` to skip all
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


def _update_impex_renamed_fields_paths(cr, old_field_name, new_field_name, only_models):
    export_q = cr.mogrify(
        """
        SELECT el.id,
               e.resource,
               STRING_TO_ARRAY(el.name, '/')
          FROM ir_exports_line el
          JOIN ir_exports e
            ON el.export_id = e.id
         WHERE el.name ~ %s
        """,
        [r"\y{}\y".format(old_field_name)],
    ).decode()
    impex_data = [(export_q, "ir_exports_line", "name")]
    if table_exists(cr, "base_import_mapping"):
        import_q = cr.mogrify(
            """
            SELECT id,
                   res_model,
                   STRING_TO_ARRAY(field_name, '/')
              FROM base_import_mapping
             WHERE field_name ~ %s
            """,
            [r"\y{}\y".format(old_field_name)],
        ).decode()
        impex_data.append((import_q, "base_import_mapping", "field_name"))

    for query, table, column in impex_data:
        cr.execute(query)
        if not cr.rowcount:
            continue
        fixed_paths = {}
        for record_id, related_model, path in cr.fetchall():
            has_id = path[-1] == ".id"
            if has_id:
                path = path[:-1]  # noqa: PLW2901

            new_path = [
                new_field_name
                if field.field_name == old_field_name and field.field_model in only_models
                else field.field_name
                for field in resolve_model_fields_path(cr, related_model, path)
            ]
            if len(new_path) == len(path) and new_path != path:
                if has_id:
                    new_path.append(".id")
                fixed_paths[record_id] = "/".join(new_path)
        if fixed_paths:
            cr.execute(
                format_query(cr, "UPDATE {} SET {} = (%s::jsonb)->>(id::text) WHERE id IN %s", table, column),
                [Json(fixed_paths), tuple(fixed_paths)],
            )


def _update_field_usage_multi(cr, models, old, new, domain_adapter=None, skip_inherit=()):
    assert models
    only_models = None if models == "*" else tuple(models)

    if only_models:
        for model in only_models:
            _validate_model(model)

    p = {
        "old": PGRegexp(r"\y{}\y".format(re.escape(old))),
        "old_pattern": PGRegexp(r"""[.'"]{0}\y""".format(re.escape(old))),
        "new": new,
        "def_old": PGRegexp(r"\ydefault_{}\y".format(re.escape(old))),
        "def_new": "default_{}".format(new),
        "models": tuple(only_models) if only_models else (),
    }

    # ir.action.server
    if column_exists(cr, "ir_act_server", "update_path") and only_models:
        cr.execute(
            """
            SELECT a.id,
                   a.update_path,
                   m.model
              FROM ir_act_server a
              JOIN ir_model m
                ON a.model_id = m.id
             WHERE a.state = 'object_write'
               AND a.update_path ~ %(old)s
            """,
            p,
        )
        to_update = {}
        for act_id, update_path, src_model in cr.fetchall():
            for dst_model in only_models:
                new_path = _replace_path(cr, old, new, src_model, dst_model, update_path)
                if new_path != update_path:
                    to_update[act_id] = new_path
        if to_update:
            cr.execute(
                "UPDATE ir_act_server SET update_path = %s::jsonb->>id::text WHERE id IN %s",
                [Json(to_update), tuple(to_update)],
            )

    # Modifying server action is dangerous.
    # The search pattern can be anywhere in the code, leading to invalid codes.
    # Moreover, limiting to some models will ignore some SA that should be modified.
    # Just search for the potential SA that need update.
    col_prefix = ""
    if not column_exists(cr, "ir_act_server", "condition"):
        col_prefix = "--"  # sql comment the line
    standard_modules = set(modules.get_modules()) | {"__upgrade__"}
    q = """
        SELECT a.id, a.{name}
          FROM ir_act_server a
     LEFT JOIN ir_model_data d
            ON d.res_id = a.id
           AND d.model = 'ir.actions.server'
         WHERE a.state = 'code'
           AND (a.code ~ %(old_pattern)s
                {col_prefix} OR a.condition ~ %(old)s
               )
           AND (  d.module IS NULL -- custom action
               OR d.module NOT IN %(standard_modules)s
               OR (d.module IN %(standard_modules)s AND d.noupdate)
               )
    """.format(col_prefix=col_prefix, name=get_value_or_en_translation(cr, "ir_act_server", "name"))
    cr.execute(q, {"old_pattern": p["old_pattern"], "old": p["old"], "standard_modules": tuple(standard_modules)})
    li = ""
    if cr.rowcount:
        li = "".join(
            "<li>{}</li>".format(get_anchor_link_to_record("ir.actions.server", aid, aname))
            for aid, aname in cr.fetchall()
        )

    if column_exists(cr, "ir_model_fields", "compute"):
        # Modifying compute methods is similarly as dangerous.
        # Only report potential compute methods that may need an update.
        q = """
            SELECT f.id, f.name
              FROM ir_model_fields f
         LEFT JOIN ir_model_data d
                ON d.res_id = f.id
               AND d.model = 'ir.model.fields'
             WHERE f.compute ~ %(old_pattern)s
               AND (  d.module IS NULL -- custom action
                   OR d.module NOT IN %(standard_modules)s
                   OR (d.module IN %(standard_modules)s AND d.noupdate)
                   )
            """
        cr.execute(q, {"old_pattern": p["old_pattern"], "standard_modules": tuple(standard_modules)})
        if cr.rowcount:
            li += "".join(
                "<li>{}</li>".format(get_anchor_link_to_record("ir.model.fields", fid, fname))
                for fid, fname in cr.fetchall()
            )
    if li:
        model_text = "All models"
        if only_models:
            model_text = "Models " + ", ".join("<kbd>{}</kbd>".format(m) for m in only_models)
        add_to_migration_reports(
            """
<details>
  <summary>
    {model_text}: the field <kbd>{old}</kbd> has been renamed to <kbd>{new}</kbd>. The following server actions and compute methods of other fields may need an update:
    If a server action or a field is a standard one and you haven't made any modifications, you may ignore them.
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

        q = format_query(
            cr,
            """
            UPDATE ir_filters
               SET {col_prefix} sort = {sort_repl},
                   context = {context_repl}
             WHERE {cond}
               AND (
                context ~ %(old)s
                OR context ~ %(def_old)s
                {col_prefix} OR sort ~ %(old)s
               )
            """,
            col_prefix=SQLStr(col_prefix),
            sort_repl=pg_replace("sort", [(p["old"], p["new"])]),
            context_repl=pg_replace("context", [(p["old"], p["new"]), (p["def_old"], p["def_new"])]),
            cond=SQLStr("model_id IN %(models)s") if only_models else SQLStr("true"),
        )
        cr.execute(q, p)

        # ir.exports.line, base_import.mapping # noqa
        if only_models:
            _update_impex_renamed_fields_paths(cr, old, new, only_models)

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
            context = safe_eval(act.get("context", "{}"))
            adapt_dict(context)

            if def_old in context:
                context[def_new] = context.pop(def_old)
            act.set("context", unicode(context))

    # domains, related and inhited models
    if only_models:
        for model in only_models:
            # skip all inherit, they will be handled by the recursive call
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
    for id_, field_model, depends in cr.fetchall():
        temp_depends = depends.split(",")
        for i in range(len(temp_depends)):
            domain = _adapt_one_domain(
                cr, target_model, old, new, field_model, [(temp_depends[i], "=", "depends")], force_adapt=True
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
    for id_, field_model, related in cr.fetchall():
        domain = _adapt_one_domain(
            cr, target_model, old, new, field_model, [(related, "=", "related")], force_adapt=True
        )
        if domain:
            cr.execute("UPDATE ir_model_fields SET related = %s WHERE id = %s", [domain[0][0], id_])

    # TODO adapt paths in email templates?

    # down on inherits
    for inh in for_each_inherit(cr, target_model, skip_inherit):
        adapt_related(cr, inh.model, old, new, skip_inherit=skip_inherit)


def update_server_actions_fields(cr, src_model, dst_model=None, fields_mapping=None):
    """
    Update server action fields.

    When some fields of `src_model` have ben copied to `dst_model` and/or have
    been copied to fields with another name, some references have to be moved.

    For server actions, this function starts by updating `ir_server_object_lines`
    to refer to new fields. If no `fields_mapping` is provided, all references to
    fields that exist in both models (source and destination) are moved. If no `dst_model`
    is given, the `src_model` is used as `dst_model`.
    Then, if `dst_model` is set, `ir_act_server` referred by modified `ir_server_object_lines`
    are also updated. A chatter message informs the customer about this modification.

    :meta private: exclude from online docs
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
