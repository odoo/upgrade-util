# -*- coding: utf-8 -*-
"""
Utility functions for modifying models.

Model operations are best done in `pre-` script of the involved modules.
"""

import logging
import re

from . import json
from .const import ENVIRON
from .fields import IMD_FIELD_PATTERN, remove_field
from .helpers import _ir_values_value, _validate_model, model_of_table, table_of_model
from .indirect_references import indirect_references
from .inherit import for_each_inherit, inherit_parents
from .misc import _cached, chunks, log_progress
from .pg import (
    _get_unique_indexes_with,
    column_exists,
    column_type,
    column_updatable,
    explode_execute,
    explode_query_range,
    format_query,
    get_m2m_tables,
    get_value_or_en_translation,
    parallel_execute,
    table_exists,
    view_exists,
)

# avoid namespace clash
from .pg import rename_table as pg_rename_table
from .records import _remove_import_export_paths, _rm_refs, remove_records, remove_view, replace_record_references_batch
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)


@_cached
def _unknown_model_id(cr):
    order = column_exists(cr, "ir_model", "order")
    extra_columns = ', "order"' if order else ""
    extra_values = ", 'id'" if order else ""
    name_value = (
        "jsonb_build_object('en_US', 'Unknown')" if column_type(cr, "ir_model", "name") == "jsonb" else "'Unknown'"
    )
    cr.execute(
        """
            INSERT INTO ir_model(name, model{})
                 SELECT {}, '_unknown'{}
                  WHERE NOT EXISTS (SELECT 1 FROM ir_model WHERE model = '_unknown')
        """.format(extra_columns, name_value, extra_values)
    )
    cr.execute("SELECT id FROM ir_model WHERE model = '_unknown'")
    return cr.fetchone()[0]


def remove_model(cr, model, drop_table=True, ignore_m2m=()):
    """
    Remove a model and its references from the database.

    Some required indirect references to the model are replaced by the *unknown model* -
    an empty model that serves as placeholder for gone models.

    :param str model: name of the model to remove
    :param bool drop_table: whether to drop the table of this model
    :param list(str) or str ignore_m2m: list of m2m tables to ignore - not removed, use
                                        `"*"` to ignore (keep) all m2m tables
    """
    _validate_model(model)
    model_underscore = model.replace(".", "_")
    chunk_size = 1000
    notify = False
    unk_id = _unknown_model_id(cr)

    # remove references
    for ir in indirect_references(cr):
        if ir.company_dependent_comodel:
            if ir.company_dependent_comodel == "ir.model":
                # clean on delete set null
                cr.execute("SELECT id FROM ir_model WHERE name = %s", [model])
                [mod_id] = cr.fetchone() or [None]
                if mod_id:
                    cr.execute(
                        format_query(
                            """
                            UPDATE {table}
                               SET {field} = (
                                   SELECT jsonb_object_agg(
                                          key,
                                          CASE
                                              WHEN value::int4 = %s THEN NULL
                                              ELSE value::int4
                                          END)
                                     FROM jsonb_each_text({field})
                                   )
                             WHERE {field} IS NOT NULL
                               AND {field} @? %s
                            """,
                            table=ir.table,
                            field=ir.res_id,
                        ),
                        [mod_id, "$.* ? (@ == {})".format(mod_id)],
                    )
            continue
        if ir.table in ("ir_model", "ir_model_fields", "ir_model_data"):
            continue
        ref_model = model_of_table(cr, ir.table)

        query = cr.mogrify(
            """
                SELECT d.res_id
                  FROM ir_model_data d
                  JOIN "{}" r ON d.model = %s AND d.res_id = r.id
                 WHERE d.module != '__export__'
                   AND {}
            """.format(ir.table, ir.model_filter(prefix="r.")),
            [ref_model, model],
        ).decode()
        if not ir.set_unknown:
            # If not set_unknown, all records should be deleted.
            query = cr.mogrify(
                'SELECT id FROM "{}" r WHERE {}'.format(ir.table, ir.model_filter(prefix="r.")), [model]
            ).decode()

        cr.execute(query)
        if ir.table == "ir_ui_view":
            for (view_id,) in cr.fetchall():
                remove_view(cr, view_id=view_id, silent=True)
        else:
            # remove in batch
            size = (cr.rowcount + chunk_size - 1) / chunk_size
            it = chunks([id for (id,) in cr.fetchall()], chunk_size, fmt=tuple)
            for sub_ids in log_progress(it, _logger, qualifier=ir.table, size=size):
                remove_records(cr, ref_model, sub_ids)
                _rm_refs(cr, ref_model, sub_ids)

        if ir.set_unknown:
            # Link remaining records not linked to a XMLID
            query = cr.mogrify(
                'SELECT id FROM "{}" r WHERE {}'.format(ir.table, ir.model_filter(prefix="r.")), [model]
            ).decode()
            cr.execute(query)
            ids = [id for (id,) in cr.fetchall()]
            if ids:
                # link to `_unknown` model
                sets, args = zip(
                    *[
                        ('"{}" = %s'.format(c), v)
                        for c, v in [(ir.res_model, "_unknown"), (ir.res_model_id, unk_id)]
                        if c
                    ]
                )

                query = 'UPDATE "{}" SET {} WHERE id IN %s'.format(ir.table, ",".join(sets))
                cr.execute(query, args + (tuple(ids),))
                notify = notify or bool(cr.rowcount)

    _remove_import_export_paths(cr, model)

    _rm_refs(cr, model)

    cr.execute(
        "SELECT id, {} FROM ir_model WHERE model=%s".format(get_value_or_en_translation(cr, "ir_model", "name")),
        [model],
    )
    [mod_id, mod_label] = cr.fetchone() or [None, model]
    if mod_id:
        # some required fk are in "ON DELETE SET NULL/RESTRICT".
        for tbl in "base_action_rule base_automation google_drive_config".split():
            if column_exists(cr, tbl, "model_id"):
                cr.execute("DELETE FROM {0} WHERE model_id=%s".format(tbl), [mod_id])
        cr.execute("DELETE FROM ir_model_relation WHERE model=%s", (mod_id,))

        # remove m2m tables
        if ignore_m2m != "*":
            tables = get_m2m_tables(cr, table_of_model(cr, model))
            ignore = set(ignore_m2m)
            for table_name in tables:
                if table_name in ignore:
                    _logger.info("remove_model(%r): m2m table %r explicitly ignored", model, table_name)
                    continue
                _logger.info("remove_model(%r): dropping m2m table %r", model, table_name)
                cr.execute('DROP TABLE "{}" CASCADE'.format(table_name))
                ENVIRON.setdefault("_gone_m2m", {})[table_name] = model

        cr.execute("DELETE FROM ir_model_constraint WHERE model=%s RETURNING id", (mod_id,))
        if cr.rowcount:
            ids = tuple(c for (c,) in cr.fetchall())
            cr.execute("DELETE FROM ir_model_data WHERE model = 'ir.model.constraint' AND res_id IN %s", [ids])

        # Drop XML IDs of ir.rule and ir.model.access records that will be cascade-dropped,
        # when the ir.model record is dropped - just in case they need to be re-created
        cr.execute(
            """
                DELETE
                  FROM ir_model_data x
                 USING ir_rule a
                 WHERE x.res_id = a.id
                   AND x.model = 'ir.rule'
                   AND a.model_id = %s
        """,
            [mod_id],
        )
        cr.execute(
            """
                DELETE
                  FROM ir_model_data x
                 USING ir_model_access a
                 WHERE x.res_id = a.id
                   AND x.model = 'ir.model.access'
                   AND a.model_id = %s
        """,
            [mod_id],
        )

        cr.execute("DELETE FROM ir_model WHERE id=%s", (mod_id,))

    cr.execute("DELETE FROM ir_model_data WHERE model=%s AND name=%s", ("ir.model", "model_%s" % model_underscore))
    cr.execute(
        "DELETE FROM ir_model_data WHERE model='ir.model.fields' AND name LIKE %s",
        [(IMD_FIELD_PATTERN % (model_underscore, "%")).replace("_", r"\_")],
    )
    cr.execute("DELETE FROM ir_model_data WHERE model = %s", [model])
    cr.execute("UPDATE ir_model_fields SET relation = '_unknown' WHERE relation = %s", [model])

    table = table_of_model(cr, model)
    if drop_table:
        if table_exists(cr, table):
            cr.execute('DROP TABLE "{0}" CASCADE'.format(table))
        elif view_exists(cr, table):
            cr.execute('DROP VIEW "{0}" CASCADE'.format(table))

    if notify:
        add_to_migration_reports(
            message="The model {model} ({label}) has been removed. "
            "The linked records (crons, mail templates, automated actions...)"
            " have also been removed (standard) or linked to the '_unknown' model (custom).".format(
                model=model, label=mod_label
            ),
            category="Removed Models",
        )


# compat layer...
delete_model = remove_model


def rename_model(cr, old, new, rename_table=True):
    """
    Rename a model.

    :param str old: current name of the model to rename
    :param str new: new name of the model to rename
    :param bool rename_table: whether to also rename the table of the model
    """
    _validate_model(old)
    _validate_model(new)
    if old in ENVIRON["__renamed_fields"]:
        ENVIRON["__renamed_fields"][new] = ENVIRON["__renamed_fields"].pop(old)
    if rename_table:
        old_table = table_of_model(cr, old)
        new_table = table_of_model(cr, new)
        if new_table != old_table:
            pg_rename_table(cr, old_table, new_table)

    updates = [("wkf", "osv")] if table_exists(cr, "wkf") else []
    updates += [(ir.table, ir.res_model) for ir in indirect_references(cr) if ir.res_model]

    for table, column in updates:
        cr.execute("SELECT 1 FROM {t} WHERE {c}=%s LIMIT 1".format(t=table, c=column), [old])
        if not cr.fetchone():
            continue
        query = cr.mogrify("UPDATE {t} SET {c}=%s WHERE {c}=%s".format(t=table, c=column), [new, old]).decode()
        explode_execute(cr, query, table=table)

    # "model-comma" fields
    cr.execute(
        """
        SELECT model, name
          FROM ir_model_fields
         WHERE ttype='reference'
    """
    )
    for model, column in cr.fetchall():
        table = table_of_model(cr, model)
        if column_updatable(cr, table, column):
            query = cr.mogrify(
                """
                    UPDATE "{table}"
                       SET "{column}"='{new}' || substring("{column}" FROM '%#",%#"' FOR '#')
                     WHERE "{column}" LIKE '{old},%'
            """.format(table=table, column=column, new=new, old=old)
            ).decode()
            explode_execute(cr, query, table=table)

    # defaults
    if table_exists(cr, "ir_values"):
        column_read, cast_write = _ir_values_value(cr)
        query = """
            UPDATE ir_values
               SET value = {cast[0]}'{new}' || substring({column} FROM '%#",%#"' FOR '#'){cast[2]}
             WHERE {column} LIKE '{old},%'
        """.format(column=column_read, new=new, old=old, cast=cast_write.partition("%s"))
        cr.execute(query)

    # translations
    if table_exists(cr, "ir_translation"):
        cr.execute(
            """
            WITH renames AS (
                SELECT id, type, lang, res_id, src, '{new}' || substring(name FROM '%#",%#"' FOR '#') as new
                  FROM ir_translation
                 WHERE name LIKE '{old},%'
            )
            UPDATE ir_translation t
               SET name = r.new
              FROM renames r
         LEFT JOIN ir_translation e ON (
                e.type = r.type
            AND e.lang = r.lang
            AND e.name = r.new
            AND CASE WHEN e.type = 'model' THEN e.res_id IS NOT DISTINCT FROM r.res_id
                     WHEN e.type = 'selection' THEN e.src IS NOT DISTINCT FROM r.src
                     ELSE e.res_id IS NOT DISTINCT FROM r.res_id AND e.src IS NOT DISTINCT FROM r.src
                 END
         )
             WHERE t.id = r.id
               AND e.id IS NULL
        """.format(new=new, old=old)
        )
        cr.execute("DELETE FROM ir_translation WHERE name LIKE '{},%'".format(old))

        cr.execute(
            """
            UPDATE ir_translation
               SET name=%s
             WHERE name=%s
               AND type IN ('constraint', 'sql_constraint', 'view', 'report', 'rml', 'xsl')
        """,
            [new, old],
        )

    old_u = old.replace(".", "_")
    new_u = new.replace(".", "_")

    cr.execute(
        "UPDATE ir_model_data SET name=%s WHERE model=%s AND name=%s",
        ("model_%s" % new_u, "ir.model", "model_%s" % old_u),
    )

    cr.execute(
        """
            UPDATE ir_model_data
               SET name=%s || substring(name from %s)
             WHERE model='ir.model.fields'
               AND name LIKE %s
    """,
        ["field_%s" % new_u, len(old_u) + 7, (IMD_FIELD_PATTERN % (old_u, "%")).replace("_", r"\_")],
    )

    col_prefix = ""
    if not column_exists(cr, "ir_act_server", "condition"):
        col_prefix = "--"  # sql comment the line

    cr.execute(
        r"""
        UPDATE ir_act_server
           SET {col_prefix} condition=regexp_replace(condition, '([''"]){old}\1', '\1{new}\1', 'g'),
               code=regexp_replace(code, '([''"]){old}\1', '\1{new}\1', 'g')
    """.format(col_prefix=col_prefix, old=old.replace(".", r"\."), new=new)
    )


def merge_model(cr, source, target, drop_table=True, fields_mapping=None, ignore_m2m=()):
    """
    Merge a model into another one.

    This function moves all the references from ``source`` model into ``target`` model and
    *removes* ``source`` model and its references. By default, only the fields with the
    same name in both models are moved from source to target, optionally a mapping with
    differently named fields can be provided.

    .. warning::
        This function does not move the records from ``source`` model to ``target`` model.

    :param str source: name of the model to be merged
    :param str target: name of the destination model to merge into
    :param bool drop_table: whether to drop the table of the source model
    :param dict or None fields_mapping: mapping of field names from source to target
                                        model, when `None` only fields with same name are
                                        moved
    :param list(str) or str ignore_m2m: list of m2m tables ignored to remove from source model.
    """
    _validate_model(source)
    _validate_model(target)
    cr.execute("SELECT model, id FROM ir_model WHERE model in %s", ((source, target),))
    model_ids = dict(cr.fetchall())
    mapping = {model_ids[source]: model_ids[target]}
    ignores = ["ir_model", "ir_model_fields", "ir_model_constraint", "ir_model_relation"]
    replace_record_references_batch(cr, mapping, "ir.model", replace_xmlid=False, ignores=ignores)

    # remap the fields on ir_model_fields
    cr.execute(
        """
        SELECT mf1.id,
               mf2.id
          FROM ir_model_fields mf1
          JOIN ir_model_fields mf2
            ON mf1.model=%s
           AND mf2.model=%s
           AND mf1.name=mf2.name
        """,
        [source, target],
    )
    field_ids_mapping = dict(cr.fetchall())
    if fields_mapping:
        jmap = json.dumps(fields_mapping)
        cr.execute(
            """
            SELECT mf1.name,
                   mf1.id,
                   mf2.id
              FROM ir_model_fields mf1
              JOIN ir_model_fields mf2
                ON mf1.model=%s
               AND mf2.model=%s
               AND mf2.name=(%s::jsonb ->> mf1.name::varchar)::varchar
            """,
            [source, target, jmap],
        )
        explicit_mapping = {name: (id_s, id_t) for name, id_s, id_t in cr.fetchall()}
        field_ids_mapping.update(explicit_mapping.values())
        # The target fields may not yet exists. We simply update the source fields in this case.
        missing = {s: t for s, t in fields_mapping.items() if s not in explicit_mapping}
        if missing:
            jmap = json.dumps(missing)
            cr.execute(
                """
                    UPDATE ir_model_fields
                       SET model = %s,
                           model_id = %s,
                           name = (%s::jsonb ->> name::varchar)::varchar
                     WHERE model = %s
                       AND name IN %s
                 RETURNING id
                """,
                [target, model_ids[target], jmap, source, tuple(missing)],
            )
            if cr.rowcount:
                # remove their xmlid (now invalid names). The ORM will recreate them.
                cr.execute(
                    "DELETE FROM ir_model_data WHERE model='ir.model.fields' AND res_id IN %s",
                    [sum(cr.fetchall(), ())],
                )

    if field_ids_mapping:
        ignores = ["ir_model_fields_group_rel", "ir_model_fields_selection"]
        replace_record_references_batch(cr, field_ids_mapping, "ir.model.fields", replace_xmlid=False, ignores=ignores)

    for ir in indirect_references(cr):
        if ir.res_model and not ir.res_id and ir.table not in ignores:
            # only update unbound references, other ones have been updated by the call to
            # `replace_record_references_batch`
            where_clauses = []
            for _, uniqs in _get_unique_indexes_with(cr, ir.table, ir.res_model):
                sub_where = " AND ".join("o.{0} = t.{0}".format(a) for a in uniqs if a != ir.res_model) or "true"
                where_clauses.append(
                    "NOT EXISTS(SELECT 1 FROM {t} o WHERE {w} AND o.{c}=%(new)s)".format(
                        t=ir.table, c=ir.res_model, w=sub_where
                    )
                )
            where = " AND ".join(where_clauses) or "true"
            query = "UPDATE {t} t SET {c}=%(new)s WHERE {w} AND {c}=%(old)s".format(t=ir.table, c=ir.res_model, w=where)
            fmt_query = cr.mogrify(query, {"new": target, "old": source}).decode()
            if column_exists(cr, ir.table, "id"):
                parallel_execute(cr, explode_query_range(cr, fmt_query, table=ir.table, alias="t"))
            else:
                cr.execute(fmt_query)

    # adapt computes of manual fields
    if column_exists(cr, "ir_model_fields", "compute"):
        cr.execute(
            r"""
            UPDATE ir_model_fields
               SET compute = regexp_replace(compute, %s, %s, 'g')
             WHERE state = 'manual'
               AND compute IS NOT NULL
               AND name LIKE 'x\_%%'
            """,
            [
                r"""\yenv\[('|"){}\1\]""".format(re.escape(source)),
                "env['{}']".format(target),
            ],
        )

    # Adapt translations
    if table_exists(cr, "ir_translation"):
        cr.execute(
            """
            UPDATE ir_translation it
               SET name = %(target)s || substr(name, %(len_source)s)
             WHERE it.name LIKE %(source_like)s
               AND NOT EXISTS (
                            -- Some translations may already exist in the target model
                            SELECT 1 FROM ir_translation itt
                             WHERE itt.name = %(target)s || substr(it.name, %(len_source)s)
                               AND itt.type = it.type
                               AND itt.lang = it.lang
                               AND itt.res_id = it.res_id
                               )
            """,
            {
                "target": target,
                "source_like": source.replace("_", r"\_") + ",%",
                "len_source": len(source) + 1,
            },
        )

    remove_model(cr, source, drop_table=drop_table, ignore_m2m=ignore_m2m)


def remove_inherit_from_model(cr, model, inherit, keep=(), skip_inherit=()):
    _validate_model(model)
    _validate_model(inherit)

    inherit_models = {inherit} | set(inherit_parents(cr, inherit, interval="[]"))

    cr.execute(
        """
        SELECT name, ttype, relation, store
          FROM ir_model_fields
         WHERE model IN %s
           AND name NOT IN ('id',
                            'create_uid', 'write_uid',
                            'create_date', 'write_date',
                            '__last_update', 'display_name')
           AND name != ALL(%s)
    """,
        [tuple(inherit_models), list(keep)],
    )
    for field, ftype, relation, store in cr.fetchall():
        if ftype == "one2many" and store:
            # for mixin, one2many are filtered by their model and res_id.
            table = table_of_model(cr, relation)
            irs = [ir for ir in indirect_references(cr) if ir.table == table and ir.res_id is not None]
            for ir in irs:
                query = 'DELETE FROM "{}" WHERE ({})'.format(ir.table, ir.model_filter())
                cr.execute(query, [model])  # cannot be executed in parallel. See git blame.
        remove_field(cr, model, field, skip_inherit="*")  # inherits will be removed by the recursive call.

    # down on inherits of `model`
    for inh in for_each_inherit(cr, model, skip_inherit):
        remove_inherit_from_model(cr, inh.model, inherit, keep=keep, skip_inherit=skip_inherit)


def convert_model_to_abstract(cr, model, drop_table=True, keep=()):
    _validate_model(model)

    if drop_table:
        table = table_of_model(cr, model)
        if table_exists(cr, table):
            cr.execute('DROP TABLE "{0}" CASCADE'.format(table))
        elif view_exists(cr, table):
            cr.execute('DROP VIEW "{0}" CASCADE'.format(table))

    # Even if `__last_update` can be changed on the model definition, we hardcode the name.
    # I'm not aware of any (standard) model that modify it. The field will need to be removed explicitly if it happen.
    for field in ["id", "__last_update", "display_name", "create_uid", "create_date", "write_uid", "write_date"]:
        if field not in keep:
            remove_field(cr, model, field)
