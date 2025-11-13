"""Utility functions for record-level operations."""

import logging
import os
import re
import uuid
from contextlib import contextmanager
from operator import itemgetter

import lxml
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

try:
    from odoo import modules, release
    from odoo.tools.convert import xml_import
    from odoo.tools.misc import file_open
    from odoo.tools.translate import xml_translate
except ImportError:
    from openerp import modules, release
    from openerp.tools.convert import xml_import
    from openerp.tools.misc import file_open

from .const import NEARLYWARN
from .exceptions import MigrationError
from .helpers import (
    _get_theme_models,
    _ir_values_value,
    _validate_model,
    model_of_table,
    resolve_model_fields_path,
    table_of_model,
)
from .inconsistencies import break_recursive_loops
from .indirect_references import indirect_references
from .inherit import direct_inherit_parents, for_each_inherit
from .misc import AUTOMATIC, chunks, parse_version, version_gte
from .orm import env, flush
from .pg import (
    PGRegexp,
    SQLStr,
    _get_unique_indexes_with,
    _validate_table,
    column_exists,
    column_type,
    column_updatable,
    explode_execute,
    explode_query_range,
    format_query,
    get_columns,
    get_fk,
    get_value_or_en_translation,
    parallel_execute,
    table_exists,
    target_of,
)
from .report import add_to_migration_reports

_logger = logging.getLogger(__name__)

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = unicode = str


def remove_view(cr, xml_id=None, view_id=None, silent=False, key=None):
    """
    Remove a view and all its descendants.

    This function recursively deletes the given view and its inherited views, as long as
    they are part of a module. It will fail as soon as a custom view exists anywhere in
    the hierarchy. It also removes multi-website COWed views.

    :param str xml_id: optional, the xml_id of the view to remove
    :param int view_id: optional, the ID of the view to remove
    :param bool silent: whether to show in the logs disabled custom views
    :param str or None key: key used to detect multi-website COWed views, if `None` then
                                set to `xml_id` if provided, otherwise set to the xml_id
                                referencing the view with ID `view_id` if any

    .. warning::
       Either `xml_id` or `view_id` must be set. Specifying both will raise an error.
    """
    assert bool(xml_id) ^ bool(view_id)
    if xml_id:
        view_id = ref(cr, xml_id)
        if view_id:
            module, _, name = xml_id.partition(".")
            cr.execute("SELECT model FROM ir_model_data WHERE module=%s AND name=%s", [module, name])

            [model] = cr.fetchone()
            if model != "ir.ui.view":
                raise ValueError("%r should point to a 'ir.ui.view', not a %r" % (xml_id, model))
    else:
        # search matching xmlid for logging or renaming of custom views
        xml_id = "?"
        if not key:
            cr.execute("SELECT module, name FROM ir_model_data WHERE model='ir.ui.view' AND res_id=%s", [view_id])
            if cr.rowcount:
                xml_id = "%s.%s" % cr.fetchone()

    # From given or determined xml_id, the views duplicated in a multi-website
    # context are to be found and removed.
    if xml_id != "?" and column_exists(cr, "ir_ui_view", "key"):
        cr.execute("SELECT id FROM ir_ui_view WHERE key = %s AND id != %s", [xml_id, view_id])
        for [v_id] in cr.fetchall():
            remove_view(cr, view_id=v_id, silent=silent, key=xml_id)

    if not key and column_exists(cr, "ir_ui_view", "key"):
        cr.execute("SELECT key FROM ir_ui_view WHERE id = %s and key != %s", [view_id, xml_id])
        [key] = cr.fetchone() or [None]

    # Occurrences of xml_id and key in the t-call of views are to be found and removed.
    if xml_id != "?":
        _remove_redundant_tcalls(cr, xml_id)
    if key and key != xml_id:
        _remove_redundant_tcalls(cr, key)

    if not view_id:
        return

    cr.execute(
        """
        SELECT v.id, x.module || '.' || x.name, v.name
        FROM ir_ui_view v LEFT JOIN
           ir_model_data x ON (v.id = x.res_id AND x.model = 'ir.ui.view' AND x.module !~ '^_')
        WHERE v.inherit_id = %s;
    """,
        [view_id],
    )
    for child_id, child_xml_id, child_name in cr.fetchall():
        if child_xml_id:
            if not silent:
                _logger.info(
                    "remove deprecated built-in view %s (ID %s) as parent view %s (ID %s) is going to be removed",
                    child_xml_id,
                    child_id,
                    xml_id,
                    view_id,
                )
            remove_view(cr, child_xml_id, silent=True)
        else:
            if not silent:
                _logger.warning(
                    "deactivate deprecated custom view with ID %s as parent view %s (ID %s) is going to be removed",
                    child_id,
                    xml_id,
                    view_id,
                )
            disable_view_query = """
                UPDATE ir_ui_view
                SET name = (name || ' - old view, inherited from ' || %%s),
                    inherit_id = NULL
                    %s
                    WHERE id = %%s
            """
            # In 8.0, disabling requires setting mode to 'primary'
            extra_set_sql = ""
            if column_exists(cr, "ir_ui_view", "mode"):
                extra_set_sql = ",  mode = 'primary' "

            # Column was not present in v7 and it's older version
            if column_exists(cr, "ir_ui_view", "active"):
                extra_set_sql += ", active = false "

            disable_view_query = disable_view_query % extra_set_sql
            cr.execute(disable_view_query, (key or xml_id, child_id))
            add_to_migration_reports(
                {"id": child_id, "name": child_name},
                "Disabled views",
            )
    if not silent:
        _logger.info("remove deprecated %s view %s (ID %s)", (key and "COWed") or "built-in", key or xml_id, view_id)

    remove_records(cr, "ir.ui.view", [view_id])


@contextmanager
def edit_view(cr, xmlid=None, view_id=None, skip_if_not_noupdate=True, active="auto"):
    """
    Context manager to edit a view's arch.

    This function returns a context manager that may yield a parsed arch of a view as an
    `etree Element <https://lxml.de/tutorial.html#the-element-class>`_. Any changes done
    in the returned object will be written back to the database upon exit of the context
    manager, updating also the translated versions of the arch. Since the function may not
    yield, use :func:`~odoo.upgrade.util.misc.skippable_cm` to avoid errors.

    .. code-block:: python

        with util.skippable_cm(), util.edit_view(cr, "xml.id") as arch:
            arch.attrib["string"] = "My Form"

    To select the target view to edit use either `xmlid` or `view_id`, not both.

    When the view is identified by `view_id`, the arch is always yielded if the view
    exists, with disregard to any `noupdate` flag it may have associated. When `xmlid` is
    set, if the view `noupdate` flag is `False` then the arch will not be yielded *unless*
    `skip_if_not_noupdate` is set to `False`. If `noupdate` is `True`, the view will be
    yielded for edit.

    If the `active` argument is `True` or `False`, the `active` flag of the view will be set
    accordingly.

    .. note::
        If `active` is "auto" (default value), the view will be activated if selected
        via `xmlid` and left untouched if selected via `view_id`.

    :param str xmlid: optional, xml_id of the view edit
    :param int view_id: optional, ID of the view to edit
    :param bool skip_if_not_noupdate: whether to force the edit of views requested via
                                     `xmlid` parameter even if they are flagged as
                                     `noupdate=True`, ignored if `view_id` is set
    :param bool or None or "auto" active: active flag value to set. Unchanged when `None`.
    :return: a context manager that yields the parsed arch, upon exit the context manager
             writes back the changes.
    """
    assert bool(xmlid) ^ bool(view_id), "You Must specify either xmlid or view_id"
    if active not in (True, False, None, "auto"):
        raise ValueError("Invalid `active` value: {!r}".format(active))
    if active == "auto":
        active = True if xmlid else None

    noupdate = True
    if xmlid:
        if "." not in xmlid:
            raise ValueError("Please use fully qualified name <module>.<name>")

        module, _, name = xmlid.partition(".")
        cr.execute(
            """
                SELECT res_id, noupdate
                  FROM ir_model_data
                 WHERE module = %s
                   AND name = %s
        """,
            [module, name],
        )
        data = cr.fetchone()
        if data:
            view_id, noupdate = data

    if view_id and not (skip_if_not_noupdate and not noupdate):
        arch_col = "arch_db" if column_exists(cr, "ir_ui_view", "arch_db") else "arch"
        jsonb_column = column_type(cr, "ir_ui_view", arch_col) == "jsonb"
        cr.execute(
            """
                SELECT {arch}
                  FROM ir_ui_view
                 WHERE id=%s
            """.format(
                arch=arch_col,
            ),
            [view_id],
        )
        [arch] = cr.fetchone() or [None]
        if arch:

            def parse(arch):
                arch = arch.encode("utf-8") if isinstance(arch, unicode) else arch
                return lxml.etree.fromstring(arch.replace(b"&#13;\n", b"\n").strip())

            if jsonb_column:

                def get_trans_terms(value):
                    terms = []
                    xml_translate(terms.append, value)
                    return terms

                translation_terms = {lang: get_trans_terms(value) for lang, value in arch.items()}
                arch_etree = parse(arch["en_US"])
                yield arch_etree
                new_arch = lxml.etree.tostring(arch_etree, encoding="unicode")
                terms_en = translation_terms["en_US"]
                arch_column_value = Json(
                    {
                        lang: xml_translate(dict(zip(terms_en, terms)).get, new_arch)
                        for lang, terms in translation_terms.items()
                    }
                )
            else:
                arch_etree = parse(arch)
                yield arch_etree
                arch_column_value = lxml.etree.tostring(arch_etree, encoding="unicode")

            set_active = ", active={}".format(bool(active)) if active is not None else ""
            cr.execute(
                "UPDATE ir_ui_view SET {arch}=%s{set_active} WHERE id=%s".format(arch=arch_col, set_active=set_active),
                [arch_column_value, view_id],
            )


def add_view(cr, name, model, view_type, arch_db, inherit_xml_id=None, priority=16, key=None):
    inherit_id = None
    if inherit_xml_id:
        inherit_id = ref(cr, inherit_xml_id)
        if not inherit_id:
            raise ValueError(
                "Unable to add view '%s' because its inherited view '%s' cannot be found!" % (name, inherit_xml_id)
            )
    # Odoo <= 8.0 doesn't have the `key`
    key_exist = column_exists(cr, "ir_ui_view", "key")
    if key_exist and view_type == "qweb" and not key:
        key = "gen_key.%s" % str(uuid.uuid4())[:6]
    arch_col = "arch_db" if column_exists(cr, "ir_ui_view", "arch_db") else "arch"
    jsonb_column = column_type(cr, "ir_ui_view", arch_col) == "jsonb"
    arch_column_value = Json({"en_US": arch_db}) if jsonb_column else arch_db
    cr.execute(
        """
        INSERT INTO ir_ui_view(name, "type",  model, inherit_id, mode, active, priority, %s)
        VALUES(%%(name)s, %%(view_type)s, %%(model)s, %%(inherit_id)s, %%(mode)s, 't', %%(priority)s, %%(arch_db)s %s)
        RETURNING id
    """
        % (arch_col + (", key" if key_exist else ""), ", %(key)s" if key_exist else ""),
        {
            "name": name,
            "view_type": view_type,
            "model": model,
            "inherit_id": inherit_id,
            "mode": "extension" if inherit_id else "primary",
            "priority": priority,
            "key": key,
            "arch_db": arch_column_value,
        },
    )
    return cr.fetchone()[0]


# fmt:off
if version_gte("saas~14.3"):
    def remove_asset(cr, name):
        cr.execute("SELECT id FROM ir_asset WHERE bundle = %s", [name])
        if cr.rowcount:
            remove_records(cr, "ir.asset", [aid for aid, in cr.fetchall()])
else:
    def remove_asset(cr, name):
        remove_view(cr, name, silent=True)
# fmt:on


def remove_record(cr, name):
    """
    Remove a record and its references corresponding to the given :term:`xml_id <external identifier>`.

    :param str name: record xml_id, under the format `module.name`
    """
    if isinstance(name, basestring):
        if "." not in name:
            raise ValueError("Please use fully qualified name <module>.<name>")
        module, _, name = name.partition(".")
        cr.execute(
            """
                SELECT model, res_id
                  FROM ir_model_data
                 WHERE module = %s
                   AND name = %s
        """,
            [module, name],
        )
        if not cr.rowcount:
            return None
        model, res_id = cr.fetchone()
    elif isinstance(name, tuple):
        if len(name) != 2:
            raise ValueError("Please use a 2-tuple (<model>, <res_id>)")
        model, res_id = name
    else:
        raise TypeError("Either use a fully qualified xmlid string <module>.<name> or a 2-tuple (<model>, <res_id>)")

    # deleguate to the right method
    if model == "ir.ui.view":
        _logger.log(NEARLYWARN, "Removing view %r", name)
        return remove_view(cr, view_id=res_id)

    if model == "ir.ui.menu":
        _logger.log(NEARLYWARN, "Removing menu %r", name)
        return remove_menus(cr, [res_id])

    if model == "res.groups":
        _logger.log(NEARLYWARN, "Removing group %r", name)
        return remove_group(cr, group_id=res_id)

    return remove_records(cr, model, [res_id])


def remove_records(cr, model, ids):
    if not ids:
        return

    ids = tuple(ids)

    # remove theme model's copy_ids
    theme_copy_model = _get_theme_models().get(model)
    if theme_copy_model:
        cr.execute(
            'SELECT id FROM "{}" WHERE theme_template_id IN %s'.format(table_of_model(cr, theme_copy_model)),
            [ids],
        )
        if theme_copy_model == "ir.ui.view":
            for (view_id,) in cr.fetchall():
                remove_view(cr, view_id=view_id)
        else:
            remove_records(cr, theme_copy_model, [rid for (rid,) in cr.fetchall()])

    for inh in for_each_inherit(cr, model, skip=()):
        if inh.via:
            table = table_of_model(cr, inh.model)
            if not column_exists(cr, table, inh.via):
                # column may not exists in case of a partially uninstalled module that left only *magic columns* in tables
                continue
            cr.execute('SELECT id FROM "{}" WHERE "{}" IN %s'.format(table, inh.via), [ids])
            if inh.model == "ir.ui.menu":
                remove_menus(cr, [menu_id for (menu_id,) in cr.fetchall()])
            elif inh.model == "ir.ui.view":
                for (view_id,) in cr.fetchall():
                    remove_view(cr, view_id=view_id)
            else:
                remove_records(cr, inh.model, [rid for (rid,) in cr.fetchall()])

    table = table_of_model(cr, model)
    base_query = format_query(cr, "DELETE FROM {} WHERE id IN %s", table)
    parallel_execute(
        cr,
        [cr.mogrify(base_query, [chunk_ids]).decode() for chunk_ids in chunks(ids, 1000, fmt=tuple)],
    )
    for ir in indirect_references(cr, bound_only=True):
        if not ir.company_dependent_comodel:
            query = 'DELETE FROM "{}" WHERE {} AND "{}" IN %s'.format(ir.table, ir.model_filter(), ir.res_id)
            cr.execute(query, [model, ids])
        elif ir.company_dependent_comodel == model:
            json_path = cr.mogrify(
                "$.* ? ({})".format(" || ".join(["@ == %s"] * len(ids))),
                ids,
            ).decode()

            query = cr.mogrify(
                format_query(
                    cr,
                    """
                    UPDATE {table}
                       SET {column} = (
                            SELECT jsonb_object_agg(
                                key,
                                CASE
                                    WHEN value::int4 IN %s THEN NULL
                                    ELSE value::int4
                                END)
                              FROM jsonb_each_text({column})
                           )
                     WHERE {column} IS NOT NULL
                       AND {column} @? {json_path}
                    """,
                    table=ir.table,
                    column=ir.res_id,
                    json_path=sql.Literal(json_path),
                ),
                [ids],
            ).decode()
            explode_execute(cr, query, table=ir.table)
    _rm_refs(cr, model, ids)

    if model == "res.groups":
        # A group is gone, the auto-generated view `base.user_groups_view` is outdated.
        # Create a shim. It will be re-generated later by creating/updating groups or
        # explicitly in `base/0.0.0/end-user_groups_view.py`.
        arch_col = "arch_db" if column_exists(cr, "ir_ui_view", "arch_db") else "arch"
        jsonb_column = column_type(cr, "ir_ui_view", arch_col) == "jsonb"
        arch_value = "json_build_object('en_US', '<form/>')" if jsonb_column else "'<form/>'"
        cr.execute(
            "UPDATE ir_ui_view SET {} = {} WHERE id = %s".format(arch_col, arch_value),
            [ref(cr, "base.user_groups_view")],
        )


def _rm_refs(cr, model, ids=None):
    if ids is None:
        match = "like %s"
        needle = model + ",%"
    else:
        if not ids:
            return
        match = "in %s"
        needle = tuple("{0},{1}".format(model, i) for i in ids)

    # "model-comma" fields
    cr.execute(
        """
        SELECT model, name
          FROM ir_model_fields
         WHERE ttype='reference'
         UNION
        SELECT 'ir.translation', 'name'
    """
    )

    for ref_model, ref_column in cr.fetchall():
        table = table_of_model(cr, ref_model)
        if column_updatable(cr, table, ref_column):
            query_tail = ' FROM "{0}" WHERE "{1}" {2}'.format(table, ref_column, match)
            if ref_model == "ir.ui.view":
                cr.execute("SELECT id" + query_tail, [needle])
                for (view_id,) in cr.fetchall():
                    remove_view(cr, view_id=view_id, silent=True)
            elif ref_model == "ir.ui.menu":
                cr.execute("SELECT id" + query_tail, [needle])
                menu_ids = tuple(m[0] for m in cr.fetchall())
                remove_menus(cr, menu_ids)
            else:
                cr.execute("SELECT id" + query_tail, [needle])
                for (record_id,) in cr.fetchall():
                    remove_record(cr, (ref_model, record_id))

    if table_exists(cr, "ir_values"):
        column, _ = _ir_values_value(cr)
        query = "DELETE FROM ir_values WHERE {0} {1}".format(column, match)
        cr.execute(query, [needle])

    if ids is None and table_exists(cr, "ir_translation"):
        cr.execute(
            """
            DELETE FROM ir_translation
             WHERE name=%s
               AND type IN ('constraint', 'sql_constraint', 'view', 'report', 'rml', 'xsl')
        """,
            [model],
        )


def _remove_import_export_paths(cr, model, field=None):
    export_q = """
            SELECT el.id,
                   e.resource,
                   STRING_TO_ARRAY(el.name, '/')
              FROM ir_exports_line el
              JOIN ir_exports e
                ON el.export_id = e.id
        """
    if field:
        export_q = cr.mogrify(export_q + " WHERE el.name ~ %s ", [r"\y{}\y".format(field)]).decode()
    else:
        export_q += " WHERE el.name IS NOT NULL"

    impex_data = [(export_q, "ir_exports_line")]
    if table_exists(cr, "base_import_mapping"):
        import_q = """
            SELECT id,
                   res_model,
                   STRING_TO_ARRAY(field_name, '/')
              FROM base_import_mapping
            """
        if field:
            import_q = cr.mogrify(import_q + " WHERE field_name ~ %s ", [r"\y{}\y".format(field)]).decode()
        else:
            import_q += " WHERE field_name IS NOT NULL "
        impex_data.append((import_q, "base_import_mapping"))

    for query, impex_model in impex_data:
        cr.execute(query)
        to_rem = [
            path_id
            for path_id, related_model, path in cr.fetchall()
            if any(
                x.field_model == model and (field is None or x.field_name == field)
                for x in resolve_model_fields_path(cr, related_model, path)
            )
        ]
        if to_rem:
            remove_records(cr, impex_model, to_rem)


def is_changed(cr, xmlid, interval="1 minute"):
    """
    Return whether a record was changed.

    This function checks if a record was changed before the current upgrade start time.
    See :file:`upgrade-util/src/base/0.0.0/pre-00-upgrade-start.py`

    This utility will return a false positive on xmlids of records that match the
    following conditions:

     * Have been updated in an upgrade preceding the current one
     * Have not been updated in the current upgrade

    If the `xmlid` doesn't exist in the DB this function returns ``None``.

    :param str xmlid: `xmlid` of the record to check
    :param str interval: SQL interval, a record is considered as changed if
                         `write_date > create_date + interval`
    :rtype: bool or None
    """
    assert "." in xmlid
    module, _, name = xmlid.partition(".")
    cr.execute("SELECT model, res_id FROM ir_model_data WHERE module=%s AND name=%s", [module, name])
    data = cr.fetchone()
    if not data:
        return None
    model, res_id = data
    table = table_of_model(cr, model)
    cr.execute(
        """
        SELECT 1
          FROM {} r
     LEFT JOIN ir_config_parameter p
            ON p.key = 'upgrade.start.time'
         WHERE r.id = %s
           -- Note: use a negative search to handle the case of NULL values in write/create_date
           AND COALESCE(r.write_date < p.value::timestamp, True)
           AND r.write_date - r.create_date > interval %s
        """.format(table),
        [res_id, interval],
    )
    return bool(cr.rowcount)


def if_unchanged(cr, xmlid, callback, interval="1 minute", **kwargs):
    """
    Run `callback` if a record is unchanged.

    This function will run a `callback` when the referred record is unchanged. The `xmlid`
    and any extra parameter, but not `interval`, will be passed to the `callback`. In case
    the record was changed it will be marked as `noupdate=True`. See also
    :func:`~odoo.upgrade.util.records.is_changed` and
    :func:`~odoo.upgrade.util.records.force_noupdate`.

    This function is useful to take an action *only* when a record hasn't been updated, a
    common example is to force an update from XML even if the record was `noupdate=True`

    .. code-block:: python

       util.if_unchanged(cr, "mymodule.myrecord", util.update_record_from_xml)

    :param str xmlid: xml_id of the record to check
    :param function callback: callback to execute in case the record was *not* changed, all
                             extra parameters to this function are passed to the callback
    :param str interval: interval after `create_date` on which a record is considered as
                        _changed_, see :func:`~odoo.upgrade.util.misc.is_changed`
    """
    changed = is_changed(cr, xmlid, interval=interval)
    if changed is None:
        return
    if changed is False:
        callback(cr, xmlid, **kwargs)
    else:
        force_noupdate(cr, xmlid, noupdate=True)


def remove_menus(cr, menu_ids):
    if not menu_ids:
        return
    cr.execute(
        """
        WITH RECURSIVE tree(id) AS (
            SELECT id
              FROM ir_ui_menu
             WHERE id IN %s
             UNION
            SELECT m.id
              FROM ir_ui_menu m
              JOIN tree t ON (m.parent_id = t.id)
        ),
        removed_menus AS (
            DELETE
              FROM ir_ui_menu m
             USING tree t
             WHERE m.id = t.id
         RETURNING m.id
        )
        DELETE
          FROM ir_model_data imd
         USING removed_menus m
         WHERE imd.res_id = m.id
           AND imd.model = 'ir.ui.menu'
        """,
        [tuple(menu_ids)],
    )


def remove_group(cr, xml_id=None, group_id=None):
    assert bool(xml_id) ^ bool(group_id)
    if xml_id:
        group_id = ref(cr, xml_id)
        if group_id:
            module, _, name = xml_id.partition(".")
            cr.execute("SELECT model FROM ir_model_data WHERE module=%s AND name=%s", [module, name])
            [model] = cr.fetchone()
            if model != "res.groups":
                raise ValueError("%r should point to a 'res.groups', not a %r" % (xml_id, model))

    if not group_id:
        return

    # Get all fks from table res_groups
    fks = get_fk(cr, "res_groups", quote_ident=False)

    # Remove records referencing the group_id from the referencing tables (restrict fks)
    standard_tables = ["ir_model_access", "rule_group_rel"]
    custom_tables = []
    for foreign_table, foreign_column, _, on_delete_action in fks:
        if on_delete_action == "r":
            if foreign_table not in standard_tables:
                cr.execute(
                    'SELECT COUNT(*) FROM "{}" WHERE "{}" = %s'.format(foreign_table, foreign_column),
                    (group_id,),
                )
                count = cr.fetchone()[0]
                if count:
                    custom_tables.append((foreign_table, foreign_column, count))
                continue

            query = 'DELETE FROM "{}" WHERE "{}" = %s'.format(foreign_table, foreign_column)
            query = cr.mogrify(query, (group_id,)).decode()

            if column_exists(cr, foreign_table, "id"):
                parallel_execute(cr, explode_query_range(cr, query, table=foreign_table))
            else:
                cr.execute(query)

    if custom_tables:
        col_name = get_value_or_en_translation(cr, "res_groups", "name")
        cr.execute("SELECT {} FROM res_groups WHERE id = %s".format(col_name), [group_id])
        group_name = cr.fetchone()[0]
        raise MigrationError(
            "\nThe following 'table (column) - records count' are referencing the group '{}'".format(group_name)
            + " and cannot be removed automatically:\n"
            + "\n".join(
                " - {} ({}) - {} record(s)".format(table, column, count) for table, column, count in custom_tables
            )
            + "\nPlease remove them manually or remove the foreign key constraints set as RESTRICT."
        )

    remove_records(cr, "res.groups", [group_id])


def rename_xmlid(cr, old, new, noupdate=None, on_collision="fail"):
    """
    Rename an :term:`external identifier` (`xml_id`) of a record.

    A rename cannot happen when the target name already exists on the database. In such
    cases there are two options to control how this function behaves:

    - `fail`: raise an exception and prevent renaming
    - `merge`: rename the external identifier, remove the old one, and *replace*
      references. See :func:`~odoo.upgrade.util.records.replace_record_references_batch`

    .. note::
       This function does not remove records, it only updates xml_ids.

    :param str old: current xml_id of the record, under the format `module.name`
    :param str new: new xml_id of the record, under the format `module.name`
    :param bool or None noupdate: value to set on the `noupdate` flag of the xml_id,
                                  ignored if `None`
    :param str on_collision: how to proceed if the xml_id already exists, the options are
                             `merge` or `fail` (default)
    :return: the ID of the record referenced by the *new* xml_id, `None` when the record
             doesn't exist
    :rtype: int or None
    """
    if "." not in old or "." not in new:
        raise ValueError("Please use fully qualified name <module>.<name>")
    if on_collision not in {"fail", "merge"}:
        raise ValueError("Invalid value for the `on_collision` argument: {0!r}".format(on_collision))
    if old == new:
        raise ValueError("Cannot rename an XMLID to itself")

    old_module, _, old_name = old.partition(".")
    new_module, _, new_name = new.partition(".")
    cr.execute("SELECT model, res_id FROM ir_model_data WHERE module = %s AND name = %s", [new_module, new_name])
    new_model, new_id = cr.fetchone() or (None, None)
    cr.execute("SELECT model, res_id FROM ir_model_data WHERE module = %s AND name = %s", [old_module, old_name])
    model, old_id = cr.fetchone() or (None, None)

    if new_id and old_id:
        if (model, old_id) != (new_model, new_id):
            if on_collision == "fail":
                raise MigrationError("Can't rename {} to {} as it already exists".format(old, new))

            if model != new_model:
                raise MigrationError("Model mismatch while renaming xmlid {}. {} to {}".format(old, model, new_model))

            replace_record_references(cr, (model, old_id), (model, new_id), replace_xmlid=False)

        if noupdate is not None:
            force_noupdate(cr, new, bool(noupdate))
        cr.execute("DELETE FROM ir_model_data WHERE module=%s AND name=%s", [old_module, old_name])
    else:
        nu = "" if noupdate is None else (", noupdate=" + str(bool(noupdate)).lower())

        cr.execute(
            """UPDATE ir_model_data
                  SET module=%s, name=%s
                   {}
                WHERE module=%s AND name=%s
            RETURNING model, res_id
            """.format(nu),
            (new_module, new_name, old_module, old_name),
        )
        model, new_id = cr.fetchone() or (None, None)

    if model and new_id:
        if model == "ir.ui.view" and column_exists(cr, "ir_ui_view", "key"):
            cr.execute("UPDATE ir_ui_view SET key=%s WHERE id=%s AND key=%s", [new, new_id, old])
            if cr.rowcount:
                # iff the key has been updated for this view, also update it for all other cowed views.
                # Don't change the view keys unconditionally to avoid changing unrelated views.
                cr.execute("UPDATE ir_ui_view SET key = %s WHERE key = %s", [new, old])

                # Adapting t-call and t-name references in views
                search_pattern = r"""\yt-(call|name)=(["']){}\2""".format(re.escape(old))
                replace_pattern = r"t-\1=\2{}\2".format(new)
                if column_type(cr, "ir_ui_view", "arch_db") == "jsonb":
                    replace_in_all_jsonb_values(
                        cr,
                        "ir_ui_view",
                        "arch_db",
                        PGRegexp(search_pattern),
                        replace_pattern,
                        extra_filter=cr.mogrify("arch_db->>'en_US' ~ %s", [search_pattern]).decode(),
                    )
                else:
                    arch_col = "arch_db" if column_exists(cr, "ir_ui_view", "arch_db") else "arch"
                    cr.execute(
                        format_query(
                            cr,
                            """
                            UPDATE ir_ui_view
                               SET {arch} = regexp_replace({arch}, %s, %s, 'g')
                             WHERE {arch} ~ %s
                            """,
                            arch=arch_col,
                        ),
                        [search_pattern, replace_pattern, search_pattern],
                    )

        if model == "ir.ui.menu" and column_exists(cr, "res_users_settings", "homemenu_config"):
            query = """
                UPDATE res_users_settings
                   SET homemenu_config = regexp_replace(homemenu_config::text, %(old)s, %(new)s)::jsonb
                 WHERE homemenu_config::text ~ %(old)s
            """
            cr.execute(query, {"old": r"\y{}\y".format(re.escape(old)), "new": new})

        for parent_model, inh in direct_inherit_parents(cr, model):
            if inh.via:
                parent = parent_model.replace(".", "_")
                rename_xmlid(
                    cr,
                    "{}_{}".format(old, parent),
                    "{}_{}".format(new, parent),
                    noupdate=noupdate,
                    on_collision=on_collision,
                )
        return new_id
    return None


def ref(cr, xmlid):
    """
    Return the id corresponding to the given :term:`xml_id <external identifier>`.

    :param str xml_id: record xml_id, under the format `module.name`
    :return: ID of the referenced record, `None` if not found.
    :rtype: int or None
    """
    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

    module, _, name = xmlid.partition(".")
    cr.execute(
        """
            SELECT res_id
              FROM ir_model_data
             WHERE module = %s
               AND name = %s
    """,
        [module, name],
    )
    data = cr.fetchone()
    if data:
        return data[0]
    return None


def force_noupdate(cr, xmlid, noupdate=True, warn=False):
    """
    Update the `noupdate` flag of a record.

    :param str xmlid: record xml_id, under the format `module.name`
    :param bool noupdate: value to set on the `noupdate` flag
    :param warn: whether to output a warning in the logs when the flag was switched from
                 `True` to `False`
    """
    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

    module, _, name = xmlid.partition(".")
    cr.execute(
        """
            UPDATE ir_model_data
               SET noupdate = %s
             WHERE module = %s
               AND name = %s
               AND noupdate != %s
    """,
        [noupdate, module, name, noupdate],
    )
    if noupdate is False and cr.rowcount and warn:
        _logger.warning("Customizations on `%s` might be lost!", xmlid)
    return cr.rowcount


def ensure_xmlid_match_record(cr, xmlid, model, values):
    """
    Ensure an xml_id references a record with specific values.

    This function ensures the record pointed by an xml_id matches the values for the
    fields specified in the `values` parameter. When the xmlid exist but it points to a
    record that doesn't match the values, the xmlid is updated to point to a record that
    matches the values if one is found. If the xmlid doesn't exist, it is created with the
    found record. When no matching record is found, nothing is done. In all cases the
    referenced record, after a possible update, of the xml_id is returned.

    :param str xmlid: record xml_id, under the format `module.name`
    :param str model: model name of the record
    :param dict values: mapping of field names to values the record must fulfill

                        .. example::

                           .. code-block:: python

                              values = {"id": 123}
                              values = {"name": "INV/2024/0001", "company_id": 1}

    :return: the ID of the matched record, `None` if no record found
    :rtype: int or None

    .. tip::
       This function is useful when migrating in-database records into a custom module, to
       create the xml_ids before the module is updated and avoid duplication.
    """
    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

    logger = _logger.getChild("ensure_xmlid_match_record")

    module, _, name = xmlid.partition(".")
    cr.execute(
        """
            SELECT id, res_id
              FROM ir_model_data
             WHERE module = %s
               AND name = %s
    """,
        [module, name],
    )
    data_id, res_id = cr.fetchone() or (None, None)

    table = table_of_model(cr, model)

    # search for existing records matching values
    where = []
    data = ()
    for k, v in values.items():
        if v is not None:
            where += ["%s = %%s" % (get_value_or_en_translation(cr, table, k),)]
            data += (v,)
        else:
            where += ["%s IS NULL" % (k,)]
            data += ()

    query = ("SELECT id FROM %s WHERE " % table) + " AND ".join(where)
    cr.execute(query, data)
    records = [id for (id,) in cr.fetchall()]
    if res_id and res_id in records:
        return res_id
    if not records:
        if res_id:
            logger.debug("`%s` refers %s(%s); values differ %r; no other match found.", xmlid, model, res_id, values)
            return res_id

        logger.debug("`%s` doesn't exist; no match found for values %r", xmlid, values)
        return None
    new_res_id = records[0]

    if data_id:
        logger.info("update `%s` from %s(%s) to %s(%s); values %r", xmlid, model, new_res_id, model, res_id, values)
        cr.execute(
            """
                UPDATE ir_model_data
                   SET res_id=%s
                 WHERE id=%s
        """,
            [new_res_id, data_id],
        )
    else:
        logger.info("create `%s` that point to %s(%s); matching values %r", xmlid, model, new_res_id, values)
        cr.execute(
            """
                INSERT INTO ir_model_data(module, name, model, res_id, noupdate)
                     VALUES (%s, %s, %s, %s, %s)
        """,
            [module, name, model, new_res_id, True],
        )

    return new_res_id


def update_record_from_xml(
    cr,
    xmlid,
    reset_write_metadata=True,
    force_create=AUTOMATIC,
    from_module=None,
    reset_translations=(),
    ensure_references=False,
    fields=None,
):
    """
    Update a record based on its definition in the :doc:`/developer/reference/backend/data`.

    This function ignores the `noupdate` flag on the record. It searches in all XML files
    from the manifest of the module in the xmlid, or the `from_module` parameter if set,
    for a matching definition. When found, it forces the ORM to update the record as in the
    specs in the data file.

    Optionally this function can reset the translations of some fields.

    :param str xmlid: record xml_id, under the format `module.name`
    :param bool reset_write_metadata: whether to update the `write_date` of the record
    :param bool force_create: whether the record is created if it does not exist.
                              `True` by default, unless `fields` is not None.
    :param str from_module: name of the module from which to update the record, necessary
                            only when the specs are in a different module than the one in
                            the xml_id
    :param set(str) reset_translations: field names whose translations get reset
    :param bool ensure_references: whether referred records via `ref` XML attributes
                                   should also be updated.
    :param set(str) or None fields: optional list of fields to include in the XML declaration.
                                    If set, all other fields will be ignored. When set, record
                                    won't be created if missing.

    .. warning::
       This functions uses the ORM, therefore it can only be used after **all** models
       referenced in the data specs of the record are already **loaded**. In practice this
       means that this function should be used in `post-` or `end-` scripts.

    .. note::
       The standard behavior of ORM is to create the record if it doesn't exist, including
       its xml_id. That will happen on this function as well, unless `force_create` is set
       to `False`.
    """
    __update_record_from_xml(
        cr,
        xmlid,
        reset_write_metadata=reset_write_metadata,
        force_create=force_create,
        from_module=from_module,
        reset_translations=reset_translations,
        ensure_references=ensure_references,
        fields=fields,
        done_refs=set(),
    )


def __update_record_from_xml(
    cr,
    xmlid,
    reset_write_metadata,
    force_create,
    from_module,
    reset_translations,
    ensure_references,
    fields,
    done_refs,
):
    from .modules import get_manifest  # noqa: PLC0415

    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

    if force_create is AUTOMATIC:
        force_create = fields is None  # don't force record creation if we are filtering fields

    module, _, name = xmlid.partition(".")

    cr.execute(
        """
        UPDATE ir_model_data d
           SET noupdate = false
          FROM ir_model_data o
         WHERE o.id = d.id
           AND d.module = %s
           AND d.name = %s
     RETURNING d.model, d.res_id, o.noupdate
    """,
        [module, name],
    )
    if cr.rowcount:
        model, res_id, noupdate = cr.fetchone()
        if model == "ir.model":
            return
    elif not force_create:
        _logger.warning("Record %r not found in database. Skip update.", xmlid)
        return
    else:
        # The xmlid doesn't already exists, nothing to reset
        reset_write_metadata = noupdate = reset_translations = False
        fields = None

    write_data = None
    if reset_write_metadata:
        table = table_of_model(cr, model)
        cr.execute("SELECT write_uid, write_date, id FROM {} WHERE id=%s".format(table), [res_id])
        write_data = cr.fetchone()

    from_module = from_module or module

    id_match = (
        "@id='{module}.{name}' or @id='{name}'".format(module=module, name=name)
        if module == from_module
        else "@id='{module}.{name}'".format(module=module, name=name)
    )
    xpath = "//*[self::act_window or self::menuitem or self::record or self::report or self::template][{}]".format(id_match)  # fmt: skip

    # use a data tag inside openerp tag to be compatible with all supported versions
    new_root = lxml.etree.fromstring("<openerp><data/></openerp>")

    manifest = get_manifest(from_module)
    template = False
    found = False
    extra_references = []

    def add_ref(ref):
        if "." not in ref:
            extra_references.append(from_module + "." + ref)
        elif ref.split(".")[0] == from_module:
            extra_references.append(ref)

    for f in manifest.get("data", []):
        if not f.endswith(".xml"):
            continue
        with file_open(os.path.join(from_module, f)) as fp:
            doc = lxml.etree.parse(fp)
            for node in doc.xpath(xpath):
                found = True
                parent = node.getparent()
                if node.tag == "record" and fields is not None:
                    for fn in node.xpath("./field[@name]"):
                        if fn.attrib["name"] not in fields:
                            node.remove(fn)
                new_root[0].append(node)

                if node.tag == "menuitem" and parent.tag == "menuitem" and "parent_id" not in node.attrib:
                    new_root[0].append(
                        lxml.builder.E.record(
                            lxml.builder.E.field(name="parent_id", ref=parent.attrib["id"]),
                            model="ir.ui.menu",
                            id=node.attrib["id"],
                        )
                    )

                if node.tag == "template":
                    template = True
                if ensure_references:
                    for ref_node in node.xpath("//field[@ref]"):
                        if fields is not None and ref_node.attrib["name"] not in fields:
                            continue
                        add_ref(ref_node.get("ref"))
                    for eval_node in node.xpath("//field[@eval]"):
                        if fields is not None and eval_node.attrib["name"] not in fields:
                            continue
                        for ref_match in re.finditer(r"\bref\((['\"])(.*?)\1\)", eval_node.get("eval")):
                            add_ref(ref_match.group(2))

    if not found:
        if noupdate:
            force_noupdate(cr, xmlid, noupdate=True)
        suffix = " in %r module" % from_module if from_module != module else ""
        raise ValueError("Cannot find %r%s" % (xmlid, suffix))

    done_refs.add(xmlid)
    for ref in extra_references:
        if ref in done_refs:
            continue
        _logger.info("Update of %s - ensuring the reference %s exists", xmlid, ref)
        __update_record_from_xml(
            cr,
            ref,
            reset_write_metadata=reset_write_metadata,
            force_create=True,
            from_module=from_module,
            reset_translations=reset_translations,
            ensure_references=True,
            fields=None,
            done_refs=done_refs,
        )

    cr_or_env = env(cr) if version_gte("saas~16.2") else cr
    importer = xml_import(cr_or_env, from_module, idref={}, mode="update")
    kw = {"mode": "update"} if parse_version("8.0") <= parse_version(release.series) <= parse_version("12.0") else {}
    importer.parse(new_root, **kw)
    if version_gte("13.0"):
        flush(env(cr)["base"])

    if noupdate:
        force_noupdate(cr, xmlid, noupdate=True)
    if reset_write_metadata and write_data:
        cr.execute("UPDATE {} SET write_uid=%s, write_date=%s WHERE id=%s".format(table), write_data)

    if reset_translations:
        if reset_translations is True:
            if fields is None:
                fields_with_values_from_xml = {elem.attrib["name"] for elem in node.xpath("//record/field")}
                if template:
                    fields_with_values_from_xml |= {"arch_db", "name"}
            else:
                fields_with_values_from_xml = fields
            if version_gte("saas~18.5"):  # translate is varchar
                sql_code = "SELECT name FROM ir_model_fields WHERE model = %s AND translate IS NOT NULL AND name IN %s"
            else:  # translate is boolean
                sql_code = "SELECT name FROM ir_model_fields WHERE model = %s AND translate = true AND name IN %s"
            cr.execute(
                sql_code,
                [model, tuple(fields_with_values_from_xml)],
            )
            reset_translations = [fname for [fname] in cr.fetchall()]

        if table_exists(cr, "ir_translation"):
            cr.execute(
                """
                    DELETE FROM ir_translation
                          WHERE name IN %s
                            AND res_id = %s
                """,
                [tuple("{},{}".format(model, f) for f in reset_translations), res_id],
            )
        else:
            query = """
                UPDATE {}
                   SET {}
                 WHERE id = %s
            """.format(
                table,
                ",".join(
                    [
                        """%s = NULLIF(jsonb_build_object('en_US', %s->>'en_US'), '{"en_US": null}'::jsonb)"""
                        % (fname, fname)
                        for fname in reset_translations
                    ]
                ),
            )
            cr.execute(query, [res_id])

        env_ = env(cr)
        module_to_reload_from = env_["ir.module.module"].search(
            [("name", "=", from_module), ("state", "=", "installed")]
        )
        if module_to_reload_from:
            if hasattr(module_to_reload_from, "_update_translations"):
                module_to_reload_from._update_translations()
            else:
                # < 9.0
                module_to_reload_from.update_translations()


def delete_unused(cr, *xmlids, **kwargs):
    """
    Remove unused records.

    This function will remove records pointed by `xmlids` only if they are not referenced
    from any table. For hierarchical records (like product categories), it verifies
    if the children marked as cascade removal are also not referenced. In which case
    the record and its children are all removed.

    .. note::
       The records that cannot be removed are set as `noupdate=True`.

    :param list(str) xmlids: list of xml_ids to check for removal
    :param bool deactivate: whether to deactivate records that cannot be removed because
                            they are referenced, `False` by default
    :param bool keep_xmlids: whether to keep the xml_ids of records that cannot be
                             removed. By default `True` for versions up to 18.0,
                             `False` from `saas~18.1` on.
    :return: list of ids of removed records, if any
    :rtype: list(int)
    """
    deactivate = kwargs.pop("deactivate", False)
    keep_xmlids = kwargs.pop("keep_xmlids", not version_gte("saas~18.1"))
    if kwargs:
        raise TypeError("delete_unused() got an unexpected keyword argument %r" % kwargs.popitem()[0])

    select_xids = " UNION ".join(
        [
            cr.mogrify("SELECT %s::varchar as module, %s::varchar as name", [module, name]).decode()
            for xmlid in xmlids
            for module, _, name in [xmlid.partition(".")]
        ]
    )

    cr.execute(
        """
       WITH xids AS (
         {}
       ),
       _upd AS (
            UPDATE ir_model_data d
               SET noupdate = true
              FROM xids x
             WHERE d.module = x.module
               AND d.name = x.name
         RETURNING d.id, d.model, d.res_id, d.module || '.' || d.name as xmlid
       )
       SELECT model, array_agg(res_id ORDER BY id), array_agg(xmlid ORDER BY id)
         FROM _upd
     GROUP BY model
    """.format(select_xids)
    )

    deleted = []
    for model, ids, xids in cr.fetchall():
        table = table_of_model(cr, model)
        res_id_to_xmlid = dict(zip(ids, xids))

        cascade_children = [
            fk_col
            for fk_tbl, fk_col, _, fk_act in get_fk(cr, table, quote_ident=False)
            if fk_tbl == table and fk_act == "c"
        ]
        if cascade_children:
            if len(cascade_children) == 1:
                join = format_query(cr, "t.{}", cascade_children[0])
            else:
                join = sql.SQL("ANY(ARRAY[{}])").format(
                    sql.SQL(", ").join(sql.Composed([sql.SQL("t."), sql.Identifier(cc)]) for cc in cascade_children)
                )

            kids_query = format_query(
                cr,
                """
                WITH RECURSIVE _child AS (
                    SELECT id AS root, id AS child
                      FROM {0}
                     WHERE id = ANY(%(ids)s)
                     UNION  -- don't use UNION ALL in case we have a loop
                    SELECT c.root, t.id as child
                      FROM {0} t
                      JOIN _child c
                        ON c.child = {1}
                )
                SELECT root AS id, array_agg(child) AS children
                  FROM _child
              GROUP BY root
                """,
                table,
                join,
            )
        else:
            kids_query = format_query(cr, "SELECT id, ARRAY[id] AS children FROM {0} WHERE id = ANY(%(ids)s)", table)

        sub = " UNION ALL ".join(
            [
                format_query(cr, "SELECT 1 FROM {} x WHERE x.{} = ANY(s.children)", fk_tbl, fk_col)
                for fk_tbl, fk_col, _, fk_act in get_fk(cr, table, quote_ident=False)
                # ignore "on delete cascade" fk (they are indirect dependencies (lines or m2m))
                if fk_act != "c"
                # ignore children records unless the deletion is restricted
                if not (fk_tbl == table and fk_act != "r")
            ]
        )
        if sub:
            query = format_query(
                cr,
                r"""
                WITH _kids AS (
                    {}
                )
                SELECT t.id
                  FROM {} t
                  JOIN _kids s
                    ON s.id = t.id
                 WHERE NOT EXISTS({})
                """,
                kids_query,
                table,
                sql.SQL(sub),
            )
            cr.execute(query, {"ids": list(ids)})
            sub_ids = list(map(itemgetter(0), cr.fetchall()))
        else:
            sub_ids = list(ids)

        if model == "res.lang" and table_exists(cr, "ir_translation"):
            cr.execute(
                """
                DELETE FROM ir_translation t
                      USING res_lang l
                      WHERE t.lang = l.code
                        AND l.id = ANY(%s)
                 """,
                [sub_ids],
            )

        remove_records(cr, model, sub_ids)
        deleted.extend(res_id_to_xmlid[r] for r in sub_ids if r in res_id_to_xmlid)

        if deactivate:
            deactivate_ids = tuple(set(sub_ids) - set(ids))
            if deactivate_ids:
                cr.execute(format_query(cr, "UPDATE {} SET active = false WHERE id IN %s", table), [deactivate_ids])

    if not keep_xmlids:
        query = """
            WITH xids AS (
                {}
            )
            DELETE
              FROM ir_model_data d
             USING xids x
             WHERE d.module = x.module
               AND d.name = x.name
        """
        cr.execute(query.format(select_xids))

    return deleted


def replace_record_references(cr, old, new, replace_xmlid=True, parent_field="parent_id"):
    """
    Replace all (in)direct references of a record by another.

    :meta private: exclude from online docs
    """
    # TODO update workflow instances?
    assert isinstance(old, tuple) and len(old) == 2
    assert isinstance(new, tuple) and len(new) == 2

    if not old[1]:
        return None

    return replace_record_references_batch(cr, {old[1]: new[1]}, old[0], new[0], replace_xmlid, parent_field)


def replace_record_references_batch(
    cr, id_mapping, model_src, model_dst=None, replace_xmlid=True, ignores=(), parent_field="parent_id"
):
    """
    Replace all references to records.

    This functions replaces *all* references, direct or indirect to records of `model_src`
    by the corresponding records in the mapping. If the target model of the mapping is not
    the same as the source one, then `model_dst` parameter must be set.

    :param dict(int, int) id_mapping: mapping of IDs to replace, key value is replaced by
                                      the mapped value
    :param str model_src: name of the source model of the records to replace
    :param str model_dst: name of the target model of the records to replace, if `None`
                          the target is assumed the same as the source
    :param bool replace_xmlid: whether to replace the references in xml_ids pointing to
                               the source records
    :param list(str) ignores: list of **table** names to skip when updating the referenced
                              values
    :paream str parent_field: when the target and source model are the same, and the model
                              table has `parent_path` column, this field will be used to
                              update it.
    """
    _validate_model(model_src)
    if model_dst is None:
        model_dst = model_src
    else:
        _validate_model(model_dst)

    if model_src == model_dst:
        same_ids = {k: v for k, v in id_mapping.items() if k == v}
        if same_ids:
            _logger.warning("Replace references in model `%s`, ignoring same-id mapping `%s`", model_src, same_ids)
            id_mapping = {k: v for k, v in id_mapping.items() if k != v}

    assert id_mapping
    assert all(isinstance(v, int) and isinstance(k, int) for k, v in id_mapping.items())

    id_update = any(k != v for k, v in id_mapping.items())

    ignores = [_validate_table(table) for table in ignores]
    if not replace_xmlid:
        ignores.append("ir_model_data")

    cr.execute("CREATE UNLOGGED TABLE _upgrade_rrr(old int PRIMARY KEY, new int)")
    execute_values(cr, "INSERT INTO _upgrade_rrr (old, new) VALUES %s", id_mapping.items())

    if model_src == model_dst:
        fk_def = []

        model_src_table = table_of_model(cr, model_src)
        for table, fk, _, _ in get_fk(cr, model_src_table, quote_ident=False):
            if table in ignores:
                continue
            query = """
                UPDATE {table} t
                   SET {fk} = r.new
                  FROM _upgrade_rrr r
                 WHERE r.old = t.{fk}
            """
            unique_indexes = _get_unique_indexes_with(cr, table, fk)
            if unique_indexes:
                conditions = [""]
                for _, uniq_cols in unique_indexes:
                    uniq_cols = set(uniq_cols) - {fk}  # noqa: PLW2901
                    where_clause = (
                        " AND ".join(format_query(cr, "u.{col} = t.{col}", col=col) for col in uniq_cols)
                        if uniq_cols
                        else "true"
                    )
                    conditions.append("NOT EXISTS(SELECT 1 FROM {table} u WHERE u.{fk} = r.new AND %s)" % where_clause)

                query += " AND ".join(conditions)

            if not column_exists(cr, table, "id"):
                # seems to be a m2m table. Avoid duplicated entries
                (col2,) = get_columns(cr, table, ignore=(fk,)).iter_unquoted()

                # handle possible duplicates after update of m2m table
                cr.execute(
                    format_query(
                        cr,
                        """
                        WITH rr2 AS (
                            SELECT array_agg(t.{fk} ORDER BY t.{fk}) AS olds,
                                   r.new AS new,
                                   t.{col2} AS col2
                              FROM {table} t
                              JOIN _upgrade_rrr r
                                ON r.old = t.{fk}
                             GROUP BY r.new, t.{col2}
                        )
                        DELETE
                          FROM {table} t
                         USING rr2 r
                         WHERE t.{col2} = r.col2
                           AND t.{fk} = ANY(r.olds[2:])
                        """,
                        table=table,
                        fk=fk,
                        col2=col2,
                    )
                )
                query += " AND NOT EXISTS(SELECT 1 FROM {table} e WHERE e.{col2} = t.{col2} AND e.{fk} = r.new)"

                col2_info = target_of(cr, table, col2)  # col2 may not be a FK
                if col2_info and col2_info[:2] == (model_src_table, "id"):
                    # a m2m on itself, remove the self referencing entries
                    # It only handle 1-level recursions. For multi-level recursions, it should be handled manually.
                    # We can't decide which link to break.
                    # XXX: add a warning?
                    query += """;
                        DELETE
                          FROM {table} t
                         USING _upgrade_rrr r
                         WHERE t.{fk} = r.new
                           AND t.{fk} = t.{col2};
                    """

                cr.execute(format_query(cr, query, table=table, fk=fk, col2=col2))

            else:  # it's a model
                fmt_query = format_query(cr, query, table=table, fk=fk)
                parallel_execute(cr, explode_query_range(cr, fmt_query, table=table, alias="t"))

                # track default values to update
                model = model_of_table(cr, table)
                fk_def.append((model, fk))

            delete_query = """
                DELETE
                  FROM {table} t
                 USING _upgrade_rrr r
                 WHERE t.{fk} = r.old
            """
            cr.execute(format_query(cr, delete_query, table=table, fk=fk))

        if fk_def:
            if table_exists(cr, "ir_values"):
                column_read, cast_write = _ir_values_value(cr, prefix="v")
                query = r"""
                    UPDATE ir_values v
                       SET value = {cast_write}
                      FROM _upgrade_rrr r
                     WHERE v.key = 'default'
                       AND {column_read} = format(E'I%%s\n.', r.old)
                       AND (v.model, v.name) IN %s
                """.format(column_read=column_read, cast_write=cast_write % r"format(E'I%%s\n.', r.new)")
            else:
                query = """
                    UPDATE ir_default d
                       SET json_value = r.new::varchar
                      FROM _upgrade_rrr r, ir_model_fields f
                     WHERE f.id = d.field_id
                       AND d.json_value = r.old::varchar
                       AND (f.model, f.name) IN %s
                """

            cr.execute(query, [tuple(fk_def)])

    cr.execute("SELECT id FROM ir_model WHERE model=%s", [model_dst])
    model_dest_id = cr.fetchone()[0]

    # indirect references
    for ir in indirect_references(cr, bound_only=True):
        if ir.table in ignores:
            continue
        if ir.company_dependent_comodel:
            if ir.company_dependent_comodel == model_src:
                if model_src != model_dst:
                    cr.execute(
                        "UPDATE ir_model_fields SET relation = %s WHERE model = %s AND name = %s",
                        [
                            model_dst,
                            model_of_table(cr, ir.table),
                            ir.res_id,
                        ],
                    )
                query = format_query(
                    cr,
                    """
                     WITH _upg_cd AS (
                         SELECT t.id,
                                jsonb_object_agg(j.key, COALESCE(r.new, j.value::int)) as value
                           FROM {table} t
                           JOIN jsonb_each_text(t.{column}) j
                             ON true
                      LEFT JOIN _upgrade_rrr r
                             ON r.old = j.value::integer
                          WHERE {{parallel_filter}}
                       GROUP BY t.id
                         HAVING bool_or(r.new IS NOT NULL)
                     )
                     UPDATE {table} t
                        SET {column} = u.value
                       FROM _upg_cd u
                      WHERE u.id = t.id
                    """,
                    table=ir.table,
                    column=ir.res_id,
                )
                explode_execute(cr, query, table=ir.table, alias="t")
                # ensure all new ids exist
                cr.execute(
                    format_query(
                        cr,
                        """
                        Select t.id AS id,
                               j.key AS c_id,
                               j.value AS ref
                          FROM {table} t
                          JOIN JSONB_EACH_TEXT(t.{column}) j
                            ON True
                         WHERE j.value IS NOT NULL
                           AND NOT EXISTS (
                                    SELECT 1 FROM {dest_table} WHERE id = j.value::int
                               )
                        """,
                        table=ir.table,
                        column=ir.res_id,
                        dest_table=table_of_model(cr, model_dst),
                    )
                )
                invalid_ref = cr.dictfetchall()
                if invalid_ref:
                    raise RuntimeError(
                        "Invalid company dependent values for {}.{} referencing model {}: {}".format(
                            model_of_table(cr, ir.table), ir.res_id, model_dst, invalid_ref
                        )
                    )
            continue
        res_model_upd = []
        if ir.res_model:
            res_model_upd.append(format_query(cr, "{} = %(model_dst)s", ir.res_model))
        if ir.res_model_id:
            res_model_upd.append(format_query(cr, "{} = %(model_dest_id)s", ir.res_model_id))
        upd = SQLStr(", ".join(res_model_upd))
        res_model_whr = SQLStr(" AND ".join(res_model_upd))
        whr = ir.model_filter(placeholder="%(model_src)s", prefix="t.")

        if not id_update:
            jmap_expr = SQLStr("true")  # no-op
            jmap_expr_upd = SQLStr("")
        else:
            jmap_expr = format_query(cr, "{} = _upgrade_rrr.new", ir.res_id)
            jmap_expr_upd = SQLStr(", " + jmap_expr)

        query = format_query(
            cr,
            """
            UPDATE {table} t
               SET {upd}
                   {jmap_expr_upd}
              FROM _upgrade_rrr
              {{extra_join}}
             WHERE {whr}
               AND _upgrade_rrr.old = t.{res_id}
            """,
            table=ir.table,
            res_id=ir.res_id,
            whr=whr,
            jmap_expr_upd=jmap_expr_upd,
            upd=upd,
        )

        unique_indexes = []
        if ir.res_model:
            unique_indexes += _get_unique_indexes_with(cr, ir.table, ir.res_id, ir.res_model)
        if ir.res_model_id:
            unique_indexes += _get_unique_indexes_with(cr, ir.table, ir.res_id, ir.res_model_id)
        if unique_indexes:
            query = format_query(
                cr,
                query,
                extra_join=format_query(
                    cr,
                    """
         LEFT JOIN _upgrade_rrr AS _upgrade_rrr2
                ON _upgrade_rrr.new = _upgrade_rrr2.new
               AND _upgrade_rrr.old < _upgrade_rrr2.old
         LEFT JOIN {table} t2
                ON _upgrade_rrr2.old = t2.{res_id}
                    """,
                    table=ir.table,
                    res_id=ir.res_id,
                ),
            )
            conditions = []
            for _, uniq_cols in unique_indexes:
                uniq_cols = set(uniq_cols) - {ir.res_id, ir.res_model, ir.res_model_id}  # noqa: PLW2901
                conditions.append(
                    format_query(
                        cr,
                        """
                         -- there is no target already present within the same constraint
                        NOT EXISTS(SELECT 1 FROM {table} WHERE {res_model_whr} AND {jmap_expr} AND {where_clause})
                         -- there is no other entry with the same target within the same constraint
                        AND NOT (t2 IS NOT NULL AND {where_clause2})
                        """,
                        table=ir.table,
                        res_model_whr=res_model_whr,
                        jmap_expr=jmap_expr,
                        where_clause=SQLStr(" AND ".join(format_query(cr, "{0}=t.{0}", col) for col in uniq_cols))
                        if uniq_cols
                        else SQLStr("True"),
                        where_clause2=SQLStr(" AND ".join(format_query(cr, "t2.{0}=t.{0}", col) for col in uniq_cols))
                        if uniq_cols
                        else SQLStr("True"),
                    )
                )
            query = format_query(
                cr,
                """{prev_query}   AND {cond};

            DELETE FROM {table} t USING _upgrade_rrr WHERE {whr} AND t.{res_id} = _upgrade_rrr.old;
                """,
                prev_query=query,
                cond=SQLStr("\nAND ".join(conditions)),
                table=ir.table,
                whr=whr,
                res_id=ir.res_id,
            )
            cr.execute(query, locals())
        else:
            fmt_query = cr.mogrify(format_query(cr, query, extra_join=SQLStr("")), locals()).decode()
            parallel_execute(cr, explode_query_range(cr, fmt_query, table=ir.table, alias="t"))

    # reference fields
    cr.execute("SELECT model, name FROM ir_model_fields WHERE ttype='reference'")
    for model, column in cr.fetchall():
        table = table_of_model(cr, model)
        if table not in ignores and column_updatable(cr, table, column):
            cr.execute(
                """
                    WITH _ref AS (
                        SELECT concat(%s, ',', old) as old, concat(%s, ',', new) as new
                          FROM _upgrade_rrr
                    )
                    UPDATE "{table}" t
                       SET "{column}" = r.new
                      FROM _ref r
                     WHERE t."{column}" = r.old
            """.format(table=table, column=column),
                [model_src, model_dst],
            )

    cr.execute("DROP TABLE _upgrade_rrr")
    if parent_field and model_dst == model_src:
        if column_exists(cr, model_src_table, "parent_path"):
            fk_target = target_of(cr, model_src_table, parent_field)
            if fk_target:
                if fk_target[0] != model_src_table:
                    _logger.warning(
                        "`%s` has a `parent_path` but `%s` is not a self-referencing FK", model_src_table, parent_field
                    )
                else:
                    update_parent_path(cr, model_src, parent_field)
            elif parent_field != "parent_id":  # check non-default value
                _logger.error("`%s` in `%s` is not a self-referencing FK", parent_field, model_src_table)
        elif column_exists(cr, model_src_table, "parent_left"):
            _logger.warning("Possibly missing update of parent_left/right in `%s`", model_src_table)


def replace_in_all_jsonb_values(cr, table, column, old, new, extra_filter=None):
    r"""
    Replace values in JSONB columns.

    This function replaces `old` by `new` in JSONB values. It is useful for replacing
    values in *all* translations of translated fields.

    :param str table: table name where the values are to be replaced
    :param str column: column name where the values are to be replaced
    :param str old: original value to replace, can be a simple term (str) or a Postgres
                    regular expression wrapped by :class:`~odoo.upgrade.util.pg.PGRegexp`
    :param str new: new value to set, can be a simple term or a expression using
                    `\<number>` notation to refer to *captured groups* if `old` is a
                    regexp expression
    :param str extra_filter: extra `WHERE` compatible clause to filter the values to
                             update, must use the `t` alias for the table, it can also
                             include `{parallel_filter}` to execute the query in parallel,
                             see :func:`~odoo.upgrade.util.pg.explode_execute`
    """
    re_old = (
        old
        if isinstance(old, PGRegexp)
        else "{}{}{}".format(
            r"\y" if re.match(r"\w", old[0]) else "",
            re.escape(old),
            r"\y" if re.match(r"\w", old[-1]) else "",
        )
    )
    match = str(Json(re_old))[1:-1]  # escapes re_old into json string

    if extra_filter is None:
        extra_filter = "true"

    query = cr.mogrify(
        """
        WITH upd AS (
             SELECT t.id,
                    jsonb_object_agg(v.key, regexp_replace(v.value, %s, %s, 'g')) AS value
               FROM "{table}" t
               JOIN LATERAL jsonb_each_text(t."{column}") v
                 ON true
              WHERE jsonb_path_match(t."{column}", 'exists($.* ? (@ like_regex {match}))')
                AND {extra_filter}
              GROUP BY t.id
        )
        UPDATE "{table}" t
           SET "{column}" = upd.value
          FROM upd
         WHERE upd.id = t.id
        """.format(**locals()),
        [re_old, new],
    ).decode()

    if "{parallel_filter}" in query:
        explode_execute(cr, query, table=table, alias="t")
    else:
        cr.execute(query)


def ensure_mail_alias_mapping(cr, model, record_xmlid, alias_xmlid, alias_name):
    _validate_model(model)

    cr.execute("SELECT id FROM ir_model WHERE model = %s", [model])
    (model_id,) = cr.fetchone()
    alias_id = ensure_xmlid_match_record(
        cr,
        alias_xmlid,
        "mail.alias",
        {
            "alias_name": alias_name,
            "alias_parent_model_id": model_id,
        },
    )

    if alias_id:
        ensure_xmlid_match_record(
            cr,
            record_xmlid,
            model,
            {"alias_id": alias_id},
        )


def remove_act_window_view_mode(cr, model, view_mode):
    default = "list,form" if version_gte("saas~17.5") else "tree,form"
    cr.execute(
        """
        WITH upd AS (
            UPDATE ir_act_window act
               SET view_mode = COALESCE(
                      NULLIF(
                          ARRAY_TO_STRING(ARRAY_REMOVE(STRING_TO_ARRAY(view_mode, ','), %s), ','),
                          '' -- invalid value
                      ),
                      %s -- default value
                   )
             WHERE act.res_model = %s
               AND %s = ANY(STRING_TO_ARRAY(act.view_mode, ','))
         RETURNING act.id

        )
        DELETE FROM ir_act_window_view av
              USING upd
              WHERE upd.id = av.act_window_id
                AND av.view_mode=%s
        """,
        [view_mode, default, model, view_mode, view_mode],
    )


def _remove_redundant_tcalls(cr, match):
    """
    Remove t-calls of the removed view.

    This function removes the t-calls to `match`.

    :param str match: t-calls value to remove, typically it would be a view's xml_id or key
    """
    arch_col = (
        get_value_or_en_translation(cr, "ir_ui_view", "arch_db")
        if column_exists(cr, "ir_ui_view", "arch_db")
        else "arch"
    )
    cr.execute(
        format_query(
            cr,
            """
            SELECT iv.id,
                   imd.module,
                   imd.name
              FROM ir_ui_view iv
         LEFT JOIN ir_model_data imd
                ON iv.id = imd.res_id
               AND imd.model = 'ir.ui.view'
             WHERE {} ~ %s
        """,
            sql.SQL(arch_col),
        ),
        [r"""\yt-call=(["']){}\1""".format(re.escape(match))],
    )
    standard_modules = set(modules.get_modules()) - {"studio_customization"}
    for vid, module, name in cr.fetchall():
        with edit_view(cr, view_id=vid) as arch:
            for node in arch.findall(".//t[@t-call='{}']".format(match)):
                node.getparent().remove(node)
        if not module or module not in standard_modules:
            _logger.info(
                "The view %swith ID: %s has been updated, removed t-calls to deprecated %r",
                ("`{}.{}` ".format(module, name) if module else ""),
                vid,
                match,
            )


def update_parent_path(cr, model, parent_field="parent_id"):
    """
    Trigger the update of parent paths in a model.

    :meta private: exclude from online docs
    """
    if not version_gte("saas~11.3"):
        _logger.error("parent_left and parent_right must be computed via the ORM")
        return
    table = table_of_model(cr, model)
    name_field = "name" if column_exists(cr, table, "name") else "id"
    break_recursive_loops(cr, model, parent_field, name_field)
    query = format_query(
        cr,
        """
        WITH RECURSIVE __parent_store_compute(id, parent_path) AS (
             SELECT row.id,
                    concat(row.id, '/')
               FROM {table} row
              WHERE row.{parent_field} IS NULL

            UNION

             SELECT row.id,
                    concat(comp.parent_path, row.id, '/')
               FROM {table} row
               JOIN __parent_store_compute comp
                 ON row.{parent_field} = comp.id
        )
        UPDATE {table} row
           SET parent_path = comp.parent_path
          FROM __parent_store_compute comp
         WHERE row.id = comp.id
        """,
        table=table,
        parent_field=parent_field,
    )
    cr.execute(query)
