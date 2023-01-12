# -*- coding: utf-8 -*-
"""Utility functions for record-level operations."""

import logging
import os
import re
from operator import itemgetter

import lxml
from psycopg2.extras import Json, execute_values

try:
    from odoo import release
    from odoo.tools.convert import xml_import
    from odoo.tools.misc import file_open
except ImportError:
    from openerp import release
    from openerp.tools.convert import xml_import
    from openerp.tools.misc import file_open

from .const import NEARLYWARN
from .exceptions import MigrationError
from .helpers import _get_theme_models, _ir_values_value, _validate_model, model_of_table, table_of_model
from .indirect_references import indirect_references
from .inherit import direct_inherit_parents, for_each_inherit
from .misc import parse_version, version_gte
from .orm import env, flush
from .pg import (
    PGRegexp,
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
from .views.records import add_view, edit_view, remove_view  # noqa: F401

_logger = logging.getLogger(__name__)

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = unicode = str


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
                # column may not exists in case of a partially unintalled module that left only *magic columns* in tables
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
    cr.execute('DELETE FROM "{}" WHERE id IN %s'.format(table), [ids])
    for ir in indirect_references(cr, bound_only=True):
        query = 'DELETE FROM "{}" WHERE {} AND "{}" IN %s'.format(ir.table, ir.model_filter(), ir.res_id)
        cr.execute(query, [model, ids])
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


def is_changed(cr, xmlid, interval="1 minute"):
    """
    Return whether a record was changed.

    This function checks if a record was changed before the current upgrade start time.
    See :file:`upgrade-util/src/base/0.0.0/pre-00-upgrade-start.py`

    This utility will return a false positive on xmlids of records that match the
    following conditions:

     * Have been updated in an upgrade preceding the current one
     * Have not been updated in the current upgrade

    :param str xmlid: `xmlid` of the record to check
    :param str interval: SQL interval, a record is considered as changed if
                         `write_date > create_date + interval`
    :rtype: bool
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
    if not is_changed(cr, xmlid, interval=interval):
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
        )
        DELETE FROM ir_ui_menu m
              USING tree t
              WHERE m.id = t.id
          RETURNING m.id
    """,
        [tuple(menu_ids)],
    )
    ids = tuple(x[0] for x in cr.fetchall())
    if ids:
        cr.execute("DELETE FROM ir_model_data WHERE model='ir.ui.menu' AND res_id IN %s", [ids])


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
    :rtype: int or `None`
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
                # iif the key has been updated for this view, also update it for all other cowed views.
                # Don't change the view keys inconditionally to avoid changing unrelated views.
                cr.execute("UPDATE ir_ui_view SET key = %s WHERE key = %s", [new, old])

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
    :rtype: int or `None`
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
    force_create=True,
    from_module=None,
    reset_translations=(),
    ensure_references=False,
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
    :param bool force_create: whether the record is created if it does not exist
    :param str from_module: name of the module from which to update the record, necessary
                            only when the specs are in a different module than the one in
                            the xml_id
    :param set(str) reset_translations: field names whose translations get reset
    :param bool ensure_references: whether referred records via `ref` XML attributes
                                   should also be updated.

    .. warning::
       This functions uses the ORM, therefore it can only be used after **all** models
       referenced in the data specs of the record are already **loaded**. In practice this
       means that this function should be used in `post-` or `end-` scripts.

    .. note::
       The standard behavior of ORM is to create the record if it doesn't exist, including
       its xml_id. That will happen on this function as well.
    """
    __update_record_from_xml(
        cr,
        xmlid,
        reset_write_metadata=reset_write_metadata,
        force_create=force_create,
        from_module=from_module,
        reset_translations=reset_translations,
        ensure_references=ensure_references,
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
    done_refs,
):
    from .modules import get_manifest

    # Force update of a record from xml file to bypass the noupdate flag
    if "." not in xmlid:
        raise ValueError("Please use fully qualified name <module>.<name>")

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
                        add_ref(ref_node.get("ref"))
                    for eval_node in node.xpath("//field[@eval]"):
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
            fields_with_values_from_xml = {elem.attrib["name"] for elem in node.xpath("//record/field")}
            if template:
                fields_with_values_from_xml |= {"arch_db", "name"}
            cr.execute(
                "SELECT name FROM ir_model_fields WHERE model = %s AND translate = true AND name IN %s",
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
    from any table.

    .. note::
       The records that cannot be removed are set as `noupdate=True`.

    :param list(str) xmlids: list of xml_ids to check for removal
    :param bool deactivate: whether to deactivate records that cannot be removed because
                            they are referenced, `False` by default
    :param bool keep_xmlids: whether to keep the xml_ids of records that cannot be
                             removed, `True` by default
    :return: list of ids of removed records, if any
    :rtype: list(int)
    """
    deactivate = kwargs.pop("deactivate", False)
    keep_xmlids = kwargs.pop("keep_xmlids", True)
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
    for model, ids, xmlids in cr.fetchall():
        table = table_of_model(cr, model)
        res_id_to_xmlid = dict(zip(ids, xmlids))

        sub = " UNION ".join(
            [
                'SELECT 1 FROM "{}" x WHERE x."{}" = t.id'.format(fk_tbl, fk_col)
                for fk_tbl, fk_col, _, fk_act in get_fk(cr, table, quote_ident=False)
                # ignore "on delete cascade" fk (they are indirect dependencies (lines or m2m))
                if fk_act != "c"
                # ignore children records unless the deletion is restricted
                if not (fk_tbl == table and fk_act != "r")
            ]
        )
        if sub:
            cr.execute(
                """
                SELECT id
                  FROM "{}" t
                 WHERE id = ANY(%s)
                   AND NOT EXISTS({})
            """.format(table, sub),
                [list(ids)],
            )
            ids = map(itemgetter(0), cr.fetchall())  # noqa: PLW2901

        ids = list(ids)  # noqa: PLW2901
        if model == "res.lang" and table_exists(cr, "ir_translation"):
            cr.execute(
                """
                DELETE FROM ir_translation t
                      USING res_lang l
                      WHERE t.lang = l.code
                        AND l.id = ANY(%s)
                 """,
                [ids],
            )
        for tid in ids:
            remove_record(cr, (model, tid))
            deleted.append(res_id_to_xmlid[tid])

        if deactivate:
            deactivate_ids = tuple(set(res_id_to_xmlid.keys()) - set(ids))
            if deactivate_ids:
                cr.execute('UPDATE "{}" SET active = false WHERE id IN %s'.format(table), [deactivate_ids])

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


def replace_record_references(cr, old, new, replace_xmlid=True):
    """
    Replace all (in)direct references of a record by another.

    :meta private: exclude from online docs
    """
    # TODO update workflow instances?
    assert isinstance(old, tuple) and len(old) == 2
    assert isinstance(new, tuple) and len(new) == 2

    if not old[1]:
        return None

    return replace_record_references_batch(cr, {old[1]: new[1]}, old[0], new[0], replace_xmlid)


def replace_record_references_batch(cr, id_mapping, model_src, model_dst=None, replace_xmlid=True, ignores=()):
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
    """
    assert id_mapping
    assert all(isinstance(v, int) and isinstance(k, int) for k, v in id_mapping.items())

    _validate_model(model_src)
    if model_dst is None:
        model_dst = model_src
    else:
        _validate_model(model_dst)

    id_update = any(k != v for k, v in id_mapping.items())
    nop = (not id_update) and (model_src == model_dst)
    assert nop is False

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
                    ands = (
                        " AND ".join(format_query(cr, "u.{col} = t.{col}", col=col) for col in uniq_cols)
                        if uniq_cols
                        else "true"
                    )
                    conditions.append("NOT EXISTS(SELECT 1 FROM {table} u WHERE u.{fk} = r.new AND %s)" % ands)

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
        res_model_upd = []
        if ir.res_model:
            res_model_upd.append('"{ir.res_model}" = %(model_dst)s')
        if ir.res_model_id:
            res_model_upd.append('"{ir.res_model_id}" = %(model_dest_id)s')
        upd = ", ".join(res_model_upd).format(ir=ir)
        res_model_whr = " AND ".join(res_model_upd).format(ir=ir)
        whr = ir.model_filter(placeholder="%(model_src)s")

        if not id_update:
            jmap_expr = "true"  # no-op
            jmap_expr_upd = ""
        else:
            jmap_expr = '"{ir.res_id}" = _upgrade_rrr.new'.format(ir=ir)
            jmap_expr_upd = ", " + jmap_expr

        query = """
            UPDATE "{ir.table}" t
               SET {upd}
                   {jmap_expr_upd}
              FROM _upgrade_rrr
             WHERE {whr}
               AND _upgrade_rrr.old = {ir.res_id}
        """

        unique_indexes = []
        if ir.res_model:
            unique_indexes += _get_unique_indexes_with(cr, ir.table, ir.res_id, ir.res_model)
        if ir.res_model_id:
            unique_indexes += _get_unique_indexes_with(cr, ir.table, ir.res_id, ir.res_model_id)
        if unique_indexes:
            conditions = []
            for _, uniq_cols in unique_indexes:
                uniq_cols = set(uniq_cols) - {ir.res_id, ir.res_model, ir.res_model_id}  # noqa: PLW2901
                conditions.append(
                    """
                        NOT EXISTS(SELECT 1 FROM {ir.table} WHERE {res_model_whr} AND {jmap_expr} AND %(ands)s)
                    """
                    % {"ands": "AND".join('"%s"=t."%s"' % (col, col) for col in uniq_cols) if uniq_cols else "True"}
                )
            query = """
                    %s
                    AND %s;
                DELETE FROM {ir.table} USING _upgrade_rrr WHERE {whr} AND {ir.res_id} = _upgrade_rrr.old;
            """ % (
                query,
                "AND".join(conditions),
            )
            cr.execute(query.format(**locals()), locals())
        else:
            fmt_query = cr.mogrify(query.format(**locals()), locals()).decode()
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
    cr.execute(
        """
        WITH upd AS (
            UPDATE ir_act_window act
               SET view_mode = COALESCE(
                      NULLIF(
                          ARRAY_TO_STRING(ARRAY_REMOVE(STRING_TO_ARRAY(view_mode, ','), %s), ','),
                          '' -- invalid value
                      ),
                      'tree,form' -- default value
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
        [view_mode, model, view_mode, view_mode],
    )
