# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager

# python3 shims
try:
    unicode  # noqa: B018
except NameError:
    unicode = str

import lxml
from psycopg2.extras import Json

try:
    from odoo.tools.translate import xml_translate
except ImportError:
    xml_translate = lambda callback, value: value

from ..pg import column_exists, column_type
from ..report import add_to_migration_reports

_logger = logging.getLogger(__name__)


__all__ = [
    "remove_view",
    "edit_view",
    "add_view",
]


def remove_view(cr, xml_id=None, view_id=None, silent=False, key=None):
    """
    Recursively delete the given view and its inherited views, as long as they
    are part of a module. Will crash as soon as a custom view exists anywhere
    in the hierarchy.

    Also handle multi-website COWed views.
    """
    from ..records import ref, remove_records

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
        _logger.info("remove deprecated %s view %s (ID %s)", key and "COWed" or "built-in", key or xml_id, view_id)

    remove_records(cr, "ir.ui.view", [view_id])


@contextmanager
def edit_view(cr, xmlid=None, view_id=None, skip_if_not_noupdate=True, active=True):
    """Contextmanager that may yield etree arch of a view.
    As it may not yield, you must use `skippable_cm`

        with util.skippable_cm(), util.edit_view(cr, 'xml.id') as arch:
            arch.attrib['string'] = 'My Form'

    When view_id is passed to identify a view, view's arch will always yield to be edited because
    we assume that xmlid for such view does not exist to check its noupdate flag.

    If view's noupdate=false then the arch will not be yielded for edit unless skip_if_not_noupdate=False,
    because when noupdate=False we assume it is a standard view that will be updated by the ORM later on anyways.

    If view's noupdate=True, the view will be yielded for edit.

    If the `active` argument is not None, the view will be (de)activated accordingly.

    For more details, see discussion in: https://github.com/odoo/upgrade-specific/pull/4216
    """
    assert bool(xmlid) ^ bool(view_id), "You Must specify either xmlid or view_id"
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


def add_view(cr, name, model, view_type, arch_db, inherit_xml_id=None, priority=16):
    from ..records import ref

    inherit_id = None
    if inherit_xml_id:
        inherit_id = ref(cr, inherit_xml_id)
        if not inherit_id:
            raise ValueError(
                "Unable to add view '%s' because its inherited view '%s' cannot be found!" % (name, inherit_xml_id)
            )
    arch_col = "arch_db" if column_exists(cr, "ir_ui_view", "arch_db") else "arch"
    jsonb_column = column_type(cr, "ir_ui_view", arch_col) == "jsonb"
    arch_column_value = Json({"en_US": arch_db}) if jsonb_column else arch_db
    cr.execute(
        """
        INSERT INTO ir_ui_view(name, "type",  model, inherit_id, mode, active, priority, %s)
        VALUES(%%(name)s, %%(view_type)s, %%(model)s, %%(inherit_id)s, %%(mode)s, 't', %%(priority)s, %%(arch_db)s)
        RETURNING id
    """
        % arch_col,
        {
            "name": name,
            "view_type": view_type,
            "model": model,
            "inherit_id": inherit_id,
            "mode": "extension" if inherit_id else "primary",
            "priority": priority,
            "arch_db": arch_column_value,
        },
    )
    return cr.fetchone()[0]
