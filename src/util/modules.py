# -*- coding: utf-8 -*-
"""
Utility functions for module-level operations.

In most cases module operations (rename, merge, remove, ...) should be performed in a
`base` script. The reason is that once the `base` module is upgraded, all the information
regarding modules should be already set in the DB for the upgrade process to function
correctly.
"""

try:
    from collections.abc import Sequence, Set
except ImportError:
    from collections import Sequence, Set

import itertools
import logging
import os
from inspect import currentframe
from operator import itemgetter

try:
    import odoo
    from odoo.modules.db import create_categories
    from odoo.tools.func import frame_codeinfo
    from odoo.tools.misc import topological_sort

    try:
        from odoo.modules import get_manifest
    except ImportError:
        from odoo.modules import load_information_from_description_file as get_manifest
except ImportError:
    import openerp as odoo
    from openerp.modules import load_information_from_description_file as get_manifest
    from openerp.modules.db import create_categories
    from openerp.tools.func import frame_codeinfo

    try:
        from openerp.tools.misc import topological_sort
    except ImportError:
        from openerp.addons.web.controllers.main import module_topological_sort as topological_sort

from .const import ENVIRON, NEARLYWARN
from .exceptions import MigrationError, SleepyDeveloperError
from .fields import remove_field
from .helpers import _validate_model, table_of_model
from .misc import on_CI, str2bool, version_gte
from .models import delete_model
from .orm import env, flush
from .pg import column_exists, table_exists, target_of
from .records import ref, remove_group, remove_menus, remove_records, remove_view, replace_record_references_batch

INSTALLED_MODULE_STATES = ("installed", "to install", "to upgrade")
_logger = logging.getLogger(__name__)

if version_gte("15.0"):
    AUTO_INSTALL = os.getenv("UPG_AUTOINSTALL")
    _NO_AUTOINSTALL = os.getenv("UPG_NO_AUTOINSTALL")
    if AUTO_INSTALL:
        if _NO_AUTOINSTALL:
            _logger.info("Ignoring the deprecated `UPG_NO_AUTOINSTALL` env variable as `UPG_AUTOINSTALL` is also set.")
    elif _NO_AUTOINSTALL:
        AUTO_INSTALL = "only_link_modules" if str2bool(_NO_AUTOINSTALL) else "all"
        # TODO add link to doc
        _logger.warning(
            "Usage of the `UPG_NO_AUTOINSTALL` environment variable is deprecated. Please use `UPG_AUTOINSTALL=%s`.",
            AUTO_INSTALL,
        )
    else:
        AUTO_INSTALL = "all"
else:
    AUTO_INSTALL = "all"

if AUTO_INSTALL not in ("all", "only_link_modules", "none"):
    raise ValueError("Invalid value for the `UPG_AUTOINSTALL` environment variable: {!r}".format(AUTO_INSTALL))

_logger.info("module auto-install strategy: %s", AUTO_INSTALL)

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = str


def modules_installed(cr, *modules):
    """
    Return whether *all* given modules are installed.

    :param list(str) modules: names of the modules to check
    :rtype: bool

    .. note::

        In the context of upgrades a module is considered as installed if it is marked for
        upgrade, or for installation; even if they are not yet fully installed.
    """
    assert modules
    cr.execute(
        """
            SELECT count(1)
              FROM ir_module_module
             WHERE name IN %s
               AND state IN %s
    """,
        [modules, INSTALLED_MODULE_STATES],
    )
    return cr.fetchone()[0] == len(modules)


def module_installed(cr, module):
    """
    Return whether a module is installed.

    :param str module: name of the module to check
    :rtype: bool

    See :func:`modules_installed`.
    """
    return modules_installed(cr, module)


def uninstall_module(cr, module):
    """
    Uninstall and remove all records owned by a module.

    :param str module: name of the module to uninstall
    """
    cr.execute("SELECT id FROM ir_module_module WHERE name=%s", (module,))
    (mod_id,) = cr.fetchone() or [None]
    if not mod_id:
        return

    # delete constraints only owned by this module
    cr.execute(
        """
            SELECT name
              FROM ir_model_constraint
          GROUP BY name
            HAVING array_agg(module) = %s
    """,
        ([mod_id],),
    )

    constraints = tuple(map(itemgetter(0), cr.fetchall()))
    if constraints:
        cr.execute(
            """
                SELECT table_name, constraint_name
                  FROM information_schema.table_constraints
                 WHERE constraint_name IN %s
        """,
            [constraints],
        )
        for table, constraint in cr.fetchall():
            cr.execute('ALTER TABLE "%s" DROP CONSTRAINT "%s"' % (table, constraint))

    cr.execute(
        """
            DELETE
              FROM ir_model_constraint
             WHERE module = %s
    """,
        [mod_id],
    )

    # delete data
    model_ids, field_ids, menu_ids = [], [], []

    # some models' data needs to be remove before or after others.
    firsts = ["ir.rule"]
    lasts = ["ir.actions.server"]

    cr.execute(
        """
            SELECT model, res_id
              FROM ir_model_data d
             WHERE NOT EXISTS (SELECT 1
                                 FROM ir_model_data
                                WHERE id != d.id
                                  AND res_id = d.res_id
                                  AND model = d.model
                                  AND module != d.module)
               AND module = %s
               AND model != 'ir.module.module'
          ORDER BY array_position(%s::text[], model::text) NULLS LAST,
                   array_position(%s::text[], model::text) NULLS FIRST,
                   id DESC
    """,
        [module, firsts, lasts],
    )
    to_group = []
    for model, res_id in cr.fetchall():
        if model == "ir.model":
            model_ids.append(res_id)
        elif model == "ir.model.fields":
            field_ids.append(res_id)
        elif model == "ir.ui.menu":
            menu_ids.append(res_id)
        else:
            to_group.append((model, res_id))

    for model, group in itertools.groupby(to_group, lambda it: it[0]):
        if model == "ir.ui.view":
            for _, res_id in group:
                remove_view(cr, view_id=res_id, silent=True)
        elif model == "res.groups":
            for _, res_id in group:
                remove_group(cr, group_id=res_id)
        else:
            remove_records(cr, model, [it[1] for it in group])

    if menu_ids:
        remove_menus(cr, menu_ids)

    # remove relations
    cr.execute(
        """
            SELECT name
              FROM ir_model_relation
          GROUP BY name
            HAVING array_agg(module) = %s
    """,
        ([mod_id],),
    )
    relations = tuple(map(itemgetter(0), cr.fetchall()))
    cr.execute("DELETE FROM ir_model_relation WHERE module=%s", (mod_id,))
    if relations:
        cr.execute("SELECT table_name FROM information_schema.tables WHERE table_name IN %s", (relations,))
        for (rel,) in cr.fetchall():
            cr.execute('DROP TABLE "%s" CASCADE' % (rel,))

    if model_ids:
        cr.execute("SELECT model FROM ir_model WHERE id IN %s", [tuple(model_ids)])
        for (model,) in cr.fetchall():
            delete_model(cr, model)

    if field_ids:
        cr.execute("SELECT model, name FROM ir_model_fields WHERE id IN %s", [tuple(field_ids)])
        for model, name in cr.fetchall():
            if name == "id":
                delete_model(cr, model)
            else:
                remove_field(cr, model, name)

    cr.execute("DELETE FROM ir_model_data WHERE module=%s", (module,))
    if table_exists(cr, "ir_translation"):
        cr.execute("DELETE FROM ir_translation WHERE module=%s", [module])
    cr.execute("UPDATE ir_module_module SET state='uninstalled' WHERE name=%s", (module,))


def uninstall_theme(cr, theme, base_theme=None):
    """
    Uninstall a theme module and remove it from websites.

    :param str theme: name of the theme module to uninstall
    :param str or None base_theme: if not `None`, unload first this base theme

    .. warning::

        This function can only be used in `post-` scripts of `website` module as it relies
        on the ORM.

    See :func:`remove_theme` and :func:`uninstall_module`.
    """
    cr.execute("SELECT id FROM ir_module_module WHERE name=%s AND state in %s", [theme, INSTALLED_MODULE_STATES])
    (theme_id,) = cr.fetchone() or [None]
    if not theme_id:
        return

    env_ = env(cr)
    IrModuleModule = env_["ir.module.module"]
    if base_theme:
        cr.execute("SELECT id FROM ir_module_module WHERE name=%s", (base_theme,))
        (website_theme_id,) = cr.fetchone() or [None]
        theme_extension = IrModuleModule.browse(theme_id)
        for website in env_["website"].search([("theme_id", "=", website_theme_id)]):
            theme_extension._theme_unload(website)
    else:
        websites = env_["website"].search([("theme_id", "=", theme_id)])
        for website in websites:
            IrModuleModule._theme_remove(website)
    flush(env_["base"])
    uninstall_module(cr, theme)


def remove_module(cr, module):
    """
    Completely remove a module.

    This operation is equivalent to uninstall and removal of *all* references to
    the module - no trace of it is left in the database.

    :param str module: name of the module to remove

    .. warning::
       Since this function removes *all* data associated to the module. Ensure to
       reassign records before calling this function.
    """
    # NOTE: we cannot use the uninstall of module because the given
    # module need to be currently installed and running as deletions
    # are made using orm.

    uninstall_module(cr, module)
    cr.execute("DELETE FROM ir_module_module_dependency WHERE name=%s", (module,))
    cr.execute("DELETE FROM ir_module_module WHERE name=%s RETURNING id", (module,))
    if cr.rowcount:
        [mod_id] = cr.fetchone()
        cr.execute("DELETE FROM ir_model_data WHERE model='ir.module.module' AND res_id=%s", [mod_id])


def remove_theme(cr, theme, base_theme=None):
    """
    Uninstall a theme module.

    .. warning::
       This function can only be used in `post-` scripts.

    See :func:`remove_module` and :func:`uninstall_theme`.
    """
    uninstall_theme(cr, theme, base_theme=base_theme)
    cr.execute("DELETE FROM ir_module_module_dependency WHERE name=%s", (theme,))
    cr.execute("DELETE FROM ir_module_module WHERE name=%s RETURNING id", (theme,))
    if cr.rowcount:
        [mod_id] = cr.fetchone()
        cr.execute("DELETE FROM ir_model_data WHERE model='ir.module.module' AND res_id=%s", [mod_id])


def _update_view_key(cr, old, new):
    # update view key for renamed & merged modules, also handle multi-website
    # COWed views.
    # View key is not always equal to it's xml_id (eg when created through a
    # website.page record, the key is the page xml_id suffixed by `_view`)
    if not column_exists(cr, "ir_ui_view", "key"):
        return
    like_old = old.replace("_", r"\_").replace("%", r"\%")
    cr.execute(
        """
        UPDATE ir_ui_view
           SET key = concat('{new}', right(key, -length('{old}')))
         WHERE key LIKE '{like_old}.%'
    """.format(old=old, new=new, like_old=like_old)
    )


def rename_module(cr, old, new):
    """
    Rename a module and all references to it.

    :param str old: current name of the module to rename
    :param str new: new name of the module to rename
    """
    cr.execute("UPDATE ir_module_module SET name=%s WHERE name=%s", (new, old))
    cr.execute("UPDATE ir_module_module_dependency SET name=%s WHERE name=%s", (new, old))
    _update_view_key(cr, old, new)
    cr.execute("UPDATE ir_model_data SET module=%s WHERE module=%s", (new, old))
    if table_exists(cr, "ir_translation"):
        cr.execute("UPDATE ir_translation SET module=%s WHERE module=%s", [new, old])

    mod_old = "module_" + old
    mod_new = "module_" + new
    cr.execute(
        """
            UPDATE ir_model_data
               SET name = %s
             WHERE name = %s
               AND module = %s
               AND model = %s
    """,
        [mod_new, mod_old, "base", "ir.module.module"],
    )


def merge_module(cr, old, into, update_dependers=True):
    """
    Merge a module into another.

    This function moves all references and records from the source module to the
    destination module.

    .. warning::
       This functions does not remove any record, but it removes xml_ids from the source
       module with a conflicting name in the destination module.

    :param str old: name of the module to be merged
    :param str into: name of the module to merge into
    :param bool update_dependers: whether the dependencies of modules that depend on `old`
                                  are updated
    """
    cr.execute("SELECT name, id FROM ir_module_module WHERE name IN %s", [(old, into)])
    mod_ids = dict(cr.fetchall())

    if old not in mod_ids:
        # this can happen in case of temp modules added after a release if the database does not
        # know about this module, i.e: account_full_reconcile in 9.0
        # `into` should be know. Let it crash if not
        _logger.log(NEARLYWARN, "Unknow module %s. Skip merge into %s.", old, into)
        return

    def _up(table, old, new):
        cr.execute(
            """
                UPDATE ir_model_{0} x
                   SET module=%s
                 WHERE module=%s
                   AND NOT EXISTS(SELECT 1
                                    FROM ir_model_{0} y
                                   WHERE y.name = x.name
                                     AND y.module = %s)
        """.format(table),
            [new, old, new],
        )

        if table == "data":
            # remove xmlids pointing to the same records
            cr.execute(
                """
                    DELETE
                      FROM ir_model_data o
                     USING ir_model_data n
                     WHERE o.module = %s
                       AND n.module = %s
                       AND o.model = n.model
                       AND o.res_id = n.res_id
                """,
                [old, new],
            )
            # merge records when they have the same model
            cr.execute(
                """
                    SELECT o.model,
                           jsonb_object_agg(o.res_id, n.res_id)
                      FROM ir_model_data o
                      JOIN ir_model_data n
                        ON n.model = o.model
                       AND n.name = o.name
                     WHERE o.module = %s
                       AND n.module = %s
                       AND o.model NOT LIKE 'ir.model%%'
                       AND o.model NOT LIKE 'ir.module.module%%'
                       AND o.model NOT IN ('ir.ui.view', 'ir.ui.menu')
                  GROUP BY o.model
                """,
                [old, new],
            )
            for model, mapping in cr.fetchall():
                replace_record_references_batch(
                    cr,
                    {int(f): int(t) for f, t in mapping.items()},  # jsonb keys are always string
                    model,
                    replace_xmlid=False,
                )

            # remove remaining records
            cr.execute(
                """
                SELECT model, array_agg(res_id)
                  FROM ir_model_data
                 WHERE module=%s
                   AND model NOT LIKE 'ir.model%%'
                   AND model NOT LIKE 'ir.module.module%%'
              GROUP BY model
            """,
                [old],
            )
            for model, res_ids in cr.fetchall():
                if model == "ir.ui.view":
                    for v in res_ids:
                        remove_view(cr, view_id=v, silent=True)
                elif model == "ir.ui.menu":
                    remove_menus(cr, tuple(res_ids))
                else:
                    remove_records(cr, model, res_ids)

        cr.execute("DELETE FROM ir_model_{0} WHERE module=%s".format(table), [old])

    _up("constraint", mod_ids[old], mod_ids[into])
    _up("relation", mod_ids[old], mod_ids[into])
    _update_view_key(cr, old, into)
    _up("data", old, into)
    if table_exists(cr, "ir_translation"):
        cr.execute("UPDATE ir_translation SET module=%s WHERE module=%s", [into, old])

    # update dependencies of modules that depends on $old
    if update_dependers:
        cr.execute(
            """
            UPDATE ir_module_module_dependency d
               SET name = %s
              FROM ir_module_module m
             WHERE m.id = d.module_id
               AND d.name = %s
               AND m.name != %s
               AND NOT EXISTS(SELECT 1
                                FROM ir_module_module_dependency o
                               WHERE o.module_id = d.module_id
                                 AND o.name = %s)
            """,
            [into, old, into, into],
        )

    cr.execute("DELETE FROM ir_module_module WHERE name=%s RETURNING state", [old])
    [state] = cr.fetchone()
    cr.execute("DELETE FROM ir_module_module_dependency WHERE name=%s", [old])
    cr.execute("DELETE FROM ir_model_data WHERE model='ir.module.module' AND res_id=%s", [mod_ids[old]])
    if state in INSTALLED_MODULE_STATES:
        force_install_module(cr, into, reason="installed {!r} module has been merged into it".format(old))


def force_install_module(cr, module, if_installed=None, reason="it has been explicitly asked for"):
    """
    Force the ORM to install a module.

    :param str module: name of the module to install
    :param list(str) or None if_installed: only force the install when these modules are
                                           already installed
    :return str: the *new* state of the module
    """
    subquery = ""
    subparams = ()
    if if_installed:
        subquery = """AND EXISTS(SELECT 1 FROM ir_module_module
                                  WHERE name IN %s
                                    AND state IN %s)"""
        subparams = (tuple(if_installed), INSTALLED_MODULE_STATES)

    cr.execute(
        """
        WITH RECURSIVE deps (mod_id, dep_name) AS (
              SELECT m.id, d.name from ir_module_module_dependency d
              JOIN ir_module_module m on (d.module_id = m.id)
              WHERE m.name = %s
            UNION
              SELECT m.id, d.name from ir_module_module m
              JOIN deps ON deps.dep_name = m.name
              JOIN ir_module_module_dependency d on (d.module_id = m.id)
        )
        UPDATE ir_module_module m
           SET state = CASE WHEN state = 'to remove' THEN 'to upgrade'
                            WHEN state = 'uninstalled' THEN 'to install'
                            ELSE state
                       END,
               demo=(select demo from ir_module_module where name='base')
          FROM deps d
         WHERE m.id = d.mod_id
           {0}
     RETURNING m.name, m.state
    """.format(subquery),
        (module,) + subparams,
    )

    states = dict(cr.fetchall())
    toinstall = [m for m in states if states[m] == "to install"]

    if module in toinstall:
        _logger.info(
            "force install of module %r (and its dependencies) because %s%s",
            module,
            reason,
            " and modules {!r} are already installed".format(if_installed) if if_installed else "",
        )

    # auto_install modules...
    if AUTO_INSTALL in ("all", "only_link_modules") and toinstall:
        # Same algo as ir.module.module.button_install(): https://git.io/fhCKd
        dep_match = ""
        if column_exists(cr, "ir_module_module_dependency", "auto_install_required"):
            dep_match = "AND d.auto_install_required = TRUE AND e.auto_install_required = TRUE"

        country_match = country_join = ""
        if table_exists(cr, "module_country"):
            country_join = """
                 LEFT JOIN module_country mc
                        ON mc.module_id = on_me.id
                 LEFT JOIN (
                        SELECT c.id, p.country_id
                          FROM res_company c
                          JOIN res_partner p
                            ON p.id = c.partner_id
                         WHERE c.active
                      ) AS c
                        ON c.country_id = mc.country_id
            """
            country_match = """
               AND (
                    count(mc) = 0  -- no country set
                 OR count(c) > 0 -- or have active companies with a matching country
               )
            """

        cat_match = ""
        if AUTO_INSTALL == "only_link_modules":
            # even if we skip auto installs, we still need to auto install the real link-modules.
            # those are in the "Hidden" category
            hidden = ref(cr, "base.module_category_hidden")
            cat_match = cr.mogrify("AND on_me.category_id = %s", [hidden]).decode()

        cr.execute(
            """
            SELECT on_me.name
              FROM ir_module_module_dependency d
              JOIN ir_module_module on_me ON on_me.id = d.module_id
              JOIN ir_module_module_dependency e ON e.module_id = on_me.id
              JOIN ir_module_module its_deps ON its_deps.name = e.name
                {}
             WHERE d.name = ANY(%s)
               AND on_me.state = 'uninstalled'
               AND on_me.auto_install = TRUE
               {}
               {}
          GROUP BY on_me.name
            HAVING
                   -- are all dependencies (to be) installed?
                   array_agg(its_deps.state)::text[] <@ %s
                {}

        """.format(country_join, dep_match, cat_match, country_match),
            [toinstall, list(INSTALLED_MODULE_STATES)],
        )
        for (mod,) in cr.fetchall():
            force_install_module(
                cr,
                mod,
                reason="it is an auto install module and its dependency {!r} has been force installed".format(module),
            )

    # TODO handle module exclusions

    return states.get(module)


def _assert_modules_exists(cr, *modules):
    assert modules
    cr.execute("SELECT name FROM ir_module_module WHERE name IN %s", [modules])
    existing_modules = {m[0] for m in cr.fetchall()}
    unexisting_modules = set(modules) - existing_modules
    if unexisting_modules:
        raise AssertionError("Unexisting modules: {}".format(", ".join(unexisting_modules)))


def new_module_dep(cr, module, new_dep):
    assert isinstance(new_dep, basestring)
    _assert_modules_exists(cr, module, new_dep)
    # One new dep at a time
    cr.execute(
        """
            INSERT INTO ir_module_module_dependency(name, module_id)
                       SELECT %s, id
                         FROM ir_module_module m
                        WHERE name=%s
                          AND NOT EXISTS(SELECT 1
                                           FROM ir_module_module_dependency
                                          WHERE module_id = m.id
                                            AND name=%s)
    """,
        [new_dep, module, new_dep],
    )

    # Update new_dep state depending on module state
    cr.execute("SELECT state FROM ir_module_module WHERE name = %s", [module])
    mod_state = (cr.fetchone() or ["n/a"])[0]
    if mod_state in INSTALLED_MODULE_STATES:
        # Module was installed, need to install all its deps, recursively,
        # to make sure the new dep is installed
        force_install_module(cr, new_dep, reason="it's a new dependency of {!r}".format(module))


def remove_module_deps(cr, module, old_deps):
    assert isinstance(old_deps, (Sequence, Set)) and not isinstance(old_deps, basestring)
    # As the goal is to have dependencies removed, the objective is reached even when they don't exist.
    # Therefore, we don't need to assert their existence (at the cost of missing typos).
    cr.execute(
        """
            DELETE
              FROM ir_module_module_dependency
             WHERE module_id = (SELECT id
                                  FROM ir_module_module
                                 WHERE name = %s)
               AND name IN %s
    """,
        [module, tuple(old_deps)],
    )


def module_deps_diff(cr, module, plus=(), minus=()):
    for new_dep in plus:
        new_module_dep(cr, module, new_dep)
    if minus:
        remove_module_deps(cr, module, tuple(minus))


def module_auto_install(cr, module, auto_install):
    if column_exists(cr, "ir_module_module_dependency", "auto_install_required"):
        params = []
        if auto_install is True:
            value = "TRUE"
        elif auto_install:
            value = "(name = ANY(%s))"
            params = [list(auto_install)]
        else:
            value = "FALSE"

        cr.execute(
            """
            UPDATE ir_module_module_dependency
               SET auto_install_required = {}
             WHERE module_id = (SELECT id
                                  FROM ir_module_module
                                 WHERE name = %s)
        """.format(value),
            params + [module],
        )

    cr.execute("UPDATE ir_module_module SET auto_install = %s WHERE name = %s", [auto_install is not False, module])


def trigger_auto_install(cr, module):
    _assert_modules_exists(cr, module)
    if AUTO_INSTALL == "none":
        return False

    dep_match = "true"
    if column_exists(cr, "ir_module_module_dependency", "auto_install_required"):
        dep_match = "d.auto_install_required = true"

    country_match = country_join = ""
    if table_exists(cr, "module_country"):
        country_join = """
             LEFT JOIN module_country mc
                    ON mc.module_id = m.id
             LEFT JOIN (
                    SELECT c.id, p.country_id
                      FROM res_company c
                      JOIN res_partner p
                        ON p.id = c.partner_id
                     WHERE c.active
                  ) AS c
                    ON c.country_id = mc.country_id
        """
        country_match = """
           AND (
                count(mc) = 0  -- no country set
             OR count(c) > 0 -- or have active companies with a matching country
           )
        """

    cat_match = "true"
    if AUTO_INSTALL == "only_link_modules":
        # even if we skip auto installs, we still need to auto install the real link-modules.
        # those are in the "Hidden" category
        hidden = ref(cr, "base.module_category_hidden")
        cat_match = cr.mogrify("m.category_id = %s", [hidden]).decode()

    query = """
            SELECT m.id
              FROM ir_module_module_dependency d
              JOIN ir_module_module m ON m.id = d.module_id
              JOIN ir_module_module md ON md.name = d.name
                {}
             WHERE m.name = %s
               AND m.state = 'uninstalled'
               AND m.auto_install = true
               AND {}
               AND {}
          GROUP BY m.id
            HAVING bool_and(md.state IN %s)
                {}
    """.format(country_join, dep_match, cat_match, country_match)

    cr.execute(query, [module, INSTALLED_MODULE_STATES])
    if cr.rowcount:
        force_install_module(cr, module, reason="it's an auto install module and all its dependencies are installed")
        return True
    return False


def _set_module_category(cr, module, category):
    cid = create_categories(cr, category.split("/"))
    cr.execute("UPDATE ir_module_module SET category_id=%s WHERE name=%s", [cid, module])


def _set_module_countries(cr, module, countries):
    if not table_exists(cr, "module_country"):
        return
    clean_query = """
        DELETE
          FROM module_country mc
         USING ir_module_module m
         WHERE m.id = mc.module_id
           AND m.name = %s
    """
    cr.execute(clean_query, [module])

    if not countries:
        return

    insert_query = """
        INSERT INTO module_country(module_id, country_id)
             SELECT m.id, c.id
               FROM ir_module_module m,
                    res_country c
              WHERE m.name = %s
                AND c.code IN %s
    """
    cr.execute(insert_query, [module, tuple(c.upper() for c in countries)])


def new_module(cr, module, deps=(), auto_install=False, category=None, countries=()):
    if deps:
        _assert_modules_exists(cr, *deps)

    cr.execute("SELECT id FROM ir_module_module WHERE name = %s", [module])
    if cr.rowcount:
        # Avoid duplicate entries for module which is already installed,
        # even before it has become standard module in new version
        # Also happen for modules added afterward, which should be added by multiple series.
        # But we should force the dependencies
        mod_id = cr.fetchone()[0]

        # In CI, it should not happen. Log it as critical
        level = logging.CRITICAL if on_CI() else logging.WARNING

        _logger.log(level, "New module %r already defined. Resetting dependencies.", module)
        cr.execute("DELETE FROM ir_module_module_dependency WHERE module_id = %s", [mod_id])

    else:
        state = "uninstalled"
        cr.execute(
            """
            INSERT INTO ir_module_module (name, state, demo)
                 VALUES (%s, %s, (SELECT demo FROM ir_module_module WHERE name='base'))
              RETURNING id
            """,
            [module, state],
        )
        (new_id,) = cr.fetchone()

        cr.execute(
            """
            INSERT INTO ir_model_data (name, module, noupdate, model, res_id)
                 VALUES ('module_'||%s, 'base', 't', 'ir.module.module', %s)
            """,
            [module, new_id],
        )

    for dep in deps:
        new_module_dep(cr, module, dep)

    if category is not None:
        _set_module_category(cr, module, category)
    _set_module_countries(cr, module, countries)

    module_auto_install(cr, module, auto_install)
    trigger_auto_install(cr, module)


def _caller_version(depth=2):
    frame = currentframe()
    version = "util"
    while version == "util":
        filename, _ = frame_codeinfo(frame, depth)
        version = ".".join(filename.split(os.path.sep)[-2].split(".")[:2])
        depth += 1

    return version


def force_upgrade_of_fresh_module(cr, module, init=True):
    """
    Force the execution of upgrade scripts for a module that is being installed.

    Standard Odoo doesn't run upgrade scripts when a module is being installed. This makes
    sense because, technically speaking, the module is not being upgraded. Still, it
    happens that a (new) module needs to perform some operations for it to be correctly
    installed, like grabbing data from another module. This is common when a module is
    functionally *split* into several ones.

    :param str module: name of the module for which to force the execution of upgrade
                       scripts
    :param bool init: whether to set the module to be in *init* mode

    Being in init mode has the side effect of not respecting noupdate flags, in XML file
    nor in `ir_model_data`.
    """
    version = _caller_version()
    if version_gte("saas~14.5"):
        # We must delay until the modules actually exists. They are added by the auto discovery process.
        ENVIRON["__modules_auto_discovery_force_upgrades"][module] = (init, version)
        return None

    return _force_upgrade_of_fresh_module(cr, module, init, version)


def _force_upgrade_of_fresh_module(cr, module, init, version):
    # Low level implementation
    # Force module state to be in `to upgrade`.
    # Needed for migration script execution. See http://git.io/vnF7f
    cr.execute(
        """
            UPDATE ir_module_module
               SET state='to upgrade',
                   latest_version=%s
             WHERE name=%s
               AND state='to install'
         RETURNING id
    """,
        [version, module],
    )
    if init and cr.rowcount:
        # Force module in `init` mode beside its state is forced to `to upgrade`
        # See http://git.io/vnF7O
        odoo.tools.config["init"][module] = "oh yeah!"


# for compatibility
force_migration_of_fresh_module = force_upgrade_of_fresh_module
"""
:meta private: exclude from online docs
"""


def _trigger_auto_discovery(cr):
    # low level implementation.
    # Called by `base/0.0.0/post-modules-auto-discovery.py` script.
    # Use accumulated values for the auto_install and force_upgrade modules.

    force_installs = ENVIRON["__modules_auto_discovery_force_installs"]

    cr.execute(
        """
            SELECT m.name, array_agg(d.name)
              FROM ir_module_module m
         LEFT JOIN ir_module_module_dependency d ON d.module_id = m.id
          GROUP BY m.name
        """
    )
    existing = dict(cr.fetchall())

    graph = {}
    for module in odoo.modules.get_modules():
        manifest = get_manifest(module)
        graph[module] = (
            set(manifest["depends"]),
            manifest["auto_install"],
            manifest["category"],
            manifest.get("countries"),
        )

    for module in topological_sort({k: v[0] for k, v in graph.items()}):
        deps, auto_install, category, countries = graph[module]
        if module not in existing:
            new_module(cr, module, deps=deps, auto_install=auto_install, category=category, countries=countries)
        else:
            current_deps = set(existing[module])
            plus = deps - current_deps
            minus = current_deps - deps
            if plus or minus:
                module_deps_diff(cr, module, plus=plus, minus=minus)
            _set_module_category(cr, module, category)
            _set_module_countries(cr, module, countries)
            module_auto_install(cr, module, auto_install)

        if module in force_installs:
            force_install_module(cr, module)

    for module, (init, version) in ENVIRON["__modules_auto_discovery_force_upgrades"].items():
        _force_upgrade_of_fresh_module(cr, module, init, version)


def modules_auto_discovery(cr, force_installs=None, force_upgrades=None):
    # Cursor, Optional[Set[str]], Optional[Set[str]] -> None

    # Register the modules to force install and force upgrade
    # The actual auto discovery is delayed in `base/0.0.0/post-modules-auto-discovery.py`

    if force_installs:
        ENVIRON["__modules_auto_discovery_force_installs"].update(force_installs)
    if force_upgrades:
        version = _caller_version()
        ENVIRON["__modules_auto_discovery_force_upgrades"].update(dict.fromkeys(force_upgrades, (False, version)))


def move_model(cr, model, from_module, to_module, move_data=False, keep=()):
    """
    Move a model from one module to another.

    :param str model: name of the model to move
    :param str from_module: name of the module where the model is originally defined
    :param str to_module: name of the destination module where the model is to be moved
    :param bool move_data: whether to also update `ir_model_data` for records of the model
    :param list(str) keep: list of XML ids to keep - not move

    This function can be called for moving overrides of a model to another module. As it
    cannot distinguish between source model or inherited model, it raises an exception if
    the destination module isn't installed.
    """
    _validate_model(model)
    if not module_installed(cr, to_module):
        raise MigrationError("Cannot move model {!r} to module {!r} as it isn't installed".format(model, to_module))

    if any("." in k for k in keep):
        raise SleepyDeveloperError("The `keep` argument must not contain fully-qualified xmlids, only the local names")

    def update_imd(data_model, path):
        table = table_of_model(cr, data_model)
        if not table_exists(cr, table):
            return

        joins = []
        where = "d.name != ALL(%(keep)s)"
        if path:
            path = path.split(".")
            column = path.pop()

            working_table = table
            for index, milestone in enumerate(path, 1):
                working_table, working_column, _ = target_of(cr, working_table, milestone)

                joins.append(
                    "JOIN {t} t{i} ON t{i}.{c} = t{j}.{m}".format(
                        t=working_table,
                        i=index,
                        c=working_column,
                        j=index - 1,
                        m=milestone,
                    )
                )

            where += " AND t{}.{} = %(linked_model)s".format(len(path), column)

        joins = "\n".join(joins)
        params = {
            "linked_model": model,
            "from_module": from_module,
            "to_module": to_module,
            "data_model": data_model,
            "keep": list(keep),
        }

        query = """
            WITH dups AS (
                SELECT d.id
                  FROM ir_model_data d
                  JOIN ir_model_data o
                    ON d.name = o.name
                  JOIN {table} t0
                    ON t0.id = d.res_id
               {joins}
                 WHERE d.model = %(data_model)s
                   AND d.module = %(from_module)s
                   AND o.module = %(to_module)s
                   AND {where}
            )
            DELETE FROM ir_model_data d
                  USING dups
                  WHERE dups.id = d.id
        """
        cr.execute(query.format(table=table, joins=joins, where=where), params)

        query = """
            UPDATE ir_model_data d
               SET module = %(to_module)s
              FROM {table} t0
           {joins}
             WHERE t0.id = d.res_id
               AND d.model = %(data_model)s
               AND d.module = %(from_module)s
               AND {where}
        """
        cr.execute(query.format(table=table, joins=joins, where=where), params)

    update_imd("ir.model", path="model")
    update_imd("ir.model.fields", path="model_id.model")
    update_imd("ir.model.fields.selection", path="field_id.model_id.model")
    update_imd("ir.model.constraint", path="model.model")
    update_imd("ir.model.relation", path="model.model")
    update_imd("ir.rule", path="model_id.model")
    update_imd("ir.model.access", path="model_id.model")
    update_imd("ir.ui.view", path="model")
    update_imd("ir.actions.act_window", path="res_model")
    update_imd("ir.actions.server", path="model_id.model")
    update_imd("ir.actions.report", path="model")
    update_imd("email.template", path="model")  # OpenERP <= 8.0
    update_imd("mail.template", path="model")
    if column_exists(cr, "ir_cron", "model"):
        # < 10.saas~14
        update_imd("ir.cron", path="model")
    else:
        update_imd("ir.cron", path="ir_actions_server_id.model_id.model")

    if move_data:
        update_imd(model, path=None)
