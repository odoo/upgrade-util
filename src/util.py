# Utility functions for migration scripts

import imp
import logging
import lxml
import os
import sys
import time

from contextlib import contextmanager
from docutils.core import publish_string
from inspect import currentframe
from itertools import chain, takewhile, islice, count
from operator import itemgetter
from textwrap import dedent

import markdown

import openerp
from openerp import release, SUPERUSER_ID
from openerp.addons.base.module.module import MyWriter
from openerp.modules.module import get_module_path
from openerp.modules.registry import RegistryManager
from openerp.sql_db import db_connect
from openerp.tools.func import frame_codeinfo
from openerp.tools.mail import html_sanitize
from openerp.tools import UnquoteEvalContext

try:
    from openerp.api import Environment
    manage_env = Environment.manage
except ImportError:
    @contextmanager
    def manage_env():
        yield

_logger = logging.getLogger(__name__)

_INSTALLED_MODULE_STATES = ('installed', 'to install', 'to upgrade')

DROP_DEPRECATED_CUSTOM = os.getenv('OE_DROP_DEPRECATED_CUSTOM')

class MigrationError(Exception):
    pass


def main(func, version=None):
    """a main() function for scripts"""
    if len(sys.argv) != 2:
        sys.exit("Usage: %s <dbname>" % (sys.argv[0],))
    dbname = sys.argv[1]
    with db_connect(dbname).cursor() as cr, manage_env():
        func(cr, version)

def splitlines(s):
    """ yield stripped lines of `s`.
        Skip empty lines
        Remove comments (starts with `#`).
    """
    return filter(None, map(lambda x: x.split('#', 1)[0].strip(), s.splitlines()))

def import_script(path):
    name = os.path.basename(path)
    full_path = os.path.join(os.path.dirname(__file__), path)
    with open(full_path) as fp:
        return imp.load_source(name, full_path, fp)

@contextmanager
def skippable_cm():
    """Allow a contextmanager to not yield.
    """
    if not hasattr(skippable_cm, '_msg'):
        @contextmanager
        def _():
            if 0:
                yield
        try:
            with _():
                pass
        except RuntimeError, r:
            skippable_cm._msg = str(r)
    try:
        yield
    except RuntimeError, r:
        if str(r) != skippable_cm._msg:
            raise

@contextmanager
def savepoint(cr):
    name = hex(int(time.time() * 1000))[1:]
    cr.execute("SAVEPOINT %s" % (name,))
    try:
        yield
        cr.execute('RELEASE SAVEPOINT %s' % (name,))
    except Exception:
        cr.execute('ROLLBACK TO SAVEPOINT %s' % (name,))
        raise

def pg_array_uniq(a, drop_null=False):
    dn = "WHERE x IS NOT NULL" if drop_null else ""
    return "ARRAY(SELECT x FROM unnest({0}) x {1} GROUP BY x)".format(a, dn)

def has_enterprise():
    """Return whernever the current installation has enterprise addons availables"""
    # NOTE should always return True as customers need Enterprise to migrate or
    #      they are on SaaS, which include enterpise addons.
    #      This act as a sanity check for developpers or in case we release the scripts.
    if os.getenv('ODOO_HAS_ENTERPRISE'):
        return True
    # XXX maybe we will need to change this for version > 9
    return bool(get_module_path('delivery_fedex', downloaded=False, display_warning=False))

def dispatch_by_dbuuid(cr, version, callbacks):
    cr.execute("""
        SELECT value
          FROM ir_config_parameter
         WHERE key IN ('database.uuid', 'origin.database.uuid')
      ORDER BY key DESC
         LIMIT 1
    """)
    [uuid] = cr.fetchone()
    callbacks.get(uuid, lambda *a: None)(cr, version)

def table_of_model(cr, model):
    return {
        'ir.actions.actions':          'ir_actions',
        'ir.actions.act_url':          'ir_act_url',
        'ir.actions.act_window':       'ir_act_window',
        'ir.actions.act_window_close': 'ir_actions',
        'ir.actions.act_window.view':  'ir_act_window_view',
        'ir.actions.client':           'ir_act_client',
        'ir.actions.report.xml':       'ir_act_report_xml',
        'ir.actions.server':           'ir_act_server',
        'ir.actions.wizard':           'ir_act_wizard',

        'stock.picking.in':  'stock_picking',
        'stock.picking.out': 'stock_picking',

        'workflow':            'wkf',
        'workflow.activity':   'wkf_activity',
        'workflow.instance':   'wkf_instance',
        'workflow.transition': 'wkf_transition',
        'workflow.triggers':   'wkf_triggers',
        'workflow.workitem':   'wkf_workitem',
    }.get(model, model.replace('.', '_'))

def env(cr):
    try:
        from openerp.api import Environment
    except ImportError:
        v = release.major_version
        raise MigrationError('Hold on! There is not yet `Environment` in %s' % v)
    return Environment(cr, SUPERUSER_ID, {})

def remove_view(cr, xml_id=None, view_id=None, deactivate_custom=DROP_DEPRECATED_CUSTOM, silent=False):
    """
    Recursively delete the given view and its inherited views, as long as they
    are part of a module. Will crash as soon as a custom view exists anywhere
    in the hierarchy.
    """
    assert bool(xml_id) ^ bool(view_id)
    if xml_id:
        view_id = ref(cr, xml_id)
        if not view_id:
            return

        module, _, name = xml_id.partition('.')
        cr.execute("SELECT model FROM ir_model_data WHERE module=%s AND name=%s",
                   [module, name])

        [model] = cr.fetchone()
        if model != 'ir.ui.view':
            raise ValueError("%r should point to a 'ir.ui.view', not a %r" % (xml_id, model))
    elif not silent or deactivate_custom:
        # search matching xmlid for logging or renaming of custom views
        cr.execute("SELECT module, name FROM ir_model_data WHERE model='ir.ui.view' AND res_id=%s",
                   [view_id])
        if cr.rowcount:
            xml_id = "%s.%s" % cr.fetchone()
        else:
            xml_id = '?'

    cr.execute("""
        SELECT v.id, x.module || '.' || x.name
        FROM ir_ui_view v LEFT JOIN
           ir_model_data x ON (v.id = x.res_id AND x.model = 'ir.ui.view' AND x.module !~ '^_')
        WHERE v.inherit_id = %s;
    """, [view_id])
    for child_id, child_xml_id in cr.fetchall():
        if child_xml_id:
            if not silent:
                _logger.info('Dropping deprecated built-in view %s (ID %s), '
                             'as parent %s (ID %s) is going to be removed',
                             child_xml_id, child_id, xml_id, view_id)
            remove_view(cr, child_xml_id, deactivate_custom=deactivate_custom,
                        silent=True)
        else:
            if deactivate_custom:
                if not silent:
                    _logger.warning('Deactivating deprecated custom view with ID %s, '
                                    'as parent %s (ID %s) was removed',
                                    child_id, xml_id, view_id)
                disable_view_query = """
                    UPDATE ir_ui_view
                    SET name = (name || ' - old view, inherited from ' || %%s),
                        model = (model || '.disabled'),
                        inherit_id = NULL
                        %s
                        WHERE id = %%s
                """
                # In 8.0, disabling requires setting mode to 'primary'
                extra_set_sql = ''
                if column_exists(cr, 'ir_ui_view', 'mode'):
                    extra_set_sql = ",  mode = 'primary'  "

                disable_view_query = disable_view_query % extra_set_sql
                cr.execute(disable_view_query, (xml_id, child_id))
            else:
                raise MigrationError('Deprecated custom view with ID %s needs migration, '
                                     'as parent %s (ID %s) is going to be removed' %
                                     (child_id, xml_id, view_id))
    if not silent:
        _logger.info('Dropping deprecated built-in view %s (ID %s).',
                     xml_id, view_id)
    remove_record(cr, ('ir.ui.view', view_id))


@contextmanager
def edit_view(cr, xmlid=None, view_id=None, skip_if_not_noupdate=True):
    """Contextmanager that may yield etree arch of a view.
    As it may not yield, you must use `skippable_cm`

        with util.skippable_cm(), util.edit_view(cr, 'xml.id') as arch:
            arch.attrib['string'] = 'My Form'
    """
    assert bool(xmlid) ^ bool(view_id), "You Must specify either xmlid or view_id"
    noupdate = True
    if xmlid:
        if '.' not in xmlid:
            raise ValueError('Please use fully qualified name <module>.<name>')

        module, _, name = xmlid.partition('.')
        cr.execute("""SELECT res_id, noupdate
                        FROM ir_model_data
                       WHERE module = %s
                         AND name = %s
                   """, (module, name))
        data = cr.fetchone()
        if data:
            view_id, noupdate = data

    if view_id and not (skip_if_not_noupdate and not noupdate):
        arch_col = 'arch_db' if column_exists(cr, 'ir_ui_view', 'arch_db') else 'arch'
        cr.execute("""SELECT {arch}
                        FROM ir_ui_view
                       WHERE id=%s
                   """.format(arch=arch_col), [view_id])
        [arch] = cr.fetchone() or [None]
        if arch:
            arch = lxml.etree.fromstring(arch)
            yield arch
            cr.execute("UPDATE ir_ui_view SET {arch}=%s WHERE id=%s".format(arch=arch_col),
                       [lxml.etree.tostring(arch), view_id])


def remove_record(cr, name, deactivate=False, active_field='active'):
    if isinstance(name, basestring):
        if '.' not in name:
            raise ValueError('Please use fully qualified name <module>.<name>')
        module, _, name = name.partition('.')
        cr.execute("""DELETE FROM ir_model_data
                            WHERE module = %s
                              AND name = %s
                        RETURNING model, res_id
                   """, (module, name))
        data = cr.fetchone()
        if not data:
            return
        model, res_id = data
    elif isinstance(name, tuple):
        if len(name) != 2:
            raise ValueError('Please use a 2-tuple (<model>, <res_id>)')
        model, res_id = name
    else:
        raise ValueError('Either use a fully qualified xmlid string ' +
                         '<module>.<name> or a 2-tuple (<model>, <res_id>)')

    table = table_of_model(cr, model)
    try:
        with savepoint(cr):
            cr.execute('DELETE FROM "%s" WHERE id=%%s' % table, (res_id,))
    except Exception:
        if not deactivate or not active_field:
            raise
        cr.execute('UPDATE "%s" SET "%s"=%%s WHERE id=%%s' % (table, active_field), (False, res_id))
    else:
        # TODO delete attachments & workflow instances
        pass

def rename_xmlid(cr, old, new):
    if '.' not in old or '.' not in new:
        raise ValueError('Please use fully qualified name <module>.<name>')

    old_module, _, old_name = old.partition('.')
    new_module, _, new_name = new.partition('.')
    cr.execute("""UPDATE ir_model_data
                     SET module=%s, name=%s
                   WHERE module=%s AND name=%s
               """, (new_module, new_name, old_module, old_name))

def ref(cr, xmlid):
    if '.' not in xmlid:
        raise ValueError('Please use fully qualified name <module>.<name>')

    module, _, name = xmlid.partition('.')
    cr.execute("""SELECT res_id
                    FROM ir_model_data
                   WHERE module = %s
                     AND name = %s
                """, (module, name))
    data = cr.fetchone()
    if data:
        return data[0]
    return None


def force_noupdate(cr, xmlid, noupdate=True, warn=False):
    if '.' not in xmlid:
        raise ValueError('Please use fully qualified name <module>.<name>')

    module, _, name = xmlid.partition('.')
    cr.execute("""UPDATE ir_model_data
                     SET noupdate = %s
                   WHERE module = %s
                     AND name = %s
                     AND noupdate != %s
                """, (noupdate, module, name, noupdate))
    if noupdate is False and cr.rowcount and warn:
        _logger.warning('Customizations on `%s` might be lost!', xmlid)
    return cr.rowcount


def ensure_xmlid_match_record(cr, xmlid, model, values):
    if '.' not in xmlid:
        raise ValueError('Please use fully qualified name <module>.<name>')

    module, _, name = xmlid.partition('.')
    cr.execute("""SELECT id, res_id
                    FROM ir_model_data
                   WHERE module = %s
                     AND name = %s
                """, (module, name))

    table = table_of_model(cr, model)
    data = cr.fetchone()
    if data:
        data_id, res_id = data
        # check that record still exists
        cr.execute("SELECT id FROM %s WHERE id=%%s" % table, (res_id,))
        if cr.fetchone():
            return res_id
    else:
        data_id = None

    # search for existing record marching values
    where = []
    data = ()
    for k, v in values.items():
        if v:
            where += ['%s = %%s' % (k,)]
            data += (v,)
        else:
            where += ['%s IS NULL' % (k,)]
            data += ()

    query = ("SELECT id FROM %s WHERE " % table) + ' AND '.join(where)
    cr.execute(query, data)
    record = cr.fetchone()
    if not record:
        return None

    res_id = record[0]

    if data_id:
        cr.execute("""UPDATE ir_model_data
                         SET res_id=%s
                       WHERE id=%s
                   """, (res_id, data_id))
    else:
        cr.execute("""INSERT INTO ir_model_data
                                  (module, name, model, res_id, noupdate)
                           VALUES (%s, %s, %s, %s, %s)
                   """, (module, name, model, res_id, True))

    return res_id

def fix_wrong_m2o(cr, table, column, target, value=None):
    cr.execute("""
        WITH wrongs_m2o AS (
            SELECT s.id
              FROM {table} s
         LEFT JOIN {target} t
                ON s.{column} = t.id
             WHERE s.{column} IS NOT NULL
               AND t.id IS NULL
        )
        UPDATE {table} s
           SET {column}=%s
          FROM wrongs_m2o w
         WHERE s.id = w.id
    """.format(table=table, column=column, target=target), [value])

def ensure_m2o_func_field_data(cr, src_table, column, dst_table):
    """
        Fix broken m2o relations.
        If any `column` not present in `dst_table`, remove column from `src_table` in
        order to force recomputation of the function field

        WARN: only call this method on m2o function/related fields!!
    """
    if not column_exists(cr, src_table, column):
        return
    cr.execute("""SELECT count(1)
                    FROM "{src_table}"
                   WHERE "{column}" NOT IN (SELECT id FROM "{dst_table}")
               """.format(src_table=src_table, column=column, dst_table=dst_table))
    if cr.fetchone()[0]:
        remove_column(cr, src_table, column, cascade=True)

def create_m2m(cr, m2m, fk1, fk2, col1=None, col2=None):
    if col1 is None:
        col1 = '%s_id' % fk1
    if col2 is None:
        col2 = '%s_id' % fk2

    cr.execute("""
        CREATE TABLE {m2m}(
            {col1} integer NOT NULL REFERENCES {fk1}(id) ON DELETE CASCADE,
            {col2} integer NOT NULL REFERENCES {fk2}(id) ON DELETE CASCADE,
            UNIQUE ({col1}, {col2})
        );
        CREATE INDEX ON {m2m}({col1});
        CREATE INDEX ON {m2m}({col2});
    """.format(**locals()))

def module_installed(cr, module):
    """return True if `module` is (about to be) installed"""
    cr.execute("""SELECT 1
                    FROM ir_module_module
                   WHERE name=%s
                     AND state IN %s
               """, [module, _INSTALLED_MODULE_STATES])
    return bool(cr.rowcount)

def remove_module(cr, module):
    """ Uninstall the module and delete references to it
       Ensure to reassign records before calling this method
    """
    # NOTE: we cannot use the uninstall of module because the given
    # module need to be currenctly installed and running as deletions
    # are made using orm.

    cr.execute("SELECT id FROM ir_module_module WHERE name=%s", (module,))
    mod_id, = cr.fetchone() or [None]
    if not mod_id:
        return

    # delete constraints only owned by this module
    cr.execute("""SELECT name
                    FROM ir_model_constraint
                GROUP BY name
                  HAVING array_agg(module) = %s""", ([mod_id],))

    constraints = tuple(map(itemgetter(0), cr.fetchall()))
    if constraints:
        cr.execute("""SELECT table_name, constraint_name
                        FROM information_schema.table_constraints
                       WHERE constraint_name IN %s""", (constraints,))
        for table, constraint in cr.fetchall():
            cr.execute('ALTER TABLE "%s" DROP CONSTRAINT "%s"' % (table, constraint))

    cr.execute("""DELETE FROM ir_model_constraint
                        WHERE module=%s
               """, (mod_id,))

    # When deleting a non-leaf module, installed modules that used to depend on it may
    # have menus whose parent will be deleted. Unfortunately, there is a constraint
    # that `RESTRICT` us to do so. Recreate it to `SET NULL` on parent.
    # The culprit menu should be corrected when corresponding module will be updated.
    cr.execute("""SELECT 1
                    FROM pg_constraint
                   WHERE conname='ir_ui_menu_parent_id_fkey'
                     AND confdeltype='r'
               """)
    if cr.rowcount:
        cr.execute("""
            ALTER TABLE ir_ui_menu DROP CONSTRAINT ir_ui_menu_parent_id_fkey;
            ALTER TABLE ir_ui_menu ADD CONSTRAINT ir_ui_menu_parent_id_fkey
                FOREIGN KEY(parent_id) REFERENCES ir_ui_menu ON DELETE SET NULL;
        """)

    # delete data
    model_ids, field_ids, view_ids = (), (), ()
    cr.execute("""SELECT model, array_agg(res_id)
                    FROM ir_model_data d
                   WHERE NOT EXISTS (SELECT 1
                                       FROM ir_model_data
                                      WHERE id != d.id
                                        AND res_id = d.res_id
                                        AND model = d.model
                                        AND module != d.module)
                     AND module=%s
                GROUP BY model
               """, (module,))
    for model, res_ids in cr.fetchall():
        if model == 'ir.model':
            model_ids = tuple(res_ids)
        elif model == 'ir.model.fields':
            field_ids = tuple(res_ids)
        elif model == 'ir.ui.view':
            view_ids = tuple(res_ids)
        else:
            cr.execute('DELETE FROM "%s" WHERE id IN %%s' % table_of_model(cr, model), (tuple(res_ids),))

    for view_id in view_ids:
        remove_view(cr, view_id=view_id, deactivate_custom=True, silent=True)

    # clean up dashboards
    if field_ids:
        # clean dashboards group_by from removed columns
        cr.execute("""\
            SELECT      f.model, array_agg(f.name), array_agg(aw.id)
            FROM        ir_model_fields f
                JOIN    ir_act_window aw
                ON      aw.res_model = f.model
            WHERE       f.id IN %s
            AND         NOT f.model_id = ANY(%s)
            GROUP BY    f.model
            """, [field_ids, list(model_ids)])
        for model, fields, actions in cr.fetchall():
            cr.execute("""\
                SELECT  id, arch
                FROM    ir_ui_view_custom
                WHERE   arch ~ %s
                """, ["name=[\"'](%s)[\"']" % '|'.join(map(str, actions))])
            for id, arch in ((x, lxml.etree.fromstring(y))
                             for x, y in cr.fetchall()):
                for action in arch.iterfind('.//action'):
                    context = eval(action.get('context', '{}'),
                                   UnquoteEvalContext())
                    if context.get('group_by'):
                        context['group_by'] = list(
                            set(context['group_by']) - set(fields))
                        action.set('context', unicode(context))
                cr.execute("""\
                    UPDATE  ir_ui_view_custom
                    SET     arch = %s
                    WHERE   id = %s
                    """, [lxml.etree.tostring(arch), id])
        cr.execute("DELETE FROM ir_model_fields WHERE id IN %s", [field_ids])

    # remove relations
    cr.execute("""SELECT name
                    FROM ir_model_relation
                GROUP BY name
                  HAVING array_agg(module) = %s""", ([mod_id],))
    relations = tuple(map(itemgetter(0), cr.fetchall()))
    cr.execute("DELETE FROM ir_model_relation WHERE module=%s", (mod_id,))
    if relations:
        cr.execute("SELECT table_name FROM information_schema.tables WHERE table_name IN %s", (relations,))
        for rel, in cr.fetchall():
            cr.execute('DROP TABLE "%s" CASCADE' % (rel,))

    if model_ids:
        cr.execute("DELETE FROM ir_model WHERE id IN %s", (model_ids,))

    cr.execute("DELETE FROM ir_model_data WHERE module=%s", (module,))
    cr.execute("DELETE FROM ir_module_module WHERE name=%s", (module,))
    cr.execute("DELETE FROM ir_module_module_dependency WHERE name=%s", (module,))

def rename_module(cr, old, new):
    cr.execute("UPDATE ir_module_module SET name=%s WHERE name=%s", (new, old))
    cr.execute("UPDATE ir_module_module_dependency SET name=%s WHERE name=%s", (new, old))
    cr.execute("UPDATE ir_model_data SET module=%s WHERE module=%s", (new, old))
    mod_old = 'module_' + old
    mod_new = 'module_' + new
    cr.execute("""UPDATE ir_model_data
                     SET name = %s
                   WHERE name = %s
                     AND module = %s
                     AND model = %s
               """, (mod_new, mod_old, 'base', 'ir.module.module'))

def merge_module(cr, old, into):
    """Move all references of module `old` into module `into`
    """
    cr.execute("SELECT name, id FROM ir_module_module WHERE name IN %s", [(old, into)])
    mod_ids = dict(cr.fetchall())

    def _up(table, old, new):
        cr.execute("""UPDATE ir_model_{0} x
                         SET module=%s
                       WHERE module=%s
                         AND NOT EXISTS(SELECT 1
                                          FROM ir_model_{0} y
                                         WHERE y.name = x.name
                                           AND y.module = %s)
                   """.format(table),
                   [new, old, new])
        cr.execute("DELETE FROM ir_model_{0} WHERE module=%s".format(table), [old])

    _up('constraint', mod_ids[old], mod_ids[into])
    _up('relation', mod_ids[old], mod_ids[into])
    _up('data', old, into)

    # update dependencies
    cr.execute("""
        INSERT INTO ir_module_module_dependency(module_id, name)
        SELECT module_id, %s
          FROM ir_module_module_dependency d
         WHERE name=%s
           AND NOT EXISTS(SELECT 1
                            FROM ir_module_module_dependency o
                           WHERE o.module_id = d.module_id
                             AND o.name=%s)
    """, [into, old, into])

    cr.execute("DELETE FROM ir_module_module WHERE name=%s", [old])
    cr.execute("DELETE FROM ir_module_module_dependency WHERE name=%s", [old])

def force_install_module(cr, module, if_installed=None):
    subquery = ""
    subparams = ()
    if if_installed:
        subquery = """AND EXISTS(SELECT 1 FROM ir_module_module
                                  WHERE name IN %s
                                    AND state IN %s)"""
        subparams = (tuple(if_installed), _INSTALLED_MODULE_STATES)

    cr.execute("""UPDATE ir_module_module
                     SET state=CASE
                                 WHEN state = %s
                                   THEN %s
                                 WHEN state = %s
                                   THEN %s
                                 ELSE state
                               END,
                        demo=(select demo from ir_module_module where name='base')
                   WHERE name=%s
               """ + subquery + """
               RETURNING state
               """, ('to remove', 'to upgrade',
                     'uninstalled', 'to install',
                     module) + subparams)

    state, = cr.fetchone() or [None]
    return state

def new_module_dep(cr, module, new_dep):
    # One new dep at a time
    cr.execute("""INSERT INTO ir_module_module_dependency(name, module_id)
                       SELECT %s, id
                         FROM ir_module_module m
                        WHERE name=%s
                          AND NOT EXISTS(SELECT 1
                                           FROM ir_module_module_dependency
                                          WHERE module_id = m.id
                                            AND name=%s)
                """, (new_dep, module, new_dep))

    # Update new_dep state depending on module state
    cr.execute("""SELECT state from ir_module_module
                  WHERE name = %s""", [module])
    mod_state = (cr.fetchone() or ['n/a'])[0]
    if mod_state in _INSTALLED_MODULE_STATES:
        # Module was installed, need to install all its deps, recursively,
        # to make sure the new dep is installed
        cr.execute("""
            WITH RECURSIVE deps (mod_id, mod_name, mod_state, dep_name) AS (
                  SELECT m.id, m.name, m.state, d.name from ir_module_module_dependency d
                  JOIN ir_module_module m on (d.module_id = m.id)
                  WHERE m.name = %s
                UNION
                  SELECT m.id, m.name, m.state, d.name from ir_module_module m
                  JOIN deps ON deps.dep_name = m.name
                  JOIN ir_module_module_dependency d on (d.module_id = m.id)
            )
            UPDATE ir_module_module m
                       SET state = CASE
                                    WHEN state = 'to remove'
                                      THEN 'to upgrade'
                                    WHEN state = 'uninstalled'
                                      THEN 'to install'
                                   END,
                           demo=(select demo from ir_module_module where name='base')
                       FROM deps d
                       WHERE m.id = d.mod_id
                         AND d.mod_state in ('to remove', 'uninstalled')
        """, (module,))

def remove_module_deps(cr, module, old_deps):
    assert isinstance(old_deps, tuple)
    cr.execute("""DELETE FROM ir_module_module_dependency
                        WHERE module_id = (SELECT id
                                             FROM ir_module_module
                                            WHERE name=%s)
                          AND name IN %s
               """, (module, old_deps))

def new_module(cr, module, auto_install_deps=None):
    if module_installed(cr, module):
        #Avoid duplicate entries for module which is already installed,
        #even before it has become standard module in new version
        return
    if auto_install_deps:
        cr.execute("""SELECT count(1)
                        FROM ir_module_module
                       WHERE name IN %s
                         AND state IN %s
                   """, (auto_install_deps, _INSTALLED_MODULE_STATES))

        state = 'to install' if cr.fetchone()[0] == len(auto_install_deps) else 'uninstalled'
    else:
        state = 'uninstalled'
    cr.execute("""\
        INSERT INTO ir_module_module (
            name, state, demo
        ) VALUES (
            %s, %s, (select demo from ir_module_module where name='base'))
        RETURNING id""", (module, state))
    new_id, = cr.fetchone()

    cr.execute("""\
        INSERT INTO ir_model_data (
            name, module, noupdate, model, res_id
        ) VALUES (
            'module_'||%s, 'base', 't', 'ir.module.module', %s
        )""", (module, new_id))

def force_migration_of_fresh_module(cr, module):
    """It may appear that new (or forced installed) modules need a migration script to grab data
       form other module. (we cannot add a pre-init hook on the fly)
    """
    filename, _ = frame_codeinfo(currentframe(), 1)
    version = '.'.join(filename.split(os.path.sep)[-2].split('.')[:2])

    # Force module state to be in `to upgrade`.
    # Needed for migration script execution. See http://git.io/vnF7f
    cr.execute("""UPDATE ir_module_module
                     SET state='to upgrade',
                         latest_version=%s
                   WHERE name=%s
                     AND state='to install'
               RETURNING id""", [version, module])
    if cr.rowcount:
        # Force module in `init` mode beside its state is forced to `to upgrade`
        # See http://git.io/vnF7O
        openerp.tools.config['init'][module] = "oh yeah!"

def column_exists(cr, table, column):
    return column_type(cr, table, column) is not None

def column_type(cr, table, column):
    cr.execute("""SELECT udt_name
                    FROM information_schema.columns
                   WHERE table_name = %s
                     AND column_name = %s
               """, (table, column))

    r = cr.fetchone()
    return r[0] if r else None

def create_column(cr, table, column, definition):
    curtype = column_type(cr, table, column)
    if curtype:
        # TODO compare with definition
        pass
    else:
        cr.execute("""ALTER TABLE "%s" ADD COLUMN "%s" %s""" % (table, column, definition))

def remove_column(cr, table, column, cascade=False):
    if column_exists(cr, table, column):
        drop_depending_views(cr, table, column)
        drop_cascade = " CASCADE" if cascade else ""
        cr.execute('ALTER TABLE "{0}" DROP COLUMN "{1}"{2}'.format(table, column, drop_cascade))

def table_exists(cr, table):
    cr.execute("""SELECT 1
                    FROM information_schema.tables
                   WHERE table_name = %s
                     AND table_type = 'BASE TABLE'
               """, (table,))
    return cr.fetchone() is not None

def view_exists(cr, view):
    cr.execute("SELECT 1 FROM information_schema.views WHERE table_name=%s", [view])
    return bool(cr.rowcount)

def get_fk(cr, table):
    """return the list of foreign keys pointing to `table`

        returns a 4 tuple: (foreign_table, foreign_column, constraint_name, on_delete_action)

        Foreign key deletion action code:
            a = no action, r = restrict, c = cascade, n = set null, d = set default
    """
    q = """SELECT quote_ident(cl1.relname) as table,
                  quote_ident(att1.attname) as column,
                  quote_ident(con.conname) as conname,
                  con.confdeltype
             FROM pg_constraint as con, pg_class as cl1, pg_class as cl2,
                  pg_attribute as att1, pg_attribute as att2
            WHERE con.conrelid = cl1.oid
              AND con.confrelid = cl2.oid
              AND array_lower(con.conkey, 1) = 1
              AND con.conkey[1] = att1.attnum
              AND att1.attrelid = cl1.oid
              AND cl2.relname = %s
              AND att2.attname = 'id'
              AND array_lower(con.confkey, 1) = 1
              AND con.confkey[1] = att2.attnum
              AND att2.attrelid = cl2.oid
              AND con.contype = 'f'
    """
    cr.execute(q, (table,))
    return cr.fetchall()

def get_index_on(cr, table, *columns):
    """
        return a tuple (index_name, unique, pk)
    """
    cr.execute("""
        select name, indisunique, indisprimary
          from (select quote_ident(i.relname) as name,
                       x.indisunique, x.indisprimary,
                       array_agg(a.attname::text order by a.attname) as attrs
                  FROM (select *, unnest(indkey) as unnest_indkey from pg_index) x
                  JOIN pg_class c ON c.oid = x.indrelid
                  JOIN pg_class i ON i.oid = x.indexrelid
                  JOIN pg_attribute a ON (a.attrelid=c.oid AND a.attnum=x.unnest_indkey)
                 WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char"]))
                   AND i.relkind = 'i'::"char"
                   AND c.relname = %s
              group by 1, 2, 3
          ) idx
         where attrs = %s
    """, [table, sorted(columns)])
    return cr.fetchone()

def get_depending_views(cr, table, column):
    # http://stackoverflow.com/a/11773226/75349
    q = """
        SELECT distinct quote_ident(dependee.relname)
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
        JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
        JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
            AND pg_depend.refobjsubid = pg_attribute.attnum
        WHERE dependent.relname = %s
        AND pg_attribute.attnum > 0
        AND pg_attribute.attname = %s
        AND dependee.relkind='v'
    """
    cr.execute(q, [table, column])
    return map(itemgetter(0), cr.fetchall())

def get_columns(cr, table, ignore=('id',), extra_prefixes=None):
    """return the list of columns in table (minus ignored ones)
        can also returns the list multiple times with different prefixes.
        This can be used to duplicating records (INSERT SELECT from the same table)
    """
    select = 'quote_ident(column_name)'
    params = []
    if extra_prefixes:
        select = ','.join([select] + ["concat(%%s, '.', %s)" % select] * len(extra_prefixes))
        params = list(extra_prefixes)

    cr.execute("""SELECT {select}
                   FROM information_schema.columns
                  WHERE table_name=%s
                    AND column_name NOT IN %s
               """.format(select=select), params + [table, ignore])
    return zip(*cr.fetchall())

def drop_depending_views(cr, table, column):
    """drop views depending on a field to allow the ORM to resize it in-place"""
    for v in get_depending_views(cr, table, column):
        cr.execute("DROP VIEW IF EXISTS {0} CASCADE".format(v))

def remove_field(cr, model, fieldname, cascade=False):
    cr.execute("DELETE FROM ir_model_fields WHERE model=%s AND name=%s RETURNING id", (model, fieldname))
    fids = tuple(map(itemgetter(0), cr.fetchall()))
    if fids:
        cr.execute("DELETE FROM ir_model_data WHERE model=%s AND res_id IN %s", ('ir.model.fields', fids))
    # cleanup translations
    cr.execute("""
       DELETE FROM ir_translation
        WHERE name=%s
          AND type in ('field', 'help', 'model', 'selection')   -- ignore wizard_* translations
    """, ['%s,%s' % (model, fieldname)])

    table = table_of_model(cr, model)
    remove_column(cr, table, fieldname, cascade=cascade)

def move_field_to_module(cr, model, fieldname, old_module, new_module):
    name = 'field_%s_%s' % (model.replace('.', '_'), fieldname)
    cr.execute("""UPDATE ir_model_data
                     SET module=%s
                   WHERE model=%s
                     AND name=%s
                     AND module=%s
               """, (new_module, 'ir.model.fields', name, old_module))

def rename_field(cr, model, old, new):
    cr.execute("UPDATE ir_model_fields SET name=%s WHERE model=%s AND name=%s RETURNING id", (new, model, old))
    [fid] = cr.fetchone() or [None]
    if fid:
        name = 'field_%s_%s' % (model.replace('.', '_'), new)
        cr.execute("UPDATE ir_model_data SET name=%s WHERE model=%s AND res_id=%s", (name, 'ir.model.fields', fid))
        cr.execute("UPDATE ir_property SET name=%s WHERE fields_id=%s", [new, fid])

    cr.execute("""
       UPDATE ir_translation
          SET name=%s
        WHERE name=%s
          AND type in ('field', 'help', 'model', 'selection')   -- ignore wizard_* translations
    """, ['%s,%s' % (model, new), '%s,%s' % (model, old)])

    table = table_of_model(cr, model)
    if column_exists(cr, table, old):
        cr.execute('ALTER TABLE "{0}" RENAME COLUMN "{1}" TO "{2}"'.format(table, old, new))

def convert_field_to_property(cr, model, field, type,
                              default_value, default_value_ref=None,
                              company_field='company_id'):
    type2field = {
        'char': 'value_text',
        'float': 'value_float',
        'boolean': 'value_integer',
        'integer': 'value_integer',
        'text': 'value_text',
        'binary': 'value_binary',
        # 'many2one': 'value_reference',    # FIXME handle this correcty. See http://git.io/vWmaw
        'date': 'value_datetime',
        'datetime': 'value_datetime',
        'selection': 'value_text',
    }

    assert type in type2field

    cr.execute("SELECT id FROM ir_model_fields WHERE model=%s AND name=%s", (model, field))
    [fields_id] = cr.fetchone()

    table = table_of_model(cr, model)

    where_clause, where_params = '{field} != %s'.format(field=field), (default_value,)
    if is_field_anonymized(cr, model, field):
        # if field is anonymized, we need to create a property for each record
        where_clause, where_params = '1 = 1', tuple()
        # and we need to unanonymize its values
        sql_default_value = cr.mogrify('%s', [default_value])
        register_unanonymization_query(
            cr, model, field,
            "UPDATE ir_property "
            "   SET {value_field} = COALESCE(%(value)s, {default_value}) "
            " WHERE res_id = CONCAT('{model},', %(id)s) "
            "   AND name='{field}' "
            "   AND type='{type}' "
            "   AND fields_id={fields_id} "
            "".format(value_field=type2field[type], default_value=sql_default_value,
                      model=model, field=field, type=type, fields_id=fields_id)
        )

    cr.execute("""INSERT INTO ir_property(name, type, fields_id, company_id, res_id, {value_field})
                    SELECT %s, %s, %s, {company_field}, CONCAT('{model},', id), {field}
                      FROM {table}
                     WHERE {where_clause}
               """.format(value_field=type2field[type], company_field=company_field, model=model,
                          table=table, field=field, where_clause=where_clause),
               (field, type, fields_id) + where_params
               )
    # default property
    if default_value:
        cr.execute("""INSERT INTO ir_property(name, type, fields_id, {value_field})
                           VALUES (%s, %s, %s, %s)
                        RETURNING id
                   """.format(value_field=type2field[type]),
                   (field, type, fields_id, default_value)
                   )
        [prop_id] = cr.fetchone()
        if default_value_ref:
            module, _, xid = default_value_ref.partition('.')
            cr.execute("""INSERT INTO ir_model_data
                                      (module, name, model, res_id, noupdate)
                               VALUES (%s, %s, %s, %s, %s)
                       """, (module, xid, 'ir.property', prop_id, True))

    remove_column(cr, table, field, cascade=True)

def is_field_anonymized(cr, model, field):
    if not module_installed(cr, 'anonymization'):
        return False
    cr.execute("""SELECT id
                    FROM ir_model_fields_anonymization
                   WHERE model_name = %s
                     AND field_name = %s
                     AND state = 'anonymized'
               """, [model, field])
    return bool(cr.rowcount)

def register_unanonymization_query(cr, model, field, query, query_type='sql', sequence=10):
    cr.execute("""INSERT INTO ir_model_fields_anonymization_migration_fix(
                    target_version, sequence, query_type, model_name, field_name, query
                  ) VALUES (%s, %s, %s, %s, %s, %s)
               """, [release.major_version, sequence, query_type, model, field, query])

def res_model_res_id(cr, filtered=True):
    each = [
        ('ir.attachment', 'res_model', 'res_id'),
        ('ir.cron', 'model', None),
        ('ir.actions.report.xml', 'model', None),
        ('ir.actions.act_window', 'res_model', 'res_id'),
        ('ir.actions.act_window', 'src_model', None),
        ('ir.actions.server', 'wkf_model_name', None),   # stored related, also need to be updated
        ('ir.actions.server', 'crud_model_name', None),  # idem
        ('ir.actions.client', 'res_model', None),
        ('ir.model', 'model', None),
        ('ir.model.fields', 'model', None),
        ('ir.model.fields', 'relation', None),      # destination of a relation field
        ('ir.model.data', 'model', 'res_id'),
        ('ir.filters', 'model_id', None),     # YUCK!, not an id
        ('ir.exports', 'resource', None),
        ('ir.ui.view', 'model', None),
        ('ir.values', 'model', 'res_id'),
        ('workflow.transition', 'trigger_model', None),
        ('workflow_triggers', 'model', None),

        ('ir.model.fields.anonymization', 'model_name', None),
        ('ir.model.fields.anonymization.migration.fix', 'model_name', None),
        ('base_import.import', 'res_model', None),
        ('email.template', 'model', None),      # stored related
        ('mail.template', 'model', None),       # model renamed in saas~6
        # ('mail.alias', 'alias_model_id.model', 'alias_force_thread_id'),
        # ('mail.alias', 'alias_parent_model_id.model', 'alias_parent_thread_id'),
        ('mail.followers', 'res_model', 'res_id'),
        ('mail.message.subtype', 'res_model', None),
        ('mail.message', 'model', 'res_id'),
        ('mail.wizard.invite', 'res_model', 'res_id'),
        ('mail.mail.statistics', 'model', 'res_id'),
        ('project.project', 'alias_model', None),
        ('rating.rating', 'res_model', 'res_id'),
    ]

    for model, res_model, res_id in each:
        if filtered:
            table = table_of_model(cr, model)
            if not column_exists(cr, table, res_model):
                continue
            if res_id and not column_exists(cr, table, res_id):
                continue

        yield model, res_model, res_id

def _rm_refs(cr, model, ids=None):
    if ids is None:
        match = 'like %s'
        needle = model + ',%'
    else:
        if not ids:
            return
        match = 'in %s'
        needle = tuple('{0},{1}'.format(model, i) for i in ids)

    # "model-comma" fields
    cr.execute("""
        SELECT model, name
          FROM ir_model_fields
         WHERE ttype='reference'
         UNION
        SELECT 'ir.translation', 'name'
         UNION
        SELECT 'ir.values', 'value'
    """)

    for ref_model, ref_column in cr.fetchall():
        table = table_of_model(cr, ref_model)
        # NOTE table_exists is needed to avoid deleting from views
        if table_exists(cr, table) and column_exists(cr, table, ref_column):
            query = 'DELETE FROM "{0}" WHERE "{1}" {2}'.format(table, ref_column, match)
            cr.execute(query, (needle,))
            # TODO make it recursive?

    if ids is None:
        cr.execute("""
            DELETE FROM ir_translation
             WHERE name=%s
               AND type IN ('constraint', 'sql_constraint', 'view', 'report', 'rml', 'xsl')
        """, [model])

    if model.startswith('ir.action'):
        query = 'DELETE FROM ir_values WHERE key=%s AND value {0}'.format(match)
        cr.execute(query, ('action', needle))
        # TODO make it recursive?

def delete_model(cr, model, drop_table=True):
    model_underscore = model.replace('.', '_')

    # remove references
    for dest_model, res_model, _ in res_model_res_id(cr):
        if dest_model == 'ir.model':
            continue
        table = table_of_model(cr, dest_model)
        query = 'DELETE FROM "{0}" WHERE "{1}"=%s RETURNING id'.format(table, res_model)
        cr.execute(query, (model,))
        ids = map(itemgetter(0), cr.fetchall())
        _rm_refs(cr, dest_model, ids)

    _rm_refs(cr, model)

    cr.execute("SELECT id FROM ir_model WHERE model=%s", (model,))
    [mod_id] = cr.fetchone() or [None]
    if mod_id:
        # some required fk are in "ON DELETE SET NULL".
        for tbl in 'base_action_rule google_drive_config'.split():
            if table_exists(cr, tbl):
                cr.execute("DELETE FROM {0} WHERE model_id=%s".format(tbl), [mod_id])
        cr.execute("DELETE FROM ir_model_constraint WHERE model=%s", (mod_id,))
        cr.execute("DELETE FROM ir_model_relation WHERE model=%s", (mod_id,))

        # Drop XML IDs of ir.rule and ir.model.access records that will be cascade-dropped,
        # when the ir.model record is dropped - just in case they need to be re-created
        cr.execute("""DELETE FROM ir_model_data x
                            USING ir_rule a
                            WHERE x.res_id = a.id AND x.model='ir.rule' AND
                                  a.model_id = %s; """, (mod_id,))
        cr.execute("""DELETE FROM ir_model_data x
                            USING ir_model_access a
                            WHERE x.res_id = a.id AND x.model='ir.model.access' AND
                                  a.model_id = %s; """, (mod_id,))

        cr.execute("DELETE FROM ir_model WHERE id=%s", (mod_id,))

    cr.execute("DELETE FROM ir_model_data WHERE model=%s AND name=%s",
               ('ir.model', 'model_%s' % model_underscore))
    cr.execute("DELETE FROM ir_model_data WHERE model=%s AND name like %s",
               ('ir.model.fields', 'field_%s_%%' % model_underscore))

    table = table_of_model(cr, model)
    if drop_table:
        if table_exists(cr, table):
            cr.execute('DROP TABLE "{0}" CASCADE'.format(table))
        elif view_exists(cr, table):
            cr.execute('DROP VIEW "{0}" CASCADE'.format(table))


def move_model(cr, model, from_module, to_module, move_data=False, delete=False):
    """
        move model `model` from `from_module` to `to_module`.
        if `delete` is set and `to_module` is not installed, delete the model.
    """
    if delete and not module_installed(cr, to_module):
        delete_model(cr, model)
        return

    model_u = model.replace('.', '_')
    cr.execute("UPDATE ir_model_data SET module=%s WHERE module=%s AND model=%s AND name=%s",
               (to_module, from_module, 'ir.model', 'model_%s' % model_u))

    cr.execute("""UPDATE ir_model_data
                     SET module=%s
                   WHERE module=%s
                     AND model=%s
                     AND name LIKE %s
               """, (to_module, from_module, 'ir.model.fields', 'field_%s_%%' % model_u))

    if move_data:
        cr.execute("""UPDATE ir_model_data
                         SET module=%s
                       WHERE module=%s
                         AND model=%s
                   """, (to_module, from_module, model))


def rename_model(cr, old, new, rename_table=True):
    if rename_table:
        old_table = table_of_model(cr, old)
        new_table = table_of_model(cr, new)
        cr.execute('ALTER TABLE "{0}" RENAME TO "{1}"'.format(old_table, new_table))
        cr.execute('ALTER SEQUENCE "{0}_id_seq" RENAME TO "{1}_id_seq"'.format(old_table, new_table))
        # find & rename primary key, may still use an old name from a former migration
        cr.execute("""
            SELECT  conname
            FROM    pg_index, pg_constraint
            WHERE   indrelid = %s::regclass
            AND     indisprimary
            AND     conrelid = indrelid
            AND     conindid = indexrelid
            AND     confrelid = 0;
            """, [new_table])
        primary_key, = cr.fetchone()
        cr.execute('ALTER INDEX "{0}" RENAME TO "{1}_pkey"'.format(primary_key, new_table))

        # DELETE all constraints and indexes (ignore the PK), ORM will recreate them.
        cr.execute("""SELECT constraint_name
                        FROM information_schema.table_constraints
                       WHERE table_name=%s
                         AND constraint_type!=%s
                         AND constraint_name !~ '^[0-9_]+_not_null$'
                   """, (new_table, 'PRIMARY KEY'))
        for const, in cr.fetchall():
            cr.execute("DELETE FROM ir_model_constraint WHERE name=%s", (const,))
            cr.execute('ALTER TABLE "{0}" DROP CONSTRAINT "{1}"'.format(new_table, const))

    updates = [('wkf', 'osv')] + [r[:2] for r in res_model_res_id(cr)]

    for model, column in updates:
        table = table_of_model(cr, model)
        query = 'UPDATE {t} SET {c}=%s WHERE {c}=%s'.format(t=table, c=column)
        cr.execute(query, (new, old))

    # "model-comma" fields
    cr.execute("""
        SELECT model, name
          FROM ir_model_fields
         WHERE ttype='reference'
         UNION
        SELECT 'ir.translation', 'name'
         UNION
        SELECT 'ir.values', 'value'
    """)
    for model, column in cr.fetchall():
        table = table_of_model(cr, model)
        if column_exists(cr, table, column):
            cr.execute("""UPDATE "{table}"
                             SET {column}='{new}' || substring({column} FROM '%#",%#"' FOR '#')
                           WHERE {column} LIKE '{old},%'
                       """.format(table=table, column=column, new=new, old=old))

    cr.execute("""
        UPDATE ir_translation
           SET name=%s
         WHERE name=%s
           AND type IN ('constraint', 'sql_constraint', 'view', 'report', 'rml', 'xsl')
    """, [new, old])
    old_u = old.replace('.', '_')
    new_u = new.replace('.', '_')

    cr.execute("UPDATE ir_model_data SET name=%s WHERE model=%s AND name=%s",
               ('model_%s' % new_u, 'ir.model', 'model_%s' % old_u))

    cr.execute("""UPDATE ir_model_data
                     SET name=%s || substring(name from %s)
                   WHERE model=%s
                     AND name LIKE %s
               """, ('field_%s_' % new_u, len(old_u) + 7, 'ir.model.fields', 'field_%s_%%' % old_u))

    cr.execute(r"""UPDATE ir_act_server
                      SET code=regexp_replace(code, '([''"]){old}\1', '\1{new}\1', 'g'),
                          condition=regexp_replace(condition, '([''"]){old}\1', '\1{new}\1', 'g')
                """.format(old=old.replace('.', r'\.'), new=new))


def replace_record_references(cr, old, new, replace_xmlid=True):
    """replace all (in)direct references of a record by another"""
    # TODO update workflow instances?
    assert isinstance(old, tuple) and len(old) == 2
    assert isinstance(new, tuple) and len(new) == 2

    if old[0] == new[0]:
        # same model, also change direct references (fk)
        for table, fk, _, _ in get_fk(cr, table_of_model(cr, old[0])):
            cr.execute('UPDATE {table} SET {fk}=%s WHERE {fk}=%s'.format(table=table, fk=fk),
                       (new[1], old[1]))

    for model, res_model, res_id in res_model_res_id(cr):
        if not res_id:
            continue
        if model == 'ir.model.data' and not replace_xmlid:
            continue
        table = table_of_model(cr, model)
        cr.execute("""UPDATE {table}
                         SET {res_model}=%s, {res_id}=%s
                       WHERE {res_model}=%s
                         AND {res_id}=%s
                   """.format(table=table, res_model=res_model, res_id=res_id),
                   new + old)

    comma_new = '%s,%d' % new
    comma_old = '%s,%d' % old
    cr.execute("SELECT model, name FROM ir_model_fields WHERE ttype=%s", ('reference',))
    for model, column in cr.fetchall():
        table = table_of_model(cr, model)
        if column_exists(cr, table, column):
            cr.execute("""UPDATE "{table}"
                             SET "{column}"=%s
                           WHERE "{column}"=%s
                       """.format(table=table, column=column),
                       (comma_new, comma_old))


def update_field_references(cr, old, new, only_models=None):
    """
        Replace all references to field `old` to `new` in:
            - ir_filters
            - ir_exports_line
            - ir_act_server
            - ir_rule
    """
    p = {
        'old': '\y%s\y' % (old,),
        'new': new,
        'def_old': '\ydefault_%s\y' % (old,),
        'def_new': 'default_%s' % (new,),
        'models': tuple(only_models) if only_models else (),
    }

    q = """
        UPDATE ir_filters
           SET domain = regexp_replace(domain, %(old)s, %(new)s, 'g'),
               context = regexp_replace(regexp_replace(context,
                                                       %(old)s, %(new)s, 'g'),
                                                       %(def_old)s, %(def_new)s, 'g')
    """
    if only_models:
        q += " WHERE model_id IN %(models)s"

    cr.execute(q, p)

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
    """
    cr.execute(q, p)

    # ir.action.server
    q = """
        UPDATE ir_act_server s
           SET condition = regexp_replace(condition, %(old)s, %(new)s, 'g'),
               code = regexp_replace(code, %(old)s, %(new)s, 'g')
    """
    if only_models:
        q += """
          FROM ir_model m
         WHERE m.id = s.model_id
           AND m.model IN %(models)s
           AND
        """
    else:
        q += " WHERE "

    q += "s.state = 'code'"
    cr.execute(q, p)

    # ir.rule
    q = """
        UPDATE ir_rule r
           SET domain_force = regexp_replace(domain_force, %(old)s, %(new)s, 'g')
    """
    if only_models:
        q += """
          FROM ir_model m
         WHERE m.id = r.model_id
           AND m.model IN %(models)s
        """
    cr.execute(q, p)


def rst2html(rst):
    overrides = dict(embed_stylesheet=False, doctitle_xform=False, output_encoding='unicode', xml_declaration=False)
    html = publish_string(source=dedent(rst), settings_overrides=overrides, writer=MyWriter())
    return html_sanitize(html, silent=False)

def md2html(md):
    extensions = [
        'markdown.extensions.smart_strong',
        'markdown.extensions.nl2br',
        'markdown.extensions.sane_lists',
    ]
    return markdown.markdown(md, extensions=extensions)

_DEFAULT_HEADER = """
<p>Odoo has been upgraded to version {version}.</p>
<h2>What's new in this upgrade?</h2>
"""

_DEFAULT_FOOTER = "<p>Enjoy the new Odoo Online!</p>"

_DEFAULT_RECIPIENT = 'mail.%s_all_employees' % ['group', 'channel'][release.version_info[:2] >= (9, 0)]

def announce(cr, version, msg, format='rst', recipient=_DEFAULT_RECIPIENT,
             header=_DEFAULT_HEADER, footer=_DEFAULT_FOOTER):
    registry = RegistryManager.get(cr.dbname)
    IMD = registry['ir.model.data']

    # do not notify early, in case the migration fails halfway through
    ctx = {'mail_notify_force_send': False, 'mail_notify_author': True}

    user = registry['res.users'].browse(cr, SUPERUSER_ID, SUPERUSER_ID, context=ctx)

    # default recipient
    poster = user.message_post

    if recipient:
        rmod, _, rxid = recipient.partition('.')
        try:
            poster = IMD.get_object(cr, SUPERUSER_ID, rmod, rxid, context=ctx).message_post
        except (ValueError, AttributeError):
            # Cannot find record, post the message on the wall of the admin
            pass

    if format == 'rst':
        msg = rst2html(msg)
    elif format == 'md':
        msg = md2html(msg)

    message = ((header or "") + msg + (footer or "")).format(version=version)
    _logger.debug(message)

    type_field = ['type', 'message_type'][release.version_info[:2] >= (9, 0)]
    kw = {type_field: 'notification'}

    try:
        poster(body=message, partner_ids=[user.partner_id.id], subtype='mail.mt_comment', **kw)
    except Exception:
        _logger.warning('Cannot announce message', exc_info=True)

def drop_workflow(cr, osv):
    cr.execute("""
        -- we want to first drop the foreign keys on the workitems because
        -- it slows down the process a lot
        ALTER TABLE wkf_triggers DROP CONSTRAINT wkf_triggers_workitem_id_fkey;
        ALTER TABLE wkf_workitem DROP CONSTRAINT wkf_workitem_act_id_fkey;
        ALTER TABLE wkf_workitem DROP CONSTRAINT wkf_workitem_inst_id_fkey;
        ALTER TABLE wkf_triggers DROP CONSTRAINT wkf_triggers_instance_id_fkey;

        WITH deleted_wkf AS (
            DELETE FROM wkf WHERE osv = %s RETURNING id
        ),
        deleted_wkf_instance AS (
            DELETE FROM wkf_instance WHERE EXISTS (
                SELECT 1 FROM deleted_wkf
                    WHERE deleted_wkf.id = wkf_instance.wkf_id
            ) RETURNING id
        ),
        deleted_triggers AS (
            DELETE FROM wkf_triggers WHERE EXISTS (
                SELECT 1 FROM deleted_wkf_instance
                    WHERE deleted_wkf_instance.id = wkf_triggers.instance_id
            ) RETURNING id
        ),
        deleted_wkf_activity AS (
            DELETE FROM wkf_activity WHERE EXISTS (
                SELECT 1 FROM deleted_wkf
                    WHERE deleted_wkf.id = wkf_activity.wkf_id
            ) RETURNING id
        )
        DELETE FROM wkf_workitem WHERE EXISTS (
            SELECT 1 FROM deleted_wkf_instance
                WHERE deleted_wkf_instance.id = wkf_workitem.inst_id
        );

        ALTER TABLE wkf_triggers ADD CONSTRAINT wkf_triggers_workitem_id_fkey
            FOREIGN KEY (workitem_id) REFERENCES wkf_workitem(id)
            ON DELETE CASCADE;
        ALTER TABLE wkf_workitem ADD CONSTRAINT wkf_workitem_act_id_fkey
            FOREIGN key (act_id) REFERENCES wkf_activity(id)
            ON DELETE CASCADE;
        ALTER TABLE wkf_workitem ADD CONSTRAINT wkf_workitem_inst_id_fkey
            FOREIGN KEY (inst_id) REFERENCES wkf_instance(id)
            ON DELETE CASCADE;
        ALTER TABLE wkf_triggers ADD CONSTRAINT wkf_triggers_instance_id_fkey
            FOREIGN KEY (instance_id) REFERENCES wkf_instance(id)
            ON DELETE CASCADE;
        """, [osv])

def chunk_and_wrap(func, it, size):
    """
    split the iterable 'it' into chunks of size 'size' and wrap each chunk
    using function 'func'
    """
    return chain.from_iterable(takewhile(bool,
        (func(islice(it, size)) for _ in count())))

def iter_browse(model, cr, uid, ids, context=None, chunk_size=200):
    """
    Iterate and browse through record without filling the cache.
    """
    def browse(model, cr, uid, ids, context=None):
        cr.commit()
        model.invalidate_cache(cr, uid)
        return model.browse(cr, uid, list(ids), context=context)
    return chunk_and_wrap(
            lambda subset: browse(model, cr, uid, subset, context=context),
            iter(ids), 200)
