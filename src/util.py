# Utility functions for migration scripts

from contextlib import contextmanager
import logging
from operator import itemgetter
from textwrap import dedent
import time

from docutils.core import publish_string

from openerp import SUPERUSER_ID
from openerp.addons.base.module.module import MyWriter
from openerp.modules.registry import RegistryManager
from openerp.tools.mail import html_sanitize

_logger = logging.getLogger(__name__)

_INSTALLED_MODULE_STATES = ('installed', 'to install', 'to upgrade')

class MigrationError(Exception):
    pass

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


def remove_record(cr, name, deactivate=False, active_field='active'):
    if isinstance(name, str):
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

    # delete relations only owned by this module
    cr.execute("""DELETE FROM ir_model_relation
                        WHERE module=%s returning name, model
               """, (mod_id,))
    relations = cr.fetchall()

    for rel in relations:
        rel_name, rel_model = rel
        cr.execute("""\
            select count(*) from ir_model_relation where name = %s and model = %s""",
        [rel_name, rel_model])
        other_relation_exists, = cr.fetchone()
        if not other_relation_exists:
            if table_exists(cr, rel_name):
                cr.execute("""DROP TABLE %s""" % (rel_name, ))

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

    # delete data
    model_ids = tuple()
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
        else:
            cr.execute('DELETE FROM "%s" WHERE id IN %%s' % table_of_model(cr, model), (tuple(res_ids),))

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
    cr.execute("""\
        UPDATE ir_model_data SET name='module_'||%s
        WHERE name='module_'%s
            AND module = 'base'
            AND model = 'ir.module.module'""",
              (new, old))

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
    # Update new_dep state depending of module state

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
                     AND EXISTS(SELECT id
                                  FROM ir_module_module
                                 WHERE name=%s
                                   AND state IN %s
                                )
               """, ('to remove', 'to upgrade',
                     'uninstalled', 'to install',
                     new_dep, module, _INSTALLED_MODULE_STATES))

    cr.execute("""INSERT INTO ir_module_module_dependency(name, module_id)
                       SELECT %s, id
                         FROM ir_module_module m
                        WHERE name=%s
                          AND NOT EXISTS(SELECT 1
                                           FROM ir_module_module_dependency
                                          WHERE module_id = m.id
                                            AND name=%s)
                """, (new_dep, module, new_dep))

def remove_module_deps(cr, module, old_deps):
    assert isinstance(old_deps, tuple)
    cr.execute("""DELETE FROM ir_module_module_dependency
                        WHERE module_id = (SELECT id
                                             FROM ir_module_module
                                            WHERE name=%s)
                          AND name IN %s
               """, (module, old_deps))

def new_module(cr, module, auto_install_deps=None):
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
        drop_cascade = " CASCADE" if cascade else ""
        cr.execute('ALTER TABLE "{0}" DROP COLUMN "{1}"{2}'.format(table, column, drop_cascade))

def table_exists(cr, table):
    cr.execute("""SELECT 1
                    FROM information_schema.tables
                   WHERE table_name = %s
                     AND table_type = 'BASE TABLE'
               """, (table,))
    return cr.fetchone() is not None

def get_fk(cr, table):
    q = """SELECT quote_ident(cl1.relname) as table,
                  quote_ident(att1.attname) as column
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


def remove_field(cr, model, fieldname):
    cr.execute("DELETE FROM ir_model_fields WHERE model=%s AND name=%s RETURNING id", (model, fieldname))
    fids = tuple(map(itemgetter(0), cr.fetchall()))
    if fids:
        cr.execute("DELETE FROM ir_model_data WHERE model=%s AND res_id IN %s", ('ir.model.fields', fids))
    table = table_of_model(cr, model)
    remove_column(cr, table, fieldname)

def move_field_to_module(cr, model, fieldname, old_module, new_module):
    name = 'field_%s_%s' % (model.replace('.', '_'), fieldname)
    cr.execute("""UPDATE ir_model_data
                     SET module=%s
                   WHERE model=%s
                     AND name=%s
                     AND module=%s
               """, (new_module, 'ir.model.fields', name, old_module))

def rename_field(cr, model, old, new):
    cr.execute("UPDATE ir_model_fields SET name=%s WHERE model=%s AND name=%s RETURNING id", (model, new, old))
    [fid] = cr.fetchone() or [None]
    if fid:
        name = 'field_%s_%s' % (model.replace('.', '_'), new)
        cr.execute("UPDATE ir_model_data SET name=%s WHERE model=%s AND res_id=%s", (name, 'ir.model.fields', fid))
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
        'many2one': 'value_reference',
        'date': 'value_datetime',
        'datetime': 'value_datetime',
        'selection': 'value_text',
    }

    assert type in type2field

    cr.execute("SELECT id FROM ir_model_fields WHERE model=%s AND name=%s", (model, field))
    [fields_id] = cr.fetchone()

    table = table_of_model(cr, model)

    cr.execute("""INSERT INTO ir_property(name, type, fields_id, company_id, res_id, {value_field})
                    SELECT %s, %s, %s, {company_field}, CONCAT('{model},', id), {field}
                      FROM {table}
                     WHERE {field} != %s
               """.format(value_field=type2field[type], company_field=company_field, model=model,
                          table=table, field=field),
               (field, type, fields_id, default_value)
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
        ('ir.model.data', 'model', 'res_id'),
        ('ir.filters', 'model_id', None),     # YUCK!, not an id
        ('ir.ui.view', 'model', None),
        ('ir.values', 'model', 'res_id'),
        ('workflow.transition', 'trigger_model', None),
        ('workflow_triggers', 'model', None),

        ('ir.model.fields.anonymization', 'model_name', None),
        ('ir.model.fields.anonymization.migration.fix', 'model_name', None),
        ('base_import.import', 'res_model', None),
        ('email.template', 'model', None),      # stored related
        # ('mail.alias', 'alias_model_id.model', 'alias_force_thread_id'),
        # ('mail.alias', 'alias_parent_model_id.model', 'alias_parent_thread_id'),
        ('mail.followers', 'res_model', 'res_id'),
        ('mail.message.subtype', 'res_model', None),
        ('mail.message', 'model', 'res_id'),
        ('mail.wizard.invite', 'res_model', 'res_id'),
        ('mail.mail.statistics', 'model', 'res_id'),
        ('project.project', 'alias_model', None),
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

    cr.execute("SELECT model, name FROM ir_model_fields WHERE ttype=%s", ('reference',))
    for ref_model, ref_column in cr.fetchall():
        table = table_of_model(cr, ref_model)
        if column_exists(cr, table, ref_column):
            query = 'DELETE FROM "{0}" WHERE "{1}" {2}'.format(table, ref_column, match)
            cr.execute(query, (needle,))
            # TODO make it recursive?

    if model.startswith('ir.action'):
        query = 'DELETE FROM ir_values WHERE key=%s AND value {0}'.format(match)
        cr.execute(query, ('action', needle))
        # TODO make it recursive?

def delete_model(cr, model, drop_table=True):
    model_underscore = model.replace('.', '_')

    # remove references
    for dest_model, res_model, _ in res_model_res_id(cr):
        table = table_of_model(cr, dest_model)
        query = 'DELETE FROM "{0}" WHERE "{1}"=%s RETURNING id'.format(table, res_model)
        cr.execute(query, (model,))
        ids = map(itemgetter(0), cr.fetchall())
        _rm_refs(cr, dest_model, ids)

    _rm_refs(cr, model)

    cr.execute("SELECT id FROM ir_model WHERE model=%s", (model,))
    [mod_id] = cr.fetchone() or [None]
    if mod_id:
        cr.execute("DELETE FROM ir_model_constraint WHERE model=%s", (mod_id,))
        cr.execute("DELETE FROM ir_model_relation WHERE model=%s", (mod_id,))
    cr.execute("DELETE FROM ir_model_data WHERE model=%s AND name=%s",
               ('ir.model', 'model_%s' % model_underscore))
    cr.execute("DELETE FROM ir_model_data WHERE model=%s AND name like %s",
               ('ir.model.fields', 'field_%s_%%' % model_underscore))

    table = table_of_model(cr, model)
    if drop_table and table_exists(cr, table):
        cr.execute('DROP TABLE "{0}" CASCADE'.format(table))


def move_model(cr, model, from_module, to_module, move_data=False):
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
        cr.execute('ALTER INDEX "{0}_pkey" RENAME TO "{1}_pkey"'.format(old_table, new_table))

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

    cr.execute("SELECT model, name FROM ir_model_fields WHERE ttype=%s", ('reference',))
    for model, column in cr.fetchall():
        table = table_of_model(cr, model)
        if column_exists(cr, table, column):
            cr.execute("""UPDATE "{table}"
                             SET {column}='{new}' || substring({column} FROM '%#",%#"' FOR '#')
                           WHERE {column} LIKE '{old},%'
                       """.format(table=table, column=column, new=new, old=old))

    old_u = old.replace('.', '_')
    new_u = new.replace('.', '_')

    cr.execute("UPDATE ir_model_data SET name=%s WHERE model=%s AND name=%s",
               ('model_%s' % new_u, 'ir.model', 'model_%s' % old_u))

    cr.execute("""UPDATE ir_model_data
                     SET name=%s || substring(name from %s)
                   WHERE model=%s
                     AND name LIKE %s
               """, ('field_%s_' % new_u, len(old_u) + 7, 'ir.model.fields', 'field_%s_%%' % old_u))


def replace_record_references(cr, old, new):
    """replace all (in)direct references of a record by another"""
    # TODO update workflow instances?
    assert isinstance(old, tuple) and len(old) == 2
    assert isinstance(new, tuple) and len(new) == 2

    if old[0] == new[0]:
        # same model, also change direct references (fk)
        for table, fk in get_fk(cr, table_of_model(cr, old[0])):
            cr.execute('UPDATE {table} SET {fk}=%s WHERE {fk}=%s'.format(table=table, fk=fk),
                       (new[1], old[1]))

    for model, res_model, res_id in res_model_res_id(cr):
        if not res_id:
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


def rst2html(rst):
    overrides = dict(embed_stylesheet=False, doctitle_xform=False, output_encoding='unicode', xml_declaration=False)
    html = publish_string(source=dedent(rst), settings_overrides=overrides, writer=MyWriter())
    return html_sanitize(html, silent=False)


_DEFAULT_HEADER = """
<p>OpenERP has been upgraded to version {version}.</p>
<h2>What's new in this upgrade?</h2>
"""

_DEFAULT_FOOTER = "<p>Enjoy the new OpenERP Online!</p>"

def announce(cr, version, msg, format='rst', recipient='mail.group_all_employees',
             header=_DEFAULT_HEADER, footer=_DEFAULT_FOOTER):
    registry = RegistryManager.get(cr.dbname)
    IMD = registry['ir.model.data']
    user = registry['res.users'].browse(cr, SUPERUSER_ID, SUPERUSER_ID)

    # default recipient
    poster = user.message_post

    if recipient:
        rmod, _, rxid = recipient.partition('.')
        try:
            poster = IMD.get_object(cr, SUPERUSER_ID, rmod, rxid).message_post
        except (ValueError, AttributeError):
            # Cannot found record, post the message on the wall of the admin
            pass

    if format == 'rst':
        msg = rst2html(msg)

    message = ((header or "") + msg + (footer or "")).format(version=version)
    _logger.debug(message)

    try:
        poster(body=message, partner_ids=[user.partner_id.id],
               type='notification', subtype='mail.mt_comment')
    except Exception:
        _logger.warning('Cannot announce message', exc_info=True)
