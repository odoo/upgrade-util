# Utility functions for migration scripts

from contextlib import contextmanager
from operator import itemgetter
import time
#import psycopg2

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

    # delete data
    model_ids = tuple()
    cr.execute("""SELECT model, array_agg(res_id)
                    FROM ir_model_data
                GROUP BY model
                  HAVING array_agg(module) = ARRAY[%s::varchar]
               """, (module,))
    for model, res_ids in cr.fetchall():
        if model == 'ir.model':
            model_ids = tuple(res_ids)
        else:
            cr.execute('DELETE FROM "%s" WHERE id IN %%s' % table_of_model(model), (tuple(res_ids),))

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


def force_install_module(cr, module):
    cr.execute("""UPDATE ir_module_module
                     SET state=CASE
                                 WHEN state = %s
                                   THEN %s
                                 WHEN state = %s
                                   THEN %s
                                 ELSE state
                               END
                   WHERE name=%s
               RETURNING state
               """, ('to remove', 'to upgrade',
                     'uninstalled', 'to install',
                     module))

    state, = cr.fetchone() or [None]
    return state

def new_module_dep(cr, module, new_dep):
    # One new dep at a time
    # Update new_dep state depending of module state

    states_mod = ('installed', 'to install', 'to upgrade')

    cr.execute("""UPDATE ir_module_module
                     SET state=CASE
                                 WHEN state = %s
                                   THEN %s
                                 WHEN state = %s
                                   THEN %s
                                 ELSE state
                               END
                   WHERE name=%s
                     AND EXISTS(SELECT id
                                  FROM ir_module_module
                                 WHERE name=%s
                                   AND state IN %s
                                )
               """, ('to remove', 'to upgrade',
                     'uninstalled', 'to install',
                     new_dep, module, states_mod))

    cr.execute("""INSERT INTO ir_module_module_dependency(name, module_id)
                       SELECT %s, id
                         FROM ir_module_module m
                        WHERE name=%s
                          AND NOT EXISTS(SELECT 1
                                           FROM ir_module_module_dependency
                                          WHERE module_id = m.id
                                            AND name=%s)
                """, (new_dep, module, new_dep))

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

def table_exists(cr, table):
    cr.execute("""SELECT 1
                    FROM information_schema.tables
                   WHERE table_name = %s
                     AND table_type = 'BASE TABLE'
               """, (table,))
    return cr.fetchone() is not None

def remove_field(cr, model, fieldname):
    cr.execute("DELETE FROM ir_model_fields WHERE model=%s AND name=%s RETURNING id", (model, fieldname))
    fids = tuple(map(itemgetter(0), cr.fetchall()))
    if fids:
        cr.execute("DELETE FROM ir_model_data WHERE model=%s AND res_id IN %s", ('ir.model.fields', fids))
    table = table_of_model(cr, model)
    if column_exists(cr, table, fieldname):
        cr.execute('ALTER TABLE "{0}" DROP COLUMN "{1}"'.format(table, fieldname))
