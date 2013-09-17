# Utility functions for migration scripts

from contextlib import contextmanager
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
    """Remove all references to a given module.
       Ensure to reassign records before calling this method
    """

    # delete constraints and relations...
    for table in ['constraint', 'relation']:
        cr.execute("""DELETE FROM ir_model_%s
                            WHERE module = (SELECT id
                                              FROM ir_module_module
                                             WHERE name=%%s)
                   """ % table, (module,))

    # remove module
    cr.execute("""
        DELETE FROM ir_module_module
              WHERE name=%s
          RETURNING state
    """, (module,))

    state = cr.fetchone()
    if not state or state[0] not in ('installed', 'to upgrade', 'to remove'):
        return

    # remove views
    cr.execute("""
        SELECT res_id
          FROM ir_model_data
         WHERE module=%s
           AND model=%s
    """, (module, 'ir.ui.view'))

    view_ids = tuple(x[0] for x in cr.fetchall())
    cr.execute('DELETE FROM ir_ui_view WHERE id IN %s', (view_ids,))

    # remove all ir.model.data
    cr.execute("DELETE FROM ir_model_data WHERE module=%s", (module,))
