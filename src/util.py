# Utility functions for migration scripts

#import psycopg2


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


def remove_record(cr, name):
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
    cr.execute('DELETE FROM %s WHERE id=%%s' % table, (res_id,))
    # TODO delete attachments & workflow instances


def column_exists(cr, table, column):
    # TODO
    pass
