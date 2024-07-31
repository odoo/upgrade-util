
import re
import json
import logging

from typing import Union, Callable, Dict, List

from itertools import chain


from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .parser import ast_to_string, transform_ast_nodes, parse, Literal

from .o_spreadsheet import load
from .revisions import CommandAdapter, Drop
from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)


def read_spreadsheet_attachments(cr, like_pattern=""):
    yield from read_spreadsheet_initial_data(cr, like_pattern)
    yield from read_spreadsheet_snapshots(cr, like_pattern)


def read_spreadsheet_snapshots(cr, like_pattern=""):
    cr.execute(
        """
        SELECT id, res_model, res_id, db_datas
          FROM ir_attachment
         WHERE res_model IN ('spreadsheet.dashboard', 'documents.document')
           AND res_field = 'spreadsheet_snapshot'
           AND position(%s::bytea in db_datas) > 0
        """,
        [like_pattern],
    )
    # TODO rename 'like_pattern', it's not LIKE because LIKE doesn't work because the field is of type bytea
    for attachment_id, res_model, res_id, db_datas in cr.fetchall():
        if db_datas:
            yield attachment_id, res_model, res_id, json.loads(db_datas.tobytes())


def read_spreadsheet_initial_data(cr, like_pattern=""):
    if util.table_exists(cr, "documents_document"):
        cr.execute(
            """
            SELECT doc.id AS document_id, a.id AS attachment_id, a.db_datas
              FROM documents_document doc
         LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
             WHERE doc.handler='spreadsheet'
               AND position(%s::bytea in db_datas) > 0
            """,
            [like_pattern],
        )
        # TODO there are excel files in there!
        for document_id, attachment_id, db_datas in cr.fetchall():
            if db_datas:
                yield attachment_id, "documents.document", document_id, json.loads(db_datas.tobytes())

    if util.table_exists(cr, "spreadsheet_dashboard"):
        data_field = _magic_spreadsheet_field(cr) #"spreadsheet_binary_data" if version_gte("saas~16.3") else "data"
        cr.execute(
            """
            SELECT id, res_model, res_id, db_datas
            FROM ir_attachment
            WHERE res_model = 'spreadsheet.dashboard'
            AND res_field = %s
            AND position(%s::bytea in db_datas) > 0
            """,
            [data_field, like_pattern],
        )
        for attachment_id, res_model, res_id, db_datas in cr.fetchall():
            if db_datas:
                yield attachment_id, res_model, res_id, json.loads(db_datas.tobytes())

def _magic_spreadsheet_field(cr):
    cr.execute(
    """
    SELECT count(1)
        FROM ir_model_fields
        WHERE model='spreadsheet.mixin'
        AND name='spreadsheet_binary_data';
    """
    )
    return cr.fetchone()[0] and 'spreadsheet_binary_data' or 'data'

def apply_in_all_spreadsheets(cr, like_pattern, callback):
    # print("like pattern :   ", like_pattern)
    b = False
    # upgrade the initial data and all revisions based on it
    for attachment_id, res_model, res_id, db_datas in read_spreadsheet_initial_data(cr, like_pattern):
        print("attachment data id:   ", attachment_id)
        print("datas:   ", len(db_datas))
        b = True

        data, _ = callback(db_datas, [])
        write_attachment(cr, attachment_id, data)

        ## FIXME TODORAR batch the calls
        ## FIXME  we have to pass in the revisions regardless of the base data stuff
    # upgrade revisions
    # regardless of res_model res_id
    revisions_data = []
    revisions_ids = []

    for revision_id, commands in get_revisions(cr, "res_model", "res_id", like_pattern):
        revisions_data.append(json.loads(commands))
        revisions_ids.append(revision_id)
        _, revisions = callback({}, revisions_data)
        for rev_id, revision in zip(revisions_ids, revisions):
            cr.execute(
                """
                UPDATE spreadsheet_revision
                    SET commands=%s
                    WHERE id=%s
                """,
                [json.dumps(revision), rev_id],
            )
    if b:
        _logger.info("upgrading initial data and revisions")

    b = False
    # upgrade snapshots
    for attachment_id, _res_model, _res_id, db_datas in read_spreadsheet_snapshots(cr, like_pattern):
        print("attachment snapshot id:   ", attachment_id)

        b=True
        data, revisions = callback(db_datas, [])
        write_attachment(cr, attachment_id, data)

    if b:
        _logger.info("upgrading snapshots")



def write_attachment(cr, attachment_id, data):
    _logger.info("replacing attachment %s", attachment_id)
    cr.execute(
        """
        UPDATE ir_attachment
           SET db_datas=%s
         WHERE id=%s
        """,
        [json.dumps(data).encode(), attachment_id]
    )


def get_revisions(cr, res_model, res_id, like_pattern):
    if util.version_gte("16.0"):
        cr.execute(
            """
            SELECT id, commands
              FROM spreadsheet_revision
             WHERE commands LIKE %s
            """,
            ['%' + like_pattern + '%'],
        )
    else:
        cr.execute(
            """
            SELECT id, commands
              FROM spreadsheet_revision
             WHERE commands LIKE %s
            """,
            [like_pattern],
        )
    return cr.fetchall()

def upgrade_data(cr, upgrade_callback):
    for attachment_id, _res_model, _res_id, data in read_spreadsheet_attachments(cr):
        upgraded_data = upgrade_callback(load(data))
        cr.execute(
            """
            UPDATE ir_attachment
                SET db_datas=%s
                WHERE id=%s
            """,
            [json.dumps(upgraded_data).encode(), attachment_id],
        )
    _logger.info("spreadsheet json data upgraded")


def transform_data_source_functions(content, data_source_ids, functions, adapter):
    """Transforms data source function calls within content.
    The 'adapter' function is called with each function call AST node matching
    the function name and any data source.
    """
    if not content or not content.startswith("=") or not data_source_ids:
        return content
    if not any(fn.upper() in content.upper() for fn in functions):
        return content
    try:
        ast = parse(content)
    except ValueError:
        return content

    data_source_ids = [str(did) for did in data_source_ids]
    def _adapter(fun_call):
        # call the provided adapter only if the function name
        # and data source matches
        if fun_call.value.upper() in functions and len(fun_call.args) > 0:
            data_source_id = fun_call.args[0].value
            if str(data_source_id) in data_source_ids:
                return adapter(fun_call)
        return fun_call

    ast = transform_ast_nodes(ast, "FUNCALL", _adapter)
    return f"={ast_to_string(ast)}"



def adapt_view_link_cells(spreadsheet: Spreadsheet, adapter: Callable[[str], Union[str, None]]):
    def adapt_view_link(content):
        """A view link is formatted as a markdown link
        [text](odoo://view/<stringified view description dict>)
        """
        match = re.match(r"^\[([^\[]+)\]\(odoo://view/(.+)\)$", content)
        if not match:
            return content
        label = match.group(1)
        view_description = json.loads(match.group(2))
        result = adapter(view_description["action"])
        if result == Drop:
            return ""
        return f"[{label}](odoo://view/{json.dumps(view_description)})"

    for cell in spreadsheet.cells:
        cell["content"] = adapt_view_link(cell["content"])
    return (CommandAdapter("UPDATE_CELL", lambda cmd: dict(cmd, content=adapt_view_link(cmd.get("content")))),)



def remove_pivots(spreadsheet: Spreadsheet, pivot_ids: List[str], insert_cmd_predicate: Callable[[Dict], bool]):
    spreadsheet.delete_pivots(*pivot_ids)

    for cell in spreadsheet.cells:
        cell["content"] = remove_data_source_function(cell["content"], pivot_ids, ["ODOO.PIVOT", "ODOO.PIVOT.HEADER"])

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if insert_cmd_predicate(pivot):
            pivot_ids.append(pivot.id)
            return Drop
        return cmd

    def adapt_cmd_with_pivotId(cmd):
        pivot_id = cmd["pivotId"]
        if str(pivot_id) in pivot_ids:
            return Drop
        return cmd

    def adapt_re_insert(cmd):
        pivot_id = cmd["id"]
        if str(pivot_id) in pivot_ids:
            return Drop
        return cmd

    def adapt_global_filters(cmd):
        if cmd.get("pivot"):
            for pivot_id in pivot_ids:
                cmd["pivot"].pop(str(pivot_id), None)
        return cmd

    def adapt_update_cell(cmd):
        content = remove_data_source_function(cmd.get("content"), pivot_ids, ["ODOO.PIVOT", "ODOO.PIVOT.HEADER"])
        if not content:
            return Drop
        cmd["content"] = content

    return (
        CommandAdapter("INSERT_PIVOT", adapt_insert),
        CommandAdapter("RE_INSERT_PIVOT", adapt_re_insert),
        CommandAdapter("UPDATE_ODOO_PIVOT_DOMAIN", adapt_cmd_with_pivotId),
        CommandAdapter("RENAME_ODOO_PIVOT", adapt_cmd_with_pivotId),
        CommandAdapter("REMOVE_PIVOT", adapt_cmd_with_pivotId),
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_global_filters),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_global_filters),
        CommandAdapter("UPDATE_CELL", adapt_update_cell),
    )



def remove_lists(spreadsheet: Spreadsheet, list_ids: List[str], insert_cmd_predicate: Callable[[Dict], bool]):
    spreadsheet.delete_lists(*list_ids)
    for cell in spreadsheet.cells:
        cell["content"] = remove_data_source_function(cell["content"], list_ids, ["ODOO.LIST", "ODOO.LIST.HEADER"])

    def adapt_insert(cmd):
        list = create_data_source_from_cmd(cmd)
        if insert_cmd_predicate(list):
            list_ids.append(list.id)
            return Drop
        return cmd

    def adapt_re_insert(cmd):
        list_id = cmd["id"]
        if list_id in list_ids:
            return Drop
        return cmd

    def adapt_cmd_with_listId(cmd):
        list_id = cmd["listId"]
        if list_id in list_ids:
            return Drop
        return cmd

    def adapt_global_filters(cmd):
        if cmd.get("list"):
            for list_id in list_ids:
                cmd["list"].pop(list_id, None)
            if not cmd["list"]:
                del cmd["list"]
        return cmd

    def adapt_update_cell(cmd):
        content = remove_data_source_function(cmd.get("content"), list_ids, ["ODOO.LIST", "ODOO.LIST.HEADER"])
        if not content:
            return Drop
        cmd["content"] = content

    return (
        CommandAdapter("INSERT_ODOO_LIST", adapt_insert),
        CommandAdapter("RE_INSERT_ODOO_LIST", adapt_re_insert),
        CommandAdapter("RENAME_ODOO_LIST", adapt_cmd_with_listId),
        CommandAdapter("UPDATE_ODOO_LIST_DOMAIN", adapt_cmd_with_listId),
        CommandAdapter("REMOVE_ODOO_LIST", adapt_cmd_with_listId),
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_global_filters),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_global_filters),
        CommandAdapter("UPDATE_CELL", adapt_update_cell),
    )

def remove_odoo_charts(spreadsheet: Spreadsheet, chart_ids: List[str], insert_cmd_predicate: Callable[[Dict], bool]):
    spreadsheet.delete_figures(*chart_ids)

    def adapt_create_chart(cmd):
        chart = create_data_source_from_cmd(cmd)
        if cmd["definition"]["type"].startswith("odoo_") and insert_cmd_predicate(chart):
            chart_ids.append(cmd["id"])
            return Drop

    def adapt_chart_cmd_with_id(cmd):

        if cmd["id"] in chart_ids:
            return Drop

    def adapt_global_filters(cmd):
        if cmd.get("chart"):
            for chart_id in chart_ids:
                cmd["chart"].pop(chart_id, None)
    return (
        CommandAdapter("CREATE_CHART", adapt_create_chart),
        CommandAdapter("UPDATE_CHART", adapt_chart_cmd_with_id),
        CommandAdapter("DELETE_FIGURE", adapt_chart_cmd_with_id),
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_global_filters),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_global_filters),
    )


def remove_data_source_function(content, data_source_ids, functions, filter_ast=lambda ast: True):
    """check if the cell content contains a function that references a data source
    >>> remove_data_source_function('=ODOO.PIVOT(1, "revenue")', [1], {"ODOO.PIVOT"})
    ''
    >>> remove_data_source_function('=ODOO.PIVOT(2, "revenue")', [1], {"ODOO.PIVOT"})
    '=ODOO.PIVOT(2, "revenue")'.
    """

    def adapter(fun_call):
        if filter_ast(fun_call):
            # remove the func call and set something else instead
            return Literal("BOOLEAN", False)
        return fun_call

    new_content = transform_data_source_functions(content, data_source_ids, functions, adapter)
    return content if new_content == content else ""
