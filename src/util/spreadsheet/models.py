
from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .misc import apply_in_all_spreadsheets, adapt_view_link_cells, remove_lists, remove_pivots, remove_odoo_charts
from .revisions import CommandAdapter, Drop, transform_revisions_data, transform_commands


def rename_model_in_all_spreadsheets(cr, old_value, new_value):
    apply_in_all_spreadsheets(cr, old_value, (lambda data, revisions_data: rename_model(old_value, new_value, data, revisions_data)))

# TODO remove cr argument
def rename_model(old, new, data, revisions = ()):
    spreadsheet = Spreadsheet(data)
    adapters = _rename_model_in_list(spreadsheet, old, new)
    adapters += _rename_model_in_pivot(spreadsheet, old, new)
    adapters += _rename_model_in_filters(spreadsheet, old, new)
    adapters += _rename_model_in_charts(spreadsheet, old, new)
    adapters += _rename_model_in_view_link(spreadsheet, old, new)
    return spreadsheet.data, transform_revisions_data(revisions, *adapters)

def remove_model_in_all_spreadsheets(cr, model):
    apply_in_all_spreadsheets(cr, model, (lambda data, revisions_data: remove_model(model, data, revisions_data)))

def remove_model(model: str, data, revisions = ()) -> str:
    spreadsheet = Spreadsheet(data)
    adapters = _remove_model_from_lists(model, spreadsheet)
    adapters += _remove_model_from_pivots(model, spreadsheet)
    adapters += _remove_model_from_charts(model, spreadsheet)
    adapters += _remove_model_from_filters(model, spreadsheet)
    adapters += _remove_model_from_view_link(model, spreadsheet)
    spreadsheet.clean_empty_cells()
    return spreadsheet.data, transform_revisions_data(revisions, *adapters)

def _rename_model_in_charts(spreadsheet: Spreadsheet, old, new):
    for chart in spreadsheet.odoo_charts:
        if chart.model == old:
            chart.model = new

    def adapt_insert(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            if chart.model == old:
                chart.model = new

    return (CommandAdapter("CREATE_CHART", adapt_insert),)


def _rename_model_in_list(spreadsheet: Spreadsheet, old, new):
    for olist in spreadsheet.lists:
        if olist.model == old:
            olist.model = new

    def adapt_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        if olist.model == old:
            olist.model = new

    return (CommandAdapter("INSERT_ODOO_LIST", adapt_insert),)


def _rename_model_in_pivot(spreadsheet: Spreadsheet, old, new):
    for pivot in spreadsheet.pivots:
        if pivot.model == old:
            pivot.model = new

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if pivot.model == old:
            pivot.model = new

    return (CommandAdapter("INSERT_PIVOT", adapt_insert),)


def _rename_model_in_filters(spreadsheet: Spreadsheet, old, new):
    def rename_relational_filter(gfilter):
        if gfilter["type"] == "relation" and gfilter["modelName"] == old:
            gfilter["modelName"] = new

    for gfilter in spreadsheet.global_filters:
        rename_relational_filter(gfilter)

    def adapt_insert(cmd):
        rename_relational_filter(cmd["filter"])

    return (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_insert),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_insert),
    )


def _rename_model_in_view_link(spreadsheet: Spreadsheet, old, new):
    def adapt_view_link(action):
        if action["modelName"] == old:
            action["modelName"] = new

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _remove_model_from_lists(model, spreadsheet: Spreadsheet):
    lists_to_delete = [list.id for list in spreadsheet.lists if list.model == model]
    return remove_lists(
        spreadsheet,
        lists_to_delete,
        lambda list: list.model == model,
    )


def _remove_model_from_pivots(model, spreadsheet: Spreadsheet):
    pivots_to_delete = [pivot.id for pivot in spreadsheet.pivots if pivot.model == model]
    return remove_pivots(
        spreadsheet,
        pivots_to_delete,
        lambda pivot: pivot.model == model,
    )


def _remove_model_from_charts(model, spreadsheet: Spreadsheet):
    chart_to_delete = [chart.id for chart in spreadsheet.odoo_charts if chart.model == model]
    return remove_odoo_charts(
        spreadsheet,
        chart_to_delete,
        lambda chart: chart.model == model,
    )


def _remove_model_from_filters(model, spreadsheet: Spreadsheet):
    global_filters = spreadsheet.global_filters
    to_delete = [
        gFilter["id"] for gFilter in global_filters if gFilter["type"] == "relation" and gFilter["modelName"] == model
    ]
    spreadsheet.delete_global_filters(*to_delete)

    def adapt_edit_filter(cmd):
        if cmd["filter"]["id"] in to_delete:
            return Drop
        return cmd

    def adapt_add_filter(cmd):
        if cmd["filter"]["type"] == "relation" and cmd["filter"]["modelName"] == model:
            to_delete.append(cmd["filter"]["id"])
            return Drop
        return cmd

    def adapt_remove_filter(cmd):
        if cmd["id"] in to_delete:
            return Drop
        return cmd

    return (
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_add_filter),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_edit_filter),
        CommandAdapter("REMOVE_GLOBAL_FILTER", adapt_remove_filter),
    )


def _remove_model_from_view_link(model, spreadsheet: Spreadsheet):
    def adapt_view_link(action):
        if action["modelName"] == model:
            return Drop

    return adapt_view_link_cells(spreadsheet, adapt_view_link)
