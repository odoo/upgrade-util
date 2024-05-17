import re
import json

from typing import Iterable

from itertools import chain


from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .misc import (
    apply_in_all_spreadsheets,
    adapt_view_link_cells,
    remove_data_source_function,
    remove_lists,
    remove_pivots,
    remove_odoo_charts,
    transform_data_source_functions,
)

from .revisions import CommandAdapter, transform_revisions_data

from odoo.osv import expression

from odoo.upgrade.util.context import adapt_context, clean_context
from odoo.upgrade.util.domains import _adapt_one_domain


# stolen from util.fields:def remove_fields
def remove_adapter(leaf, is_or, negated):
    # replace by TRUE_LEAF, unless negated or in a OR operation but not negated
    if is_or ^ negated:
        return [expression.FALSE_LEAF]
    return [expression.TRUE_LEAF]


def rename_field_in_all_spreadsheets(cr, model, old_value, new_value):
    apply_in_all_spreadsheets(
        cr,
        old_value,
        (lambda data, revisions_data: rename_field(cr, model, old_value, new_value, data, revisions_data)),
    )


def rename_field(cr, model, old, new, data, revisions=()):
    spreadsheet = Spreadsheet(data)
    adapters = _rename_field_in_list(cr, spreadsheet, model, old, new)
    # adapters += _rename_field_in_pivot(cr, spreadsheet, model, old, new)
    # adapters += _rename_field_in_chart(cr, spreadsheet, model, old, new)
    # adapters += _rename_field_in_filters(cr, spreadsheet, model, old, new)
    # adapters += _rename_field_in_view_link(cr, spreadsheet, model, old, new)
    return spreadsheet.data, transform_revisions_data(revisions, *adapters)


def remove_field_in_all_spreadsheets(cr, model, field):
    apply_in_all_spreadsheets(
        cr, field, (lambda data, revisions_data: remove_field(cr, model, field, data, revisions_data))
    )


def remove_field(cr, model, field, data, revisions=()):
    spreadsheet = Spreadsheet(data)
    _remove_field_from_filter_matching(cr, spreadsheet, model, field)
    adapters = _remove_field_from_list(cr, spreadsheet, model, field)
    adapters += _remove_field_from_pivot(cr, spreadsheet, model, field)
    adapters += _remove_field_from_graph(cr, spreadsheet, model, field)
    adapters += _remove_field_from_view_link(cr, spreadsheet, model, field)
    spreadsheet.clean_empty_cells()
    return spreadsheet.data, transform_revisions_data(revisions, *adapters)


def _rename_function_fields(content, data_source_ids, functions, old, new):
    def adapter(fun_call):
        for arg in fun_call.args[1:]:
            if arg.value == old:
                arg.value = new
        return fun_call

    return transform_data_source_functions(content, data_source_ids, functions, adapter)


def _rename_field_in_chain(cr, model, field_model, field_chain, old, new) -> str:
    """Model on which the field chain refers to."""
    domain = [(field_chain, "=", 1)]
    domain = _adapt_one_domain(cr, field_model, old, new, model, domain)
    if domain is None:
        return field_chain
    return domain[0][0]


def _rename_field_in_list(cr, spreadsheet: Spreadsheet, model, old, new):
    list_ids = set()

    def rename(olist):
        _rename_data_source_field(cr, olist, model, old, new)
        if olist.model == model:
            list_ids.add(olist.id)
            olist.fields = _rename_fields(old, new, olist.fields)

    for olist in spreadsheet.lists:
        rename(olist)

    for cell in spreadsheet.cells:
        cell["content"] = _rename_function_fields(
            cell["content"], list_ids, {"ODOO.LIST", "ODOO.LIST.HEADER"}, old, new
        )

    list_models = {olist.id: olist.model for olist in spreadsheet.lists}

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def rename_re_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        if list_models[olist.id] == model:
            olist.fields = _rename_fields(old, new, olist.fields)

    return (
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("INSERT_ODOO_LIST", lambda cmd: rename(create_data_source_from_cmd(cmd))),
        CommandAdapter("RE_INSERT_ODOO_LIST", rename_re_insert),
        CommandAdapter(
            "UPDATE_CELL",
            lambda cmd: dict(
                cmd,
                content=_rename_function_fields(
                    cmd.get("content"), list_ids, {"ODOO.LIST", "ODOO.LIST.HEADER"}, old, new
                ),
            ),
        ),
    )


def _rename_fields(old: str, new: str, fields: Iterable[str]) -> Iterable[str]:
    renamed = []
    for field in fields:
        if ":" in field:
            field, aggregate_operator = field.split(":")
            if field == old:
                renamed.append(new + ":" + aggregate_operator)
            else:
                renamed.append(field)
        elif field == old:
            renamed.append(new)
        else:
            renamed.append(field)
    return renamed


def _rename_field_in_pivot(cr, spreadsheet: Spreadsheet, model, old, new):
    pivot_ids = set()

    def rename(pivot):
        _rename_data_source_field(cr, pivot, model, old, new)
        if pivot.model == model:
            pivot_ids.add(pivot.id)
            pivot.col_group_by = _rename_fields(old, new, pivot.col_group_by)
            pivot.row_group_by = _rename_fields(old, new, pivot.row_group_by)
            pivot.measures = _rename_fields(old, new, pivot.measures)

    for pivot in spreadsheet.pivots:
        rename(pivot)

    for cell in spreadsheet.cells:
        cell["content"] = _rename_function_fields(
            cell["content"], pivot_ids, {"ODOO.PIVOT", "ODOO.PIVOT.HEADER"}, old, new
        )

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        rename(pivot)
        adapt_pivot_table(cmd)

    def adapt_pivot_table(cmd):
        table = cmd["table"]
        for row in table["cols"]:
            for cell in row:
                cell["fields"] = _rename_fields(old, new, cell["fields"])
                # value can be the name of the measure (a field name)
                cell["values"] = _rename_fields(old, new, cell["values"])
        for row in table["rows"]:
            row["fields"] = _rename_fields(old, new, row["fields"])
            row["values"] = _rename_fields(old, new, row["values"])
        cmd["table"]["measures"] = _rename_fields(old, new, table["measures"])

    return (
        CommandAdapter("INSERT_PIVOT", adapt_insert),
        CommandAdapter("RE_INSERT_PIVOT", adapt_pivot_table),
        CommandAdapter(
            "UPDATE_CELL",
            lambda cmd: dict(
                cmd,
                content=_rename_function_fields(
                    cmd.get("content"), pivot_ids, {"ODOO.PIVOT", "ODOO.PIVOT.HEADER"}, old, new
                ),
            ),
        ),
    )


def _rename_data_source_field(cr, data_source, model, old, new):
    data_source.domain = (
        _adapt_one_domain(cr, model, old, new, data_source.model, data_source.domain) or data_source.domain
    )
    for measure in data_source.fields_matching.values():
        measure["chain"] = _rename_field_in_chain(cr, data_source.model, model, measure["chain"], old, new)
    if data_source.model == model:
        adapt_context(data_source.context, old, new)
        if data_source.order_by:
            data_source.order_by = _rename_order_by(data_source.order_by, old, new)


def _remove_data_source_field(cr, data_source, model, field):
    if data_source.model == model:
        data_source.domain = (
            _adapt_one_domain(
                cr, model, field, "ignored", data_source.model, data_source.domain, remove_adapter, force_adapt=True
            )
            or data_source.domain
        )

        adapt_context(data_source.context, field, "ignored")
        if data_source.order_by:
            data_source.order_by = _remove_order_by(data_source.order_by, field)


def _remove_order_by(order_by, field):
    if isinstance(order_by, list):
        return [order for order in order_by if order["field"] != field]
    if order_by and order_by["field"] == field:
        return None
    return order_by

def _rename_order_by(order_by, old, new):
    if isinstance(order_by, list):
        return [_rename_order_by(order, old, new) for order in order_by]
    if order_by and order_by["field"] == old:
        order_by["field"] = new
    return order_by


def _rename_field_in_chart(cr, spreadsheet: Spreadsheet, model, old, new):
    def rename(chart):
        _rename_data_source_field(cr, chart, model, old, new)
        if chart.model == model:
            if chart.measure == old:
                chart.measure = new
            chart.group_by = _rename_fields(old, new, chart.group_by)
        return chart

    for chart in spreadsheet.odoo_charts:
        rename(chart)

    def adapt_create_chart(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            rename(chart)

    return (CommandAdapter("CREATE_CHART", adapt_create_chart),)


def _rename_field_in_filters(cr, spreadsheet: Spreadsheet, model, old, new):
    pivot_models = {pivot.id: pivot.model for pivot in spreadsheet.pivots}
    list_models = {olist.id: olist.model for olist in spreadsheet.lists}
    chart_models = {chart.id: chart.model for chart in spreadsheet.odoo_charts}

    def adapt_filter(cmd):
        for pivot_id, field in cmd["pivot"].items():
            pivot_model = pivot_models[pivot_id]
            field["chain"] = _rename_field_in_chain(cr, pivot_model, model, field["chain"], old, new)
        for list_id, field in cmd["list"].items():
            list_model = list_models[list_id]
            field["chain"] = _rename_field_in_chain(cr, list_model, model, field["chain"], old, new)
        for chart_id, field in cmd["chart"].items():
            chart_model = chart_models[chart_id]
            field["chain"] = _rename_field_in_chain(cr, chart_model, model, field["chain"], old, new)

    def collect_pivot(cmd):
        pivot = create_data_source_from_cmd(cmd)
        pivot_models[pivot.id] = pivot.model

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def collect_charts(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            chart_models[chart.id] = chart.model

    return (
        CommandAdapter("INSERT_PIVOT", collect_pivot),
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("CREATE_CHART", collect_charts),
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_filter),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_filter),
    )


def match_markdown_link(content):
    return re.match(r"\[.*\]\(odoo://view/(.*)\)", content)


def _rename_field_in_view_link(cr, spreadsheet: Spreadsheet, model, old, new):
    def adapt_view_link(action):
        if action["modelName"] != model:
            return
        domain = _adapt_one_domain(cr, model, old, new, action["modelName"], action["domain"])
        if domain:
            if isinstance(action["domain"], str):
                domain = str(domain)
            action["domain"] = domain
        adapt_context(action["context"], old, new)

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


## Removal


def _remove_list_functions(content, list_ids, field):
    """Remove functions such as ODOO.LIST(1, 'field') or ODOO.LIST.HEADER(1, 'field')"""

    def filter_func(func_call_ast):
        return any(arg.value == field for arg in func_call_ast.args[1:])

    return remove_data_source_function(content, list_ids, {"ODOO.LIST", "ODOO.LIST.HEADER"}, filter_func)


def _remove_field_from_list(cr, spreadsheet: Spreadsheet, model, field):
    def _remove_field(olist):
        _remove_data_source_field(cr, olist, model, field)
        if olist.model == model:
            olist.fields = [column for column in olist.fields if column != field]

    for olist in spreadsheet.lists:
        _remove_field(olist)

    def adapt_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        _remove_field(olist)

    # collect all list models inserted by INSERT_ODOO_LIST
    # because we need the models to adapt RE_INSERT_ODOO_LIST
    list_models = {olist.id: olist.model for olist in spreadsheet.lists}

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def adapt_re_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        if list_models[olist.id] == model:
            _remove_field(olist)

    return (
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("INSERT_ODOO_LIST", adapt_insert),
        CommandAdapter("RE_INSERT_ODOO_LIST", adapt_re_insert),
    )


def _remove_field_from_pivot(cr, spreadsheet: Spreadsheet, model, field):
    def _remove_field(pivot):
        _remove_data_source_field(cr, pivot, model, field)
        if pivot.model == model:
            pivot.col_group_by = [f for f in pivot.col_group_by if f != field]
            pivot.row_group_by = [f for f in pivot.row_group_by if f != field]
            pivot.measures = [f for f in pivot.measures if f != field]

    for pivot in spreadsheet.pivots:
        _remove_field(pivot)

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        _remove_field(pivot)

    return (CommandAdapter("INSERT_PIVOT", adapt_insert),)


def _remove_field_from_graph(cr, spreadsheet: Spreadsheet, model, field):
    def _remove_field(chart):
        _remove_data_source_field(cr, chart, model, field)
        if chart.model == model:
            chart.measure = chart.measure if chart.measure != field else None

    for chart in spreadsheet.odoo_charts:
        _remove_field(chart)

    def adapt_create_chart(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            _remove_field(chart)

    return (CommandAdapter("CREATE_CHART", adapt_create_chart),)


def _remove_field_from_view_link(cr, spreadsheet: Spreadsheet, model, field):
    def adapt_view_link(action):
        if action["modelName"] == model:
            clean_context(action["context"], field)
            action["domain"] = _adapt_one_domain(
                cr, model, field, "ignored", model, action["domain"], remove_adapter, force_adapt=True
            )

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _remove_field_from_filter_matching(cr, spreadsheet: Spreadsheet, model, field):
    data_sources = chain(spreadsheet.lists, spreadsheet.pivots, spreadsheet.odoo_charts)
    for data_source in data_sources:
        matching_to_delete = []
        for filter_id, measure in data_source.fields_matching.items():
            if _is_field_in_chain(cr, model, field, data_source.model, measure["chain"]):
                matching_to_delete.append(filter_id)
        for filter_id in matching_to_delete:
            del data_source.fields_matching[filter_id]


def _is_field_in_chain(cr, field_model, field, data_source_model, field_chain):
    def adapter(*args, **kwargs):
        return expression.FALSE_DOMAIN

    domain = [(field_chain, "=", 1)]
    domain = _adapt_one_domain(cr, field_model, field, "ignored", data_source_model, domain, adapter=adapter)
    return domain == expression.FALSE_DOMAIN


def domain_fields(domain):
    """return all field names used in the domain
    >>> domain_fields([['field1', '=', 1], ['field2', '=', 2]])
    ['field1', 'field2']
    """
    return [leaf[0] for leaf in domain if len(leaf) == 3]


def pivot_measure_fields(pivot):
    return [measure for measure in pivot.measures if measure != "__count"]


def pivot_fields(pivot):
    """return all field names used in a pivot definition"""
    fields = set(pivot.col_group_by + pivot.row_group_by + pivot_measure_fields(pivot) + domain_fields(pivot.domain))
    measure = pivot.order_by and pivot.order_by["field"]
    if measure and measure != "__count":
        fields.add(measure)
    return fields


def chart_fields(chart):
    """return all field names used in a chart definitions"""
    fields = set(chart.group_by + domain_fields(chart.domain))
    measure = chart.measure
    if measure != "__count":
        fields.add(measure)
    return fields


def list_order_fields(list_definition):
    return [order["field"] for order in list_definition.order_by]
