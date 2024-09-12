import json
from typing import Iterable, List, Union

from odoo.upgrade.util.misc import version_gte

from .o_spreadsheet import load

"""This file provides (partial) wrappers for reading and writing spreadsheet data
and commands.
The goal is to abstract away the implementation details of the underlying data
structures and how they evolved between versions.
"""

# TODO segregate the data wrappers by version (and use the right one).
# how should should it be done?
# Maybe they could be integrated in odoo source code?
# It could also be useful to have wrappers for validation in odoo (its currently coupled
# to the json file schema).


def create_data_source_from_cmd(cmd):
    if cmd["type"] in ["CREATE_CHART", "UPDATE_CHART"]:
        return OdooChartCmdV16(cmd)
    elif cmd["type"] == "INSERT_PIVOT":
        if version_gte("saas~17.1"):
            return InsertPivotCmdV171(cmd)
        return InsertPivotCmdV16(cmd)
    elif cmd["type"] == "RE_INSERT_PIVOT":
        return InsertPivotCmdV16(cmd)
    elif cmd["type"] == "INSERT_ODOO_LIST":
        return InsertListCmdV16(cmd)
    elif cmd["type"] == "RE_INSERT_ODOO_LIST":
        return ReInsertListCmdV16(cmd)
    return cmd


class Spreadsheet:
    def __init__(self, data: Union[str, dict]):
        if isinstance(data, str):
            data = json.loads(data)
        self.data = load(data)
        self.pivotContructor = version_gte("saas~17.2") and PivotV17_2 or PivotV16

    def __str__(self) -> str:
        return self.to_json()

    def __repr__(self) -> str:
        return self.to_json()

    def to_json(self) -> str:
        return json.dumps(self.data, indent=4)

    @property
    def cells(self) -> Iterable[dict]:
        for sheet in self.data["sheets"]:
            for cell in sheet["cells"].values():
                if cell.get("content"):
                    yield cell

    @property
    def odoo_charts(self):
        for sheet in self.data["sheets"]:
            for figure in sheet["figures"]:
                if _is_odoo_chart(figure):
                    yield OdooChartV16(dict(figure["data"], id=figure["id"]))

    @property
    def pivots(self) -> List[str]:
        return [self.pivotContructor(d) for d in self.data.get("pivots", {}).values()]

    @property
    def lists(self) -> List[str]:
        return [SpreadsheetList(d) for d in self.data.get("lists", {}).values()]

    @property
    def global_filters(self) -> List[str]:
        return self.data.get("globalFilters", [])

    def delete_lists(self, *list_ids: Iterable[str]):
        if "lists" not in self.data:
            return
        for list_id in list_ids:
            del self.data["lists"][list_id]

    def delete_pivots(self, *pivot_ids: Iterable[str]):
        if "pivots" not in self.data:
            return
        for pivot_id in pivot_ids:
            del self.data["pivots"][pivot_id]

    def delete_global_filters(self, *filter_ids: Iterable[str]):
        if "globalFilters" not in self.data:
            return
        filter_ids = set(filter_ids)
        self.data["globalFilters"] = [filter for filter in self.global_filters if filter["id"] not in filter_ids]

    def delete_figures(self, *figure_ids: Iterable[str]):
        figure_ids = set(figure_ids)
        for sheet in self.data["sheets"]:
            sheet["figures"] = [figure for figure in sheet["figures"] if figure["id"] not in figure_ids]

    def clean_empty_cells(self):
        for sheet in self.data["sheets"]:
            sheet["cells"] = {xc: cell for xc, cell in sheet["cells"].items() if cell and cell != {"content": ""}}


def _is_odoo_chart(figure):
    return figure["tag"] == "chart" and figure["data"]["type"].startswith("odoo_")


class DataSource:
    def __init__(self, definition):
        self.definition = definition

    @property
    def id(self):
        return self.definition["id"]

    @property
    def model(self):
        return self.definition["model"]

    @model.setter
    def model(self, model):
        self.definition["model"] = model

    @property
    def domain(self):
        return self.definition["domain"]

    @domain.setter
    def domain(self, domain):
        self.definition["domain"] = domain

    @property
    def context(self):
        return self.definition.get("context")

    @property
    def order_by(self):
        return self.definition.get("orderBy")

    @order_by.setter
    def order_by(self, order_by):
        self.definition["orderBy"] = order_by

    @property
    def fields_matching(self):
        return self.definition.get("fieldMatching", {})


class SpreadsheetList(DataSource):
    @property
    def fields(self) -> List[str]:
        return self.definition["columns"]

    @fields.setter
    def fields(self, fields: List[str]):
        self.definition["columns"] = fields

    @property
    def order_by(self):
        if "searchParams" not in self.definition:
            return []
        order_bys = self.definition["searchParams"].get("orderBy")
        if order_bys:
            return [{"field": order_by["name"], "asc": order_by["asc"]} for order_by in order_bys]
        return []

    @order_by.setter
    def order_by(self, order_bys):
        if not order_bys:
            return
        self.definition["searchParams"]["orderBy"] = [
            {
                "name": order_by["field"],
                "asc": order_by["asc"],
            }
            for order_by in order_bys
        ]


class InsertListCmdV16:
    def __init__(self, cmd):
        self.cmd = cmd
        self.definition = cmd["definition"]

    @property
    def id(self):
        return self.cmd["id"]

    @property
    def model(self):
        return self.definition["metaData"]["resModel"]

    @model.setter
    def model(self, model):
        self.definition["metaData"]["resModel"] = model

    @property
    def domain(self):
        return self.definition["searchParams"]["domain"]

    @domain.setter
    def domain(self, domain):
        self.definition["searchParams"]["domain"] = domain

    @property
    def context(self):
        return self.definition["searchParams"]["context"]

    @property
    def order_by(self):
        order_bys = self.definition["searchParams"].get("orderBy")
        if order_bys:
            return [{"field": order_by["name"], "asc": order_by["asc"]} for order_by in order_bys]
        return []

    @order_by.setter
    def order_by(self, order_bys):
        self.definition["searchParams"]["orderBy"] = [
            {
                "name": order_by["field"],
                "asc": order_by["asc"],
            }
            for order_by in order_bys
        ]

    @property
    def fields_matching(self):
        return {}

    @property
    def fields(self) -> List[str]:
        return self.definition["metaData"]["columns"]

    @fields.setter
    def fields(self, fields: List[str]):
        self.definition["metaData"]["columns"] = fields
        self.cmd["columns"] = [{"name": field} for field in fields]


class ReInsertListCmdV16:
    def __init__(self, cmd):
        self.cmd = cmd

    @property
    def id(self):
        return self.cmd["id"]

    @property
    def fields(self) -> List[str]:
        return [col["name"] for col in self.cmd["columns"]]

    @fields.setter
    def fields(self, fields: List[str]):
        self.cmd["columns"] = [{"name": field} for field in fields]


class PivotV16:
    """Wrapper around a pivot data source, hiding
    its internal structure for v16"""

    def __init__(self, definition):
        self.definition = definition

    @property
    def id(self):
        return self.definition["id"]

    @property
    def model(self):
        return self.definition["model"]

    @model.setter
    def model(self, model):
        self.definition["model"] = model

    @property
    def domain(self):
        return self.definition["domain"]

    @domain.setter
    def domain(self, domain):
        self.definition["domain"] = domain

    @property
    def context(self):
        return self.definition.get("context")

    @property
    def order_by(self):
        sorted_column = self.definition.get("sortedColumn")
        if not sorted_column:
            return
        return {
            "field": sorted_column["measure"],
            "asc": sorted_column["order"].lower() == "asc",
        }

    @order_by.setter
    def order_by(self, order_by):
        sorted_column = {
            "order": "asc" if order_by["asc"] else "desc",
            "measure": order_by["field"],
        }
        self.definition["sortedColumn"].update(sorted_column)

    @property
    def fields_matching(self):
        return self.definition.get("fieldMatching", {})

    @property
    def measures(self):
        return [m["field"] for m in self.definition.get("measures", [])]

    @measures.setter
    def measures(self, measures):
        self.definition["measures"] = [{"field": m} for m in measures]

    @property
    def row_group_by(self):
        return self.definition.get("rowGroupBys", [])

    @row_group_by.setter
    def row_group_by(self, group_by):
        self.definition["rowGroupBys"] = group_by

    @property
    def col_group_by(self):
        return self.definition.get("colGroupBys", [])

    @col_group_by.setter
    def col_group_by(self, group_by):
        self.definition["colGroupBys"] = group_by

class PivotV17_2(PivotV16):
    @property
    def measures(self):
        return self.definition.get("measures", [])

    @measures.setter
    def measures(self, measures):
        self.definition["measures"] = measures

class OdooChartV16:
    def __init__(self, definition):
        self.definition = definition

    @property
    def id(self):
        return self.definition["id"]

    @property
    def model(self):
        return self.definition["metaData"]["resModel"]

    @model.setter
    def model(self, model):
        self.definition["metaData"]["resModel"] = model

    @property
    def domain(self):
        return self.definition["searchParams"]["domain"]

    @domain.setter
    def domain(self, domain):
        self.definition["searchParams"]["domain"] = domain

    @property
    def context(self):
        return self.definition["searchParams"].get("context", {})

    @property
    def order_by(self):
        if self.definition["metaData"].get("order"):
            return {
                "field": self.measure,
                "asc": self.definition["metaData"]["order"].lower() == "asc",
            }

    @order_by.setter
    def order_by(self, order_by):
        self.definition["metaData"]["order"] = "ASC" if order_by["asc"] else "DESC"

    @property
    def fields_matching(self):
        return self.definition.get("fieldMatching", {})

    @property
    def group_by(self):
        return self.definition["metaData"]["groupBy"]

    @group_by.setter
    def group_by(self, group_by):
        self.definition["metaData"]["groupBy"] = group_by

    @property
    def measure(self):
        return self.definition["metaData"].get("measure", [])

    @measure.setter
    def measure(self, measure):
        self.definition["metaData"]["measure"] = measure


class OdooChartCmdV16(OdooChartV16):
    def __init__(self, cmd):
        super().__init__(cmd["definition"])
        self.cmd = cmd

    @property
    def id(self):
        return self.cmd["id"]


class InsertPivotCmdV16:
    """Wrapper around an INSERT_PIVOT command, hiding
    the internal structure of the command."""

    def __init__(self, cmd):
        self.cmd = cmd
        self.definition = cmd["definition"]

    @property
    def id(self):
        return self.cmd["id"]

    @property
    def model(self):
        return self.definition["metaData"]["resModel"]

    @model.setter
    def model(self, model):
        self.definition["metaData"]["resModel"] = model

    @property
    def domain(self):
        return self.definition["searchParams"]["domain"]

    @domain.setter
    def domain(self, domain):
        self.definition["searchParams"]["domain"] = domain

    @property
    def context(self):
        return self.definition["searchParams"]["context"]

    @property
    def order_by(self):
        sorted_column = self.definition["metaData"].get("sortedColumn")
        if not sorted_column:
            return
        return {
            "field": sorted_column["measure"],
            "asc": sorted_column["order"].lower() == "asc",
        }

    @order_by.setter
    def order_by(self, order_by):
        sorted_column = {
            "order": "asc" if order_by["asc"] else "desc",
            "measure": order_by["field"],
        }
        self.definition["metaData"]["sortedColumn"].update(sorted_column)

    @property
    def fields_matching(self):
        return {}

    @property
    def measures(self):
        return self.definition["metaData"]["activeMeasures"]

    @measures.setter
    def measures(self, measures):
        self.definition["metaData"]["activeMeasures"] = measures

    @property
    def row_group_by(self):
        return self.definition["metaData"]["rowGroupBys"]

    @row_group_by.setter
    def row_group_by(self, group_by):
        self.definition["metaData"]["rowGroupBys"] = group_by

    @property
    def col_group_by(self):
        return self.definition["metaData"]["colGroupBys"]

    @col_group_by.setter
    def col_group_by(self, group_by):
        self.definition["metaData"]["colGroupBys"] = group_by


class InsertPivotCmdV171(InsertPivotCmdV16):
    @property
    def id(self):
        return self.cmd["pivotId"]
