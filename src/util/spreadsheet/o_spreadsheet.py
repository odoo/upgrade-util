import math
import re


INITIAL_SHEET_ID = "Sheet1"
DEFAULT_REVISION_ID = "START_REVISION"


def load(data=None) -> dict:
    """
    Load spreadsheet data and try to fix missing fields/corrupted state by providing
    sensible default values
    """

    if not data:
        return _create_empty_workbook()
    data = {**_create_empty_workbook(), **data}
    data["sheets"] = [
        {**_create_empty_sheet("Sheet{}".format(i + 1), "Sheet{}".format(i + 1)), **sheet}
        for i, sheet in enumerate(data["sheets"])
    ]
    if not data["sheets"]:
        data["sheets"].append(_create_empty_sheet(INITIAL_SHEET_ID, "Sheet1"))
    return data


def _create_empty_sheet(sheet_id: str, name: str) -> dict:
    return {
        "id": sheet_id,
        "name": name,
        "cells": {},
        "figures": [],
        "isVisible": True,
    }


def _create_empty_workbook(sheet_name: str = "Sheet1") -> dict:
    return {
        "sheets": [_create_empty_sheet(INITIAL_SHEET_ID, sheet_name)],
        "styles": {},
        "formats": {},
        "borders": {},
        "revisionId": DEFAULT_REVISION_ID,
    }


# def upgrade_data(data, upgrade_functions, to_version, version_field="version"):
#     data = load(data)
#     upgrade_functions.sort(key=lambda x: x[0])
#     for upgrade_version, upgrade in upgrade_functions:
#         if data.get(version_field, 0) < upgrade_version:
#             upgrade(data)
#             data[version_field] = upgrade_version
#         if upgrade_version == to_version:
#             return data
#     return data


# Reference of a column header (eg. A, AB)
col_header = re.compile(r"^([A-Z]{1,3})+$")
cell_reference = r"\$?([A-Z]{1,3})\$?([0-9]{1,7})"
# Reference of a normal range or a full row range (eg. A1:B1, 1:$5, $A2:5)
full_row_xc = r"(\$?[A-Z]{1,3})?\$?[0-9]{1,7}\s*:\s*(\$?[A-Z]{1,3})?\$?[0-9]{1,7}\s*"
# Reference of a normal range or a column row range (eg. A1:B1, A:$B, $A1:C)
full_col_xc = r"\$?[A-Z]{1,3}(\$?[0-9]{1,7})?\s*:\s*\$?[A-Z]{1,3}(\$?[0-9]{1,7})?\s*"
# Reference of a cell or a range, it can be a bounded range, a full row or a full column
range_reference = re.compile(r"^\s*('.+'!|[^']+!)?" + "({}|{}|{})$".format(cell_reference, full_row_xc, full_col_xc))
# Reference of a column (eg. A, $CA, Sheet1!B)
col_reference = re.compile(r"^\s*('.+'!|[^']+!)?\$?([A-Z]{1,3})$")
# Reference of a row (eg. 1, 59, Sheet1!9)
row_reference = re.compile(r"^\s*('.+'!|[^']+!)?\$?([0-9]{1,7})$")


cell_reference = re.compile(r"\$?([A-Z]{1,3})\$?([0-9]{1,7})")


def zone_to_xc(zone) -> str:
    top = zone["top"]
    bottom = zone["bottom"]
    left = zone["left"]
    right = zone["right"]

    has_header = zone.get("hasHeader", False)
    is_one_cell = top == bottom and left == right

    if bottom is None and right is not None:
        return f"{number_to_letters(left)}:{number_to_letters(right)}" if top == 0 and not has_header else f"{to_xc(left, top)}:{number_to_letters(right)}"
    elif right is None and bottom is not None:
        return f"{top + 1}:{bottom + 1}" if left == 0 and not has_header else f"{to_xc(left, top)}:{bottom + 1}"
    elif bottom is not None and right is not None:
        return to_xc(left, top) if is_one_cell else f"{to_xc(left, top)}:{to_xc(right, bottom)}"

    raise ValueError("Bad zone format")

def letters_to_number(letters):
    """
    Convert a string (describing a column) to its number value.
    >>> letters_to_number("A")
    0
    >>> letters_to_number("Z")
    25
    >>> letters_to_number("AA")
    26
    """
    result = 0
    length = len(letters)
    for i in range(length):
        n = ord(letters[i]) - 65 + (i < length - 1)
        result += n * 26 ** (length - i - 1)
    return result


def number_to_letters(n):
    """
    >>> number_to_letters(0)
    'A'
    >>> number_to_letters(1)
    'B'
    """
    if n < 0:
        raise ValueError(f"number must be positive. Got {n}")
    if n < 26:
        return chr(65 + n)
    else:
        return number_to_letters(math.floor(n / 26) - 1) + number_to_letters(n % 26)


def to_cartesian(xc):
    """Convert a cell reference to a cartesian coordinate
    >>> to_cartesian("A1")
    {'col': 0, 'row': 0}
    >>> to_cartesian("B2")
    {'col': 1, 'row': 1}
    """
    xc = xc.upper().strip()
    match = cell_reference.match(xc)
    if match:
        (letters, numbers) = match.groups()
        col = letters_to_number(letters)
        row = int(numbers) - 1
        return {"col": col, "row": row}
    raise ValueError(f"Invalid cell description: {xc}")


def is_col_reference(xc):
    return col_reference.match(xc)


def is_row_reference(xc):
    return row_reference.match(xc)


def is_col_header(str):
    return col_header.match(str)


def to_zone_without_boundary_changes(xc):
    xc = xc.split("!").pop()
    ranges = [x.strip() for x in xc.replace("$", "").split(":")]
    full_col = full_row = has_header = False
    first_range_part = ranges[0]
    second_range_part = len(ranges) == 2 and ranges[1]
    if is_col_reference(first_range_part):
        left = right = letters_to_number(first_range_part)
        top = bottom = 0
        full_col = True
    elif is_row_reference(first_range_part):
        top = bottom = int(first_range_part) - 1
        left = right = 0
        full_row = True
    else:
        c = to_cartesian(first_range_part)
        left = right = c["col"]
        top = bottom = c["row"]
        has_header = True
    if len(ranges) == 2:
        if is_col_reference(second_range_part):
            right = letters_to_number(second_range_part)
            full_col = True
        elif is_row_reference(second_range_part):
            bottom = int(second_range_part) - 1
            full_row = True
        else:
            c = to_cartesian(second_range_part)
            right = c["col"]
            bottom = c["row"]
            if full_col:
                top = bottom
            if full_row:
                left = right
            has_header = True
    if full_col and full_row:
        raise ValueError("Wrong zone xc. The zone cannot be at the same time a full column and a full row")
    zone = {
        "top": top,
        "left": left,
        "bottom": None if full_col else bottom,
        "right": None if full_row else right,
    }
    has_header = has_header and (full_row or full_col)
    if has_header:
        zone["has_header"] = has_header
    return zone


def to_unbounded_zone(xc):
    zone = to_zone_without_boundary_changes(xc)
    if zone["right"] is not None and zone["right"] < zone["left"]:
        zone["left"], zone["right"] = zone["right"], zone["left"]
    if zone["bottom"] is not None and zone["bottom"] < zone["top"]:
        zone["top"], zone["bottom"] = zone["bottom"], zone["top"]
    return zone


def to_zone(xc):
    """Convert from a cartesian reference to a zone.
    >>> to_zone("A1")
    {'top': 0, 'left': 0, 'bottom': 0, 'right': 0}
    >>> to_zone("B1:B3")
    {'top': 0, 'left': 1, 'bottom': 2, 'right': 1}
    >>> to_zone("Sheet1!A1")
    {'top': 0, 'left': 0, 'bottom': 0, 'right': 0}
    """
    zone = to_unbounded_zone(xc)
    if zone["bottom"] is None or zone["right"] is None:
        raise ValueError("This does not support unbounded ranges")
    return zone


def to_xc(col, row):
    """
    >>> to_xc(0, 0)
    'A1'
    >>> to_xc(1, 1)
    'B2'
    """
    return number_to_letters(col) + str(row + 1)


def overlap(z1, z2) -> bool:
    if z1["bottom"] < z2["top"] or z2["bottom"] < z1["top"]:
        return False
    if z1["right"] < z2["left"] or z2["right"] < z1["left"]:
        return False
    return True