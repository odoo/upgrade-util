# -*- coding: utf-8 -*-
import inspect
import logging
import re
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor

from lxml import etree, html
from psycopg2.extensions import quote_ident
from psycopg2.extras import Json

from .exceptions import MigrationError
from odoo.upgrade import util

_logger = logging.getLogger(__name__)
utf8_parser = html.HTMLParser(encoding="utf-8")


class Snippet:
    def __init__(self, name, tag="*", klass="", selector=""):
        self.name = name
        self.tag = tag
        self.klass = klass or name
        self.selector = selector or f'//{tag}[hasclass("{self.klass}")]'


def add_snippet_names(cr, table, column, snippets, select_query):
    """
    Execute the select_query then for each snippet contained in arch add the right data-snippet
    attribute on the right element.

    :param str table: The table we are working on
    :param str column: The column we are working on
    :param list snippets: list of all snippets to migrate
    :param str select_query: a query that when executed will return (id, list of snippets contained in the arch, arch)
    """
    _logger.info("Add snippet names on %s.%s", table, column)
    cr.execute(select_query)

    it = util.log_progress(cr.fetchall(), _logger, qualifier="rows", size=cr.rowcount, log_hundred_percent=True)

    for res_id, regex_matches, arch in it:
        regex_matches = [match[0] for match in regex_matches]
        arch = arch.replace("\r", "")  # otherwise html parser below will transform \r -> &#13;
        body = html.fromstring(arch, parser=utf8_parser)
        changed = False
        for snippet in snippets:
            if snippet.klass in regex_matches:
                body_snippets = body.xpath(snippet.selector)
                for body_snippet in body_snippets:
                    body_snippet.attrib["data-snippet"] = snippet.name
                    changed = True
        if changed:
            body = etree.tostring(body, encoding="unicode")
            cr.execute(f"UPDATE {table} SET {column} = %s WHERE id = %s", [body, res_id])


def add_snippet_names_on_html_field(cr, table, column, snippets, regex):
    """
    Will search for all the snippets in the fields mentionned (should be html fields) and add
    the corresponding data-snippet on them.
    """
    query = cr.mogrify(
        f"""
            SELECT id, array((SELECT regexp_matches({column}, %(regex)s, 'g'))), {column}
              FROM {table}
             WHERE {column} ~ %(regex)s
        """,
        dict(regex=regex),
    ).decode()
    bytes_per_chunk = 1024 * 1024 * 200
    cr.execute(
        cr.mogrify(
            f"""
            WITH info AS (
                SELECT id,
                       sum(pg_column_size(({column}, id))) OVER (ORDER BY id) / {bytes_per_chunk} AS chunk
                  FROM {table}
                 WHERE {column} ~ %(regex)s
            ) SELECT min(id), max(id) FROM info GROUP BY chunk
            """,
            dict(regex=regex),
        ).decode()
    )
    for id0, id1 in cr.fetchall():
        add_snippet_names(cr, table, column, snippets, query + f" AND id BETWEEN {id0} AND {id1}")


def get_regex_from_snippets_list(snippets):
    return "(%s)" % "|".join(snippet.klass for snippet in snippets)


def get_html_fields(cr):
    # yield (table, column) of stored html fields (that needs snippets updates)
    for table, columns in html_fields(cr):
        for column in columns:
            yield table, quote_ident(column, cr._cnx)


def html_fields(cr):
    cr.execute(
        """
        SELECT f.model, array_agg(f.name)
          FROM ir_model_fields f
          JOIN ir_model m ON m.id = f.model_id
         WHERE f.ttype = 'html'
           AND f.store = true
           AND m.transient = false
           AND f.model NOT LIKE 'ir.actions%'
           AND f.model != 'mail.message'
      GROUP BY f.model
    """
    )
    for model, columns in cr.fetchall():
        table = util.table_of_model(cr, model)
        if not util.table_exists(cr, table):
            # an SQL VIEW
            continue
        columns = [column for column in columns if util.column_exists(cr, table, column)]
        if columns:
            yield table, columns


def parse_style(attr):
    """
    Converts an HTML style attribute's text into a dict mapping property names to property values.

    :param str attr: value of an HTML style attribute
    :return: dict of CSS property values per property name
    """
    # Captures two groups:
    # - identifier: sequence of word character or hyphen that is followed by a colon
    # - value: sequence of:
    #   - any non semicolon character or
    #   - sequence of any non single quote character or escaped single quote
    #     surrounded by single quotes or
    #   - sequence of any non double quote character or escaped double quote
    #     surrounded by double quotes
    regex = r"""
        ([\w\-]+)\s*:\s*((?:[^;\"']|'(?:[^']|(?:\\'))*'|\"(?:[^\"]|(?:\\\"))*\")+)
    """.strip()
    return dict(re.findall(regex, attr))


def format_style(styles):
    """
    Converts a dict of CSS property names to property values into an HTML style attribute string.

    :param dict styles: CSS property value per property name
    :return: str HTML style attribute
    """
    style = "; ".join(["%s: %s" % entry for entry in styles.items()])
    if len(style) > 0 and style[-1] != ";":
        style += ";"
    return style


def html_converter(transform_callback, selector=None):
    """
    Creates an upgrade converter for a single HTML text content or for HTML elements
    that match a selector.

    :param func transform_callback: transforms an HTML tree and returns True if
        a change happened
    :param str selector: targets the elements to loop on
    :return: object HTMLConverter with callback
    """
    return HTMLConverter(transform_callback, selector)


def make_pickleable_callback(callback):
    """`ProcessPoolExecutor.map` arguments needs to be pickleable
    Functions can only be pickled if they are importable.
    However, the callback's file is not importable due to the dash in the filename.
    We should then put the executed function in its own importable file.
    """
    callback_filepath = inspect.getfile(callback)
    name = f"_upgrade_{uuid.uuid4().hex}"
    mod = sys.modules[name] = util.import_script(callback_filepath, name=name)
    try:
        return getattr(mod, callback.__name__)
    except AttributeError:
        error_msg = (
            f"The converter callback `{callback.__name__}` is a nested function in `{callback.__module__}`.\n"
            "Move it outside the `migrate()` function to make it top-level."
        )
        raise MigrationError(error_msg) from None


class HTMLConverter:
    def __init__(self, callback, selector=None):
        self.selector = selector
        self.callback = make_pickleable_callback(callback)

    def has_changed(self, els):
        if self.selector:
            return any([self.callback(el) for el in els.xpath(self.selector)])
        return self.callback(els)

    def __call__(self, content):
        # Remove `<?xml ...>` header
        if not content:
            return (False, content)
        content = re.sub(r"^<\?xml .+\?>\s*", "", content.strip())
        # Wrap in <wrap> node before parsing to preserve external comments and
        # multi-root nodes
        els = html.fromstring(f"<wrap>{content}</wrap>", parser=utf8_parser)
        has_changed = self.has_changed(els)
        new_content = (
            re.sub(r"(^<wrap>|</wrap>$)", "", etree.tostring(els, encoding="unicode").strip())
            if has_changed
            else content
        )
        return (has_changed, new_content)


class Convertor:
    def __init__(self, converters, callback):
        self.converters = converters
        self.callback = callback

    def __call__(self, row):
        converters = self.converters
        columns = self.converters.keys()
        converter_callback = self.callback
        res_id, *contents = row
        changes = {}
        for column, content in zip(columns, contents):
            if content and converters[column]:
                # jsonb column; convert all keys
                new_content = {}
                has_changed, new_content["en_US"] = converter_callback(content.pop("en_US"))
                if has_changed:
                    for lang, value in content.items():
                        _, new_content[lang] = converter_callback(value)
                new_content = Json(new_content)
            else:
                has_changed, new_content = converter_callback(content)
            changes[column] = new_content
            if has_changed:
                changes["id"] = res_id
        return changes


def convert_html_columns(cr, table, columns, converter_callback, where_column="IS NOT NULL", extra_where="true"):
    """
    Converts HTML content for the given table column.

    :param cursor cr: database cursor
    :param str table: table name
    :param str column: column name
    :param func converter_callback: conversion function that converts the HTML
        text content and returns a tuple with a boolean that indicates whether a
        change happened and the new content must be saved
    :param str where_column: filtering such as
        - "like '%abc%xyz%'"
        - "~* '\\yabc.*xyz\\y'"
    :param str extra_where: extra filtering on the where clause
    """

    assert "id" not in columns

    converters = {column: "->>'en_US'" if util.column_type(cr, table, column) == "jsonb" else "" for column in columns}
    select = ", ".join(f'"{column}"' for column in columns)
    where = " OR ".join(f'"{column}"{converters[column]} {where_column}' for column in columns)

    base_select_query = f"""
        SELECT id, {select}
          FROM {table}
         WHERE ({where})
           AND ({extra_where})
    """

    split_queries = util.explode_query_range(cr, base_select_query, table=table)

    update_sql = ", ".join(f'"{column}" = %({column})s' for column in columns)
    update_query = f"UPDATE {table} SET {update_sql} WHERE id = %(id)s"

    with ProcessPoolExecutor() as executor:
        convert = Convertor(converters, converter_callback)
        for query in util.log_progress(split_queries, logger=_logger, qualifier=f"{table} updates"):
            cr.execute(query)
            for data in executor.map(convert, cr.fetchall()):
                if "id" in data:
                    cr.execute(update_query, data)


def convert_html_content(
    cr,
    converter_callback,
    where_column="IS NOT NULL",
    **kwargs,
):
    """
    Converts HTML content.

    :param cursor cr: database cursor
    :param func converter_callback: conversion function that converts the HTML
        text content and returns a tuple with a boolean that indicates whether a
        change happened and the new content must be saved
    :param str where_column: filtering such as
        - "like '%abc%xyz%'"
        - "~* '\\yabc.*xyz\\y'"
    :param dict kwargs: extra keyword arguments to pass to :func:`convert_html_column`
    """

    convert_html_columns(
        cr,
        "ir_ui_view",
        ["arch_db"],
        converter_callback,
        where_column=where_column,
        **dict(kwargs, extra_where="type = 'qweb'"),
    )

    for table, columns in html_fields(cr):
        convert_html_columns(cr, table, columns, converter_callback, where_column=where_column, **kwargs)
