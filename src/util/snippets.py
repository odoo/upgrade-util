# -*- coding: utf-8 -*-
import logging
import re

from lxml import etree, html
from psycopg2.extensions import quote_ident
from psycopg2.extras import Json

from odoo.upgrade import util

_logger = logging.getLogger("odoo.upgrade.web_editor.132." + __name__)
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
    cr.execute(
        """
        SELECT f.model, f.name
          FROM ir_model_fields f
          JOIN ir_model m ON m.id = f.model_id
         WHERE f.ttype = 'html'
           AND f.store = true
           AND m.transient = false
           AND f.model NOT LIKE 'ir.actions%'
           AND f.model != 'mail.message'
    """
    )
    for model, column in cr.fetchall():
        table = util.table_of_model(cr, model)
        if util.table_exists(cr, table) and util.column_exists(cr, table, column):
            yield table, quote_ident(column, cr._cnx)


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


def html_converter(transform_callback):
    """
    Creates an upgrade converter for a single HTML text content.

    :param func transform_callback: transforms an HTML tree and returns True if
        a change happened
    :return: func conversion callback
    """

    def converter(content):
        """
        Converts content.

        :param str content: content to convert
        :return: A tuple:
            1) A boolean which indicates if the content changed
            2) The updated content
        """

        # Remove `<?xml ...>` header
        content = re.sub(r"^<\?xml .+\?>\s*", "", content.strip())
        # Wrap in <wrap> node before parsing to preserve external comments and
        # multi-root nodes
        els = html.fromstring(f"<wrap>{content}</wrap>", parser=utf8_parser)
        has_changed = transform_callback(els)
        new_content = (
            re.sub(r"(^<wrap>|</wrap>$)", "", etree.tostring(els, encoding="unicode").strip())
            if has_changed
            else content
        )
        return (has_changed, new_content)

    return converter


def html_selector_converter(transform_callback, selector=None):
    """
    Creates an upgrade converter for HTML elements that match a selector.

    :param func transform_callback: transforms a selected HTML element and
        returns True if a change happened
    :param str selector: targets the elements to loop on
    :return: func conversion callback
    """

    def looper(root_el):
        return any([transform_callback(el) for el in root_el.xpath(selector)])

    return html_converter(looper if selector else transform_callback)


def convert_html_content(
    cr,
    converter_callback,
    where_column="IS NOT NULL",
    maximum_batch_size=10000,
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
    :param int maximum_batch_size: Maximum number of rows to fetch at once
        before migrating them
    """

    def convert_column(cr, table, column, extra_where=""):
        jsonb_column = util.column_type(cr, table, column.strip('"')) == "jsonb"
        where_column_expression = column
        if jsonb_column:
            where_column_expression = f"{where_column_expression}->>'en_US'"
        base_select_query = f"""
            SELECT id, {column}
            FROM {table}
            WHERE {where_column_expression} {where_column}
            {extra_where}
        """
        update_query = f"""
            UPDATE {table}
            SET {column} = %s
            WHERE id = %s
        """

        select_queries = (
            (base_select_query,)
            if maximum_batch_size is None
            else util.explode_query_range(cr, base_select_query, table=table, bucket_size=maximum_batch_size)
        )
        for select_query in select_queries:
            cr.execute(select_query)
            for res_id, content in cr.fetchall():
                if jsonb_column and content:
                    new_content = {}
                    has_changed, new_content["en_US"] = converter_callback(content.pop("en_US"))
                    if has_changed:
                        for lang, value in content.items():
                            _, new_content[lang] = converter_callback(value)
                        new_content = Json(new_content)
                else:
                    has_changed, new_content = converter_callback(content)
                if has_changed:
                    cr.execute(update_query, [new_content, res_id])

    convert_column(cr, "ir_ui_view", "arch_db", "AND type = 'qweb'")
    for table, column in get_html_fields(cr):
        convert_column(cr, table, column)
