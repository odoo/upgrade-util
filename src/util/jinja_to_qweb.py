import contextlib
import functools
import html
import logging
import re
from multiprocessing import Process, Queue

import babel
import lxml
from dateutil import relativedelta
from jinja2.sandbox import SandboxedEnvironment
from markupsafe import Markup
from werkzeug import urls

from odoo import tools
from odoo.tools import is_html_empty, mute_logger, safe_eval

from .helpers import _validate_table, model_of_table
from .orm import env as get_env
from .pg import named_cursor, table_exists
from .report import add_to_migration_reports, html_escape

_logger = logging.getLogger(__name__)

REMOVE_SAFE_REGEX = re.compile(r"\s*\|\s*safe\s*", re.IGNORECASE)

JINJA_EXPRESSION = r"""
\$\{
        (?=(?P<insidebracket>(?:
            [^}\'\"\\]
            |'
                (?=(?P<singlequote>
                    (?:[^'\\]|\\.)*
                ))(?P=singlequote)'?
            |\"
                (?=(?P<doublequote>
                    (?:[^\"\\]|\\.)*
                ))(?P=doublequote)\"?
            |
                (?:\\.)
        )*))(?P=insidebracket)
    }
"""

JINJA_EXPRESSION_REGEX = re.compile(JINJA_EXPRESSION, flags=re.X | re.DOTALL)

JINJA_REGEX = re.compile(
    rf"""
        (?=(?P<string>
        (?:
            (?:\\\\)
            |(?:\\\$)
            |(?:(?!\$\{{).)
        )*
    ))(?P=string)
    (?:{JINJA_EXPRESSION})?
    """,
    re.X | re.DOTALL,
)


def _remove_safe(expression):
    return re.sub(REMOVE_SAFE_REGEX, " ", expression).strip()


def _transform_to_t_out(expression):
    return str(Markup('<t t-out="{}"/>').format(_remove_safe(expression)))


def convert_jinja_to_inline(string):
    result = []
    for element in re.finditer(JINJA_REGEX, string):
        static_string = element.group("string")
        expression = element.group("insidebracket")
        if not static_string and not expression:
            continue

        if static_string:
            result.append(static_string)
        if expression:
            result.append("{{ %s }}" % (_remove_safe(expression),))
    return "".join(result)


def _convert_jinja_to_t_out_text(node):
    comment = isinstance(node, lxml.etree._Comment)
    last_node = None

    index = 0
    for element in re.finditer(JINJA_REGEX, node.text):
        static_string = element.group("string")
        expression = element.group("insidebracket")
        if not static_string and not expression:
            continue

        if last_node is None:
            node.text = static_string
            last_node = node
        elif comment and static_string:
            node.text += static_string
        elif static_string:
            last_node.tail = static_string

        if expression:
            if comment:
                node.text += _transform_to_t_out(expression)
            else:
                new_node = lxml.html.fragment_fromstring(_transform_to_t_out(expression))
                node.insert(index, new_node)
                index += 1
                last_node = new_node


def _convert_jinja_to_t_out_tail(node):
    last_node = node

    for element in re.finditer(JINJA_REGEX, node.tail):
        static_string = element.group("string")
        expression = element.group("insidebracket")
        if not static_string and not expression:
            continue

        last_node.tail = ""
        if expression:
            node = lxml.html.fragment_fromstring(_transform_to_t_out(expression))
            last_node.addnext(node)
        if static_string:
            last_node.tail = static_string
        if expression:
            last_node = node


def convert_jinja_to_qweb(string):
    string = re.sub(r"""^<\?xml version=("|')1\.0\1\?>\s*""", "", string, flags=re.M)
    # Create a parent in case there is multiples root nodes
    element = lxml.html.fragment_fromstring(string, create_parent="div")
    for el in element.getiterator():
        if el.text:
            _convert_jinja_to_t_out_text(el)
        if el.tail:
            _convert_jinja_to_t_out_tail(el)
        for key, value in el.attrib.items():
            if re.search(JINJA_EXPRESSION_REGEX, value):
                del el.attrib[key]
                el.set("t-attf-" + key, convert_jinja_to_inline(value))
    result = lxml.html.tostring(element, encoding="unicode")
    # Remove the technically created parent div, otherwise the first jinja
    # instruction will not match a jinja regex.
    result = result[5:-6]

    for func in [
        _replace_set,
        _replace_for,
        _replace_endfor,
        _replace_if,
        _replace_elif,
        _replace_else,
        _replace_endif,
    ]:
        result = func(result)

    # Make sure the html is correct
    result = lxml.etree.tostring(lxml.html.fragment_fromstring(result, create_parent="div"), encoding="unicode")

    # Remove the parent div
    return result[5:-6]


def _get_set(matchobj):
    return Markup("""{}<t t-set="{}" t-value="{}"/>""").format(
        matchobj.group(1),
        html.unescape(matchobj.group(2).strip()),
        html.unescape(matchobj.group(3).strip()),
    )


def _replace_set(string):
    reg = re.compile(r"^(\s*)%\s*set([^=]*)=(.*)", re.IGNORECASE | re.MULTILINE)
    return reg.sub(_get_set, string)


def _get_for(matchobj):
    return Markup("""{}<t t-foreach="{}" t-as="{}">""").format(
        matchobj.group(1), html.unescape(matchobj.group(3).strip()), html.unescape(matchobj.group(2).strip())
    )


def _replace_for(string):
    reg = re.compile(r"^(\s*)%\s*for((?:(?! in ).)*?) in (.*?):?\s*$", re.IGNORECASE | re.MULTILINE)
    return reg.sub(_get_for, string)


def _replace_endfor(string):
    reg = re.compile(r"^(\s*)%\s*endfor.*", re.IGNORECASE | re.MULTILINE)
    return reg.sub(r"\1</t>", string)


def _get_if(matchobj):
    return Markup("""{}<t t-if="{}">""").format(matchobj.group(1), html.unescape(matchobj.group(2).strip()))


def _replace_if(string):
    reg = re.compile(r"^(\s*)%\s*if(.*?):?\s*$", re.IGNORECASE | re.MULTILINE)
    return reg.sub(_get_if, string)


def _get_elif(matchobj):
    return Markup("""{}</t>\n{}<t t-elif="{}">""").format(
        matchobj.group(1), matchobj.group(1), html.unescape(matchobj.group(2).strip())
    )


def _replace_elif(string):
    reg = re.compile(r"^(\s*)%\s*elif(.*?):?\s*$", re.IGNORECASE | re.MULTILINE)
    return reg.sub(_get_elif, string)


def _replace_else(string):
    reg = re.compile(r"^(\s*)%\s*else.*", re.IGNORECASE | re.MULTILINE)
    return reg.sub(r'\1</t>\n\1<t t-else="">', string)


def _replace_endif(string):
    reg = re.compile(r"^(\s*)%\s*endif.*", re.IGNORECASE | re.MULTILINE)
    return reg.sub(r"\1</t>", string)


def setup_templates_to_check(cr):
    if table_exists(cr, "_upgrade_jinja_to_qweb"):
        return
    cr.execute(
        """
        CREATE UNLOGGED TABLE _upgrade_jinja_to_qweb(
            table_name          varchar,
            template_type       varchar,
            table_id            int4,
            template_field      varchar,
            model_name          varchar,
            template_name       varchar,
            template_name_field varchar,
            template_converted  varchar,
            PRIMARY KEY(table_name, template_type, table_id, template_field)
        )
        """
    )


def insert_templates_to_check(cr, *args):
    cr.execute(
        """
        INSERT INTO _upgrade_jinja_to_qweb (
            table_name,
            template_type,
            table_id,
            template_field,
            model_name,
            template_name,
            template_name_field,
            template_converted
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        args,
    )


def cleanup_templates_to_check(cr):
    cr.execute("DROP TABLE _upgrade_jinja_to_qweb")


def upgrade_jinja_fields(
    cr,
    table_name,
    inline_template_fields,
    qweb_fields,
    name_field="name",
    model_name=None,
    table_model_name="model",
    fetch_model_name=False,
):
    _validate_table(table_name)
    all_field = inline_template_fields + qweb_fields
    if not model_name:
        all_field = [table_model_name] + all_field
    sql_fields = ", ".join(all_field)

    sql_where_inline_fields = [field + " like '%${%'" for field in inline_template_fields]
    sql_where_qweb_fields = [field + r"~ '(\$\{|%\s*(if|for))'" for field in qweb_fields]
    sql_where_fields = " OR ".join(sql_where_inline_fields + sql_where_qweb_fields)

    setup_templates_to_check(cr)
    model = model_of_table(cr, table_name)

    cr.commit()  # ease the processing for PG
    ncr = named_cursor(cr, 100)
    ncr.execute(
        f"""
        SELECT id, {name_field}, {sql_fields}
          FROM {table_name}
         WHERE {sql_where_fields}
        """
    )
    for data in ncr.iterdict():
        _logger.info("process %s(%s) %s", table_name, data["id"], data[name_field])

        # only for mailing.mailing
        if fetch_model_name:
            cr.execute(
                """
                SELECT model FROM ir_model WHERE id=%s
            """,
                [data[table_model_name]],
            )
            model_name = cr.fetchone()[0]
        else:
            model_name = model_name or data[table_model_name]

        # convert the fields
        templates_converted = {}
        for field in inline_template_fields:
            _logger.info(" `- convert inline field %s", field)
            converted = convert_jinja_to_inline(data[field]) if data[field] else ""
            templates_converted[field] = converted
            if data[field]:
                insert_templates_to_check(
                    cr,
                    table_name,
                    "inline_template",
                    data["id"],
                    field,
                    model_name,
                    data[name_field],
                    name_field,
                    converted,
                )

        for field in qweb_fields:
            _logger.info(" `- convert qweb field %s", field)
            converted = convert_jinja_to_qweb(data[field]) if data[field] else ""
            templates_converted[field] = converted
            insert_templates_to_check(
                cr, table_name, "qweb", data["id"], field, model_name, data[name_field], name_field, converted
            )

        fields = [f for f in (inline_template_fields + qweb_fields) if data[f] != templates_converted[f]]
        if fields:
            sql_fields = ",".join([field + "=%s" for field in fields])
            field_values = [templates_converted[field] for field in fields]

            cr.execute(
                f"""
                  UPDATE {table_name}
                     SET {sql_fields}
                   WHERE id = %s
                """,
                field_values + [data["id"]],
            )
    ncr.close()

    if not table_exists(cr, "ir_translation"):
        return

    _logger.info("process translations for model %s", model)
    # NOTE: Not all translations may not be updated.
    # Difference jinja values can give the same qweb value.
    # `${object.company_id.name|safe}` and `${object.company_id.name}` both give `{{ object.company_id.name }}`
    # Which will violates the unique constraint.
    # In this case, we just ignore the update and remove the duplicated row.

    inline_entries = [f"{model},{name}" for name in inline_template_fields]
    if inline_entries:
        cr.execute(
            """
            SELECT id, src, value
              FROM ir_translation
             WHERE name IN %s
               AND (src LIKE '%%${%%' OR value LIKE '%%${%%')
            """,
            [tuple(inline_entries)],
        )
        for tid, src, value in cr.fetchall():
            converted_src = convert_jinja_to_inline(src) if src else ""
            converted_value = convert_jinja_to_inline(value) if value else ""
            cr.execute(
                """
                DELETE FROM ir_translation orig
                      USING ir_translation dup
                      WHERE orig.id = %s
                        AND dup.id != orig.id
                         -- "ir_translation_unique" UNIQUE, btree (type, name, lang, res_id, md5(src))
                        AND dup.type = orig.type
                        AND dup.name = orig.name
                        AND dup.lang = orig.lang
                        AND dup.res_id = orig.res_id
                        AND dup.src = %s
                  RETURNING orig.id
                """,
                [tid, converted_src],
            )
            if not cr.rowcount:
                # no duplicate found, update the translation
                cr.execute(
                    "UPDATE ir_translation SET src=%s, value=%s WHERE id=%s",
                    [converted_src, converted_value, tid],
                )

    cr.commit()  # commit changes for the named cursor below
    qweb_entries = [f"{model},{name}" for name in qweb_fields]
    if qweb_entries:
        ncr = named_cursor(cr, 1000)
        ncr.execute(
            r"""
            SELECT id, src, value
              FROM ir_translation
             WHERE name IN %s
               AND (src ~ '(\$\{|%%\s*(if|for))' OR value ~ '(\$\{|%%\s*(if|for))')
            """,
            [tuple(qweb_entries)],
        )

        for tid, src, value in ncr:
            converted_src = convert_jinja_to_qweb(src) if src else ""
            converted_value = convert_jinja_to_qweb(value) if value else ""
            cr.execute(
                """
                DELETE FROM ir_translation orig
                      USING ir_translation dup
                      WHERE orig.id = %s
                        AND dup.id != orig.id
                         -- "ir_translation_unique" UNIQUE, btree (type, name, lang, res_id, md5(src))
                        AND dup.type = orig.type
                        AND dup.name = orig.name
                        AND dup.lang = orig.lang
                        AND dup.res_id = orig.res_id
                        AND dup.src = %s
                  RETURNING orig.id
                """,
                [tid, converted_src],
            )
            if not cr.rowcount:
                # no duplicate found, update the translation
                cr.execute(
                    "UPDATE ir_translation SET src=%s, value=%s WHERE id=%s",
                    [converted_src, converted_value, tid],
                )
        ncr.close()


def verify_upgraded_jinja_fields(cr):
    env = get_env(cr)
    cr.execute("SELECT DISTINCT(table_name) FROM _upgrade_jinja_to_qweb")
    for (table_name,) in cr.fetchall():
        field_errors = {}
        missing_records = []
        ncr = named_cursor(cr, 100)
        ncr.execute(
            """
              SELECT template_type,
                     table_id,
                     template_field,
                     model_name,
                     template_name,
                     template_name_field,
                     template_converted
                FROM _upgrade_jinja_to_qweb
               WHERE table_name = %s
            ORDER BY template_type,
                     template_field
            """,
            [table_name],
        )
        for (
            template_type,
            table_id,
            template_field,
            model_name,
            template_name,
            template_name_field,
            template_converted,
        ) in ncr:
            if model_name not in env:
                # custom model not loaded yet. Ignore
                continue
            model = env[model_name]
            record = model.with_context({"active_test": False}).search([], limit=1, order="id")

            key = (table_id, template_name, template_name_field)
            field_errors[key] = []

            if not record:
                missing_records.append(key)

            is_valid = is_converted_template_valid(
                env, template_field, template_converted, model_name, record.id, engine=template_type
            )
            if not is_valid:
                field_errors[key].append(template_field)

        if missing_records:
            list_items = "\n".join(
                f'<li>id: "{id}", {html_escape(name_field)}: "{html_escape(name)}" </li>'
                for id, name, name_field in missing_records
            )
            add_to_migration_reports(
                f"""
                    <details>
                        <summary>
                            Some of the records for the table {html_escape(table_name)} could not be tested because there is no
                            record in the database.
                            The {html_escape(table_name)} records are:
                        </summary>
                        <ul>{list_items}</ul>
                    </details>
                """,
                "Jinja upgrade",
                format="html",
            )
        field_errors = dict(filter(lambda x: bool(x[1]), field_errors.items()))

        if field_errors:
            string = []
            for (id, name, name_field), fields in field_errors.items():
                fields_string = "\n".join(f"<li>{html_escape(field)}</li>" for field in fields)
                string.append(
                    f"""<li>id: {id}, {html_escape(name_field)}: {html_escape(name)},
                     fields: <ul>{fields_string}</ul></li>"""
                )

            string = "\n".join(string)
            add_to_migration_reports(
                f"""
                    <details>
                        <summary>
                            Some of the fields of the table {html_escape(table_name)} does not render the same value
                            before and after being converted.
                            The mail.template are:
                        </summary>
                        <ul>{string}</ul>
                    </details>
                """,
                "Jinja upgrade",
                format="html",
            )
        ncr.close()
    cleanup_templates_to_check(cr)


def is_converted_template_valid(env, template_before, template_after, model_name, record_id, engine="inline_template"):
    def callback(q):
        render_before = None
        with contextlib.suppress(Exception):
            render_before = _render_template_jinja(env, template_before, model_name, record_id)

        render_after = None
        if render_before is not None:
            try:
                with mute_logger("odoo.addons.mail.models.mail_render_mixin"):
                    render_after = env["mail.render.mixin"]._render_template(
                        template_after, model_name, [record_id], engine=engine
                    )[record_id]
            except Exception:
                pass

        # post process qweb render to remove comments from the rendered jinja in
        # order to avoid false negative because qweb never render comments.
        if render_before and render_after and engine == "qweb":
            element_before = lxml.html.fragment_fromstring(render_before, create_parent="div")
            for comment_element in element_before.xpath("//comment()"):
                comment_element.getparent().remove(comment_element)
            render_before = lxml.html.tostring(element_before, encoding="unicode")
            render_after = lxml.html.tostring(
                lxml.html.fragment_fromstring(render_after, create_parent="div"), encoding="unicode"
            )

        q.put(render_before is not None and render_before == render_after)

    # to avoid memory leaks in external C libraries (lxml/libxml2), process in a forked child
    queue = Queue()
    proc = Process(target=callback, args=[queue])
    proc.start()
    res = queue.get(timeout=60)
    if proc.is_alive():
        proc.kill()
    return res


# jinja render


def format_date(env, date, pattern=False, lang_code=False):
    try:
        return tools.format_date(env, date, date_format=pattern, lang_code=lang_code)
    except babel.core.UnknownLocaleError:
        return date


def format_datetime(env, dt, tz=False, dt_format="medium", lang_code=False):
    try:
        return tools.format_datetime(env, dt, tz=tz, dt_format=dt_format, lang_code=lang_code)
    except babel.core.UnknownLocaleError:
        return dt


def format_time(env, time, tz=False, time_format="medium", lang_code=False):
    try:
        return tools.format_time(env, time, tz=tz, time_format=time_format, lang_code=lang_code)
    except babel.core.UnknownLocaleError:
        return time


def relativedelta_proxy(*args, **kwargs):
    # dateutil.relativedelta is an old-style class and cannot be directly
    # instantiated within a jinja2 expression, so a lambda "proxy" is
    # is needed, apparently
    return relativedelta.relativedelta(*args, **kwargs)


template_env_globals = {
    "str": str,
    "quote": urls.url_quote,
    "urlencode": urls.url_encode,
    "datetime": safe_eval.datetime,
    "len": len,
    "abs": abs,
    "min": min,
    "max": max,
    "sum": sum,
    "filter": filter,
    "reduce": functools.reduce,
    "map": map,
    "relativedelta": relativedelta_proxy,
    "round": round,
}

jinja_template_env = SandboxedEnvironment(
    block_start_string="<%",
    block_end_string="%>",
    variable_start_string="${",
    variable_end_string="}",
    comment_start_string="<%doc>",
    comment_end_string="</%doc>",
    line_statement_prefix="%",
    line_comment_prefix="##",
    trim_blocks=True,  # do not output newline after blocks
    autoescape=True,  # XML/HTML automatic escaping
)

jinja_template_env.globals.update(template_env_globals)


def _render_template_jinja(env, template_txt, model, res_id):
    if not template_txt:
        return ""

    template = jinja_template_env.from_string(tools.ustr(template_txt))

    record = env[model].browse([res_id])
    variables = {
        "format_date": functools.partial(format_date, env),
        "format_datetime": functools.partial(format_datetime, env),
        "format_time": functools.partial(format_time, env),
        "format_amount": functools.partial(tools.format_amount, env),
        "format_duration": tools.format_duration,
        "user": env.user,
        "ctx": {},
        "is_html_empty": is_html_empty,
        "object": record,
    }

    safe_eval.check_values(variables)
    render_result = template.render(variables)
    if render_result == "False":
        render_result = ""
    return render_result
