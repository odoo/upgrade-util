# -*- coding: utf-8 -*-
import logging
import os
import re
import sys
from textwrap import dedent

import lxml
from docutils.core import publish_string

try:
    import markdown
except ImportError:
    markdown = None

from .helpers import _validate_model
from .misc import parse_version

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = unicode = str

try:
    from markupsafe import Markup, escape

    from odoo.tools.misc import html_escape

    if html_escape is not escape:
        Markup = None
except ImportError:
    Markup = None

try:
    try:
        from odoo.api import SUPERUSER_ID
    except ImportError:
        from odoo import SUPERUSER_ID
    from odoo import release
    from odoo.tools.mail import html_sanitize
except ImportError:
    from openerp import SUPERUSER_ID, release
    from openerp.tools.mail import html_sanitize


if sys.version_info > (3,):
    from odoo.tools import html_escape
else:
    # In python2, `html_escape` always returns a byte-string with non-ascii characters replaced
    # by their html entities.

    import werkzeug.utils

    # Avoid DeprecationWarning while still remaining compatible with werkzeug pre-0.9
    if parse_version(getattr(werkzeug, "__version__", "0.0")) < parse_version("0.9.0"):

        def html_escape(text):
            return werkzeug.utils.escape(text, quote=True).encode("ascii", "xmlcharrefreplace")

    else:

        def html_escape(text):
            return werkzeug.utils.escape(text).encode("ascii", "xmlcharrefreplace")


try:
    from odoo.addons.base.models.ir_module import MyWriter  # > 11.0
except ImportError:
    try:
        from odoo.addons.base.module.module import MyWriter
    except ImportError:
        from openerp.addons.base.module.module import MyWriter

from .exceptions import MigrationError
from .misc import has_enterprise, split_osenv, version_between, version_gte
from .orm import env, get_admin_channel, guess_admin_id

migration_reports = {}
_logger = logging.getLogger(__name__)


_ENV_AM = set(split_osenv("UPG_ANNOUNCE_MEDIA", default="discuss"))
ANNOUNCE_MEDIA = _ENV_AM & {"", "discuss", "logger"}
if _ENV_AM - ANNOUNCE_MEDIA:
    raise ValueError(
        "Invalid value for the environment variable `UPG_ANNOUNCE_MEDIA`: {!r}. "
        "Authorized values are a combination of 'discuss', 'logger', or an empty string.".format(
            os.getenv("UPG_ANNOUNCE_MEDIA")
        )
    )
ANNOUNCE_MEDIA -= {""}


ODOO_SHOWCASE_VIDEOS = {
    "saas~19.1": "jk1CANZ82lU",
    "19.0": "OZLP-SCHW7A",
    "saas~18.4": "fiPyJXzeNjQ",
    "saas~18.3": "oyev2DxC5yY",
    "saas~18.2": "bwn_HWuLuTA",
    "saas~18.1": "is9oLyIkQGk",
    "18.0": "gbE3azm_Io0",
    "saas~17.4": "8F4-uDwom8A",
    "saas~17.2": "ivjgo_2-wkE",
    "17.0": "qxb74CMR748",
    "16.0": "RVFZL3D9plg",
}


def report(message, category="Other", format="text"):
    assert format in {"text", "html", "md", "rst"}
    if format == "md":
        message = md2html(dedent(message))
    elif format == "rst":
        message = rst2html(message)
    raw = False
    if format != "text":
        if Markup:
            message = Markup(message)
        else:
            raw = True
    migration_reports.setdefault(category, []).append((message, raw))
    migration_reports_length = sum(len(msg) for reps in migration_reports.values() for msg, _ in reps) + sum(
        map(len, migration_reports)
    )
    if migration_reports_length > 1000000:
        _logger.warning("Upgrade report is growing suspiciously long: %s characters so far.", migration_reports_length)


add_to_migration_reports = report


def report_with_summary(summary, details, category="Other"):
    """Append the upgrade report with a new entry.

    :param str summary: Description of a report entry.
    :param str details: Detailed description that is going to be folded by default.
    :param str category: Title of a report entry.
    """
    msg = (
        "<summary>{}<details>{}</details></summary>".format(summary, details)
        if details
        else "<summary>{}</summary>".format(summary)
    )
    report(message=msg, category=category, format="html")
    return msg


def report_with_list(summary, data, columns, row_format, links=None, total=None, limit=100, category="Other"):
    """Append the upgrade report with a new entry that displays a list of records.

    The entry consists of a category (title) and a summary (body).
    The entry displays a list of records previously returned by SQL query, or any list.

    .. example::

        .. code-block:: python

            total = cr.rowcount
            data = cr.fetchmany(20)
            util.report_with_list(
                summary="The following records were altered.",
                data=data,
                columns=("id", "name", "city", "comment", "company_id", "company_name"),
                row_format="Partner with id {partner_link} works at company {company_link} in {city}, ({comment})",
                links={"company_link": ("res.company", "company_id", "company_name"), "partner_link": ("res.partner", "id", "name")},
                total=total,
                category="Accounting"
            )

    :param str summary: description of a report entry.
    :param list(tuple) data: data to report, each entry would be a row in the report.
                             It could be empty, in which case only the summary is rendered.
    :param tuple(str) columns: columns in `data`, can be referenced in `row_format`.
    :param str row_format: format for rows, can use any name from `columns` or `links`, e.g.:
                           "Partner {partner_link} that lives in {city} works at company {company_link}."
    :param dict(str, tuple(str, str, str)) links: optional model/record links spec,
                                                  the keys can be referenced in `row_format`.
    :param int total: optional, total number of records.
                      Taken as `len(data)` when `None` is passed.
                      Useful when `data` was limited by the caller.
    :param int limit: maximum number of records to list in the report.
                      If `data` contains more records than `limit`, the `total`
                      number would be included in the report as well.
                      Set `-1` for no limit.
    :param str category: title of a report entry.
    """

    def row_to_html(row):
        row_dict = dict(zip(columns, row))
        if links:
            row_dict.update(
                {
                    link: get_anchor_link_to_record(rec_model, row_dict[id_col], row_dict[name_col])
                    for link, (rec_model, id_col, name_col) in links.items()
                }
            )
        return "<li>{}</li>".format(row_format.format(**row_dict))

    if not data:
        row_to_html(columns)  # Validate the format is correct, including links
        return report_with_summary(summary=summary, details="", category=category)

    disclaimer = "The total number of affected records is {}. ".format(total) if total else ""
    total = len(data) if total is None else total
    limit = min(limit, total) if limit != -1 else total
    if total > limit:
        disclaimer += "This list is showing the first {} records.".format(limit)

    rows = "<ul>\n" + "\n".join([row_to_html(row) for row in data[:limit]]) + "\n</ul>"
    return report_with_summary(summary, "<i>{}</i>{}".format(disclaimer, rows), category)


def announce_release_note(cr):
    filepath = os.path.join(os.path.dirname(__file__), "release-note.xml")
    with open(filepath, "rb") as fp:
        contents = fp.read()
        if not version_gte("15.0"):
            contents = contents.replace(b"t-out", b"t-esc")
        report = lxml.etree.fromstring(contents)
    e = env(cr)
    major_version, minor_version = re.findall(r"\d+", release.major_version)
    values = {
        "version": release.major_version,
        "major_version": major_version,
        "minor_version": minor_version,
        "odoo_showcase_video_id": ODOO_SHOWCASE_VIDEOS.get(release.major_version, ""),
    }
    _logger.info("Rendering release note for version %s", release.version)
    render = e["ir.qweb"].render if hasattr(e["ir.qweb"], "render") else e["ir.qweb"]._render
    message = render(report, values=values)
    _announce_to_db(cr, message, to_admin_only=False)


def announce_migration_report(cr):
    filepath = os.path.join(os.path.dirname(__file__), "report-migration.xml")
    with open(filepath, "rb") as fp:
        contents = fp.read()
        if Markup:
            contents = contents.replace(b"t-raw", b"t-out")
        if not version_gte("15.0"):
            contents = contents.replace(b"t-out", b"t-esc")
        report = lxml.etree.fromstring(contents)
    e = env(cr)
    major_version, minor_version = re.findall(r"\d+", release.major_version)
    values = {
        "action_view_id": e.ref("base.action_ui_view").id,
        "version": release.major_version,
        "major_version": major_version,
        "minor_version": minor_version,
        "messages": migration_reports,
        "get_anchor_link_to_record": get_anchor_link_to_record,
    }
    _logger.info(migration_reports)
    render = e["ir.qweb"].render if hasattr(e["ir.qweb"], "render") else e["ir.qweb"]._render
    message = render(report, values=values)
    _announce_to_db(cr, message)
    # To avoid posting multiple time the same messages in case this method is called multiple times.
    migration_reports.clear()


def _announce_to_db(cr, message, to_admin_only=True):
    """Send a rendered message to the database via mail channel."""
    if not isinstance(message, basestring):
        message = message.decode("utf-8")
    if message.strip():
        message = message.replace("{", "{{").replace("}", "}}")
        kw = {}
        # If possible, post the migration report message to administrators only.
        recipient = get_admin_channel(cr) if to_admin_only else None
        if recipient:
            kw["recipient"] = recipient
        announce(cr, release.major_version, message, format="html", header=None, footer=None, **kw)


def rst2html(rst):
    overrides = {
        "embed_stylesheet": False,
        "doctitle_xform": False,
        "output_encoding": "unicode",
        "xml_declaration": False,
    }
    html = publish_string(source=dedent(rst), settings_overrides=overrides, writer=MyWriter())
    return html_sanitize(html, silent=False)


def md2html(md):
    assert markdown

    mdversion = markdown.__version_info__ if hasattr(markdown, "__version_info__") else markdown.version_info
    extensions = [
        "markdown.extensions.nl2br",
        "markdown.extensions.sane_lists",
    ]
    if mdversion[0] < 3:
        extensions.append("markdown.extensions.smart_strong")

    return markdown.markdown(md, extensions=extensions)


_DEFAULT_HEADER = """
<p>Odoo has been upgraded to version {version}.</p>
<h2>What's new in this upgrade?</h2>
"""

_DEFAULT_FOOTER = "<p>Enjoy the new Odoo Online!</p>"

_DEFAULT_RECIPIENT = "mail.%s_all_employees" % ["group", "channel"][version_gte("9.0")]


def announce(
    cr,
    version,
    msg,
    format="rst",
    recipient=_DEFAULT_RECIPIENT,
    header=_DEFAULT_HEADER,
    footer=_DEFAULT_FOOTER,
    pluses_for_enterprise=None,
):
    if not ANNOUNCE_MEDIA:
        return
    if pluses_for_enterprise is None:
        # default value depend on format and version
        major = version[0]
        pluses_for_enterprise = (major == "s" or int(major) >= 9) and format == "md"

    if pluses_for_enterprise:
        plus_re = r"^(\s*)\+ (.+)\n"
        replacement = r"\1- \2\n" if has_enterprise() else ""
        msg = re.sub(plus_re, replacement, msg, flags=re.M)

    if format == "rst":
        msg = rst2html(msg)
    elif format == "md":
        msg = md2html(msg)

    message = ((header or "") + msg + (footer or "")).format(version=version)
    if "logger" in ANNOUNCE_MEDIA:
        _logger.info(message)

    if "discuss" not in ANNOUNCE_MEDIA:
        return

    # do not notify early, in case the migration fails halfway through
    ctx = {"mail_notify_force_send": False, "mail_notify_author": True}

    uid = guess_admin_id(cr)
    try:
        registry = env(cr)
        user = registry["res.users"].browse([uid])[0].with_context(ctx)

        def ref(xid):
            return registry.ref(xid).with_context(ctx)

    except MigrationError:
        try:
            from openerp.modules.registry import RegistryManager  # noqa: PLC0415
        except ImportError:
            from openerp.modules.registry import Registry as RegistryManager  # noqa: PLC0415
        registry = RegistryManager.get(cr.dbname)
        user = registry["res.users"].browse(cr, SUPERUSER_ID, uid, context=ctx)

        def ref(xid):
            rmod, _, rxid = recipient.partition(".")
            return registry["ir.model.data"].get_object(cr, SUPERUSER_ID, rmod, rxid, context=ctx)

    # default recipient
    poster = user.message_post if hasattr(user, "message_post") else user.partner_id.message_post

    if recipient:
        try:
            if isinstance(recipient, str):  # noqa: SIM108
                recipient = ref(recipient)
            else:
                recipient = recipient.with_context(**ctx)
            poster = recipient.message_post
        except (ValueError, AttributeError):
            # Cannot find record, post the message on the wall of the admin
            pass

    type_field = ["type", "message_type"][version_gte("9.0")]
    # From 12.0, system notificatications are sent by email,
    # and do not increment the upper right notification counter.
    # While comments, in a mail.channel, do.
    # We want the notification counter to appear for announcements, so we force the comment type from 12.0.
    type_value = ["notification", "comment"][version_gte("12.0")]
    subtype_key = ["subtype", "subtype_xmlid"][version_gte("saas~13.1")]

    kw = {type_field: type_value, subtype_key: "mail.mt_comment"}

    try:
        poster(body=message, partner_ids=[user.partner_id.id], **kw)
    except Exception:
        _logger.warning("Cannot announce message", exc_info=True)
    else:
        # Chat window with the report will be open post-upgrade for the admin user
        if version_between("9.0", "saas~18.1") and user.partner_id and recipient:
            channel_member_model = (
                "discuss.channel.member"
                if version_gte("saas~16.3")
                else "mail.channel.member"
                if version_gte("16.0")
                else "mail.channel.partner"
            )
            domain = [("partner_id", "=", user.partner_id.id), ("channel_id", "=", recipient.id)]
            try:
                registry[channel_member_model].search(domain)[:1].with_context(ctx).fold_state = "open"
            except Exception:
                _logger.warning("Cannot unfold chat window", exc_info=True)


def get_anchor_link_to_record(model, id, name=None, action_id=None):
    _validate_model(model)
    if not name:
        name = "{}(id={})".format(model, id)
    if version_gte("saas~17.2"):
        part1 = "action-{}".format(action_id) if action_id else model
        url = "/odoo/{}/{}?debug=1".format(part1, id)
    else:
        url = "/web?debug=1#view_type=form&amp;model={}&amp;action={}&amp;id={}".format(model, action_id or "", id)

    anchor_tag = '<a target="_blank" href="{}">{}</a>'.format(url, html_escape(name))
    if Markup:
        anchor_tag = Markup(anchor_tag)
    return anchor_tag
