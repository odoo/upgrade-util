# -*- coding: utf-8 -*-
import logging
import os
import re
from textwrap import dedent

import lxml
from docutils.core import publish_string

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
    from odoo import SUPERUSER_ID, release
    from odoo.tools import html_escape
    from odoo.tools.mail import html_sanitize
except ImportError:
    from openerp import SUPERUSER_ID, release
    from openerp.tools.mail import html_sanitize

    try:
        from openerp.tools.misc import html_escape
    except ImportError:
        import werkzeug.utils

        # Avoid DeprecationWarning while still remaining compatible with werkzeug pre-0.9
        if parse_version(getattr(werkzeug, "__version__", "0.0")) < parse_version("0.9.0"):

            def html_escape(text):
                return werkzeug.utils.escape(text, quote=True)

        else:

            def html_escape(text):
                return werkzeug.utils.escape(text)


try:
    from odoo.addons.base.models.ir_module import MyWriter  # > 11.0
except ImportError:
    try:
        from odoo.addons.base.module.module import MyWriter
    except ImportError:
        from openerp.addons.base.module.module import MyWriter

from .exceptions import MigrationError
from .misc import has_enterprise, version_gte
from .orm import env, get_admin_channel, guess_admin_id

migration_reports = {}
_logger = logging.getLogger(__name__)


def add_to_migration_reports(message, category="Other", format="text"):
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


def announce_migration_report(cr):
    filepath = os.path.join(os.path.dirname(__file__), "report-migration.xml")
    with open(filepath, "rb") as fp:
        contents = fp.read()
        if Markup:
            contents = contents.replace(b"t-raw", b"t-out")
        report = lxml.etree.fromstring(contents)
    e = env(cr)
    values = {
        "action_view_id": e.ref("base.action_ui_view").id,
        "major_version": release.major_version,
        "messages": migration_reports,
        "get_anchor_link_to_record": get_anchor_link_to_record,
    }
    _logger.info(migration_reports)
    render = e["ir.qweb"].render if hasattr(e["ir.qweb"], "render") else e["ir.qweb"]._render
    message = render(report, values=values)
    if not isinstance(message, basestring):
        message = message.decode("utf-8")
    if message.strip():
        message = message.replace("{", "{{").replace("}", "}}")
        kw = {}
        # If possible, post the migration report message to administrators only.
        admin_channel = get_admin_channel(cr)
        if admin_channel:
            kw["recipient"] = admin_channel
        announce(cr, release.major_version, message, format="html", header=None, footer=None, **kw)
    # To avoid posting multiple time the same messages in case this method is called multiple times.
    migration_reports.clear()


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
    import markdown

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
    if pluses_for_enterprise is None:
        # default value depend on format and version
        major = version[0]
        pluses_for_enterprise = (major == "s" or int(major) >= 9) and format == "md"

    if pluses_for_enterprise:
        plus_re = r"^(\s*)\+ (.+)\n"
        replacement = r"\1- \2\n" if has_enterprise() else ""
        msg = re.sub(plus_re, replacement, msg, flags=re.M)

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
            from openerp.modules.registry import RegistryManager
        except ImportError:
            from openerp.modules.registry import Registry as RegistryManager
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

    if format == "rst":
        msg = rst2html(msg)
    elif format == "md":
        msg = md2html(msg)

    message = ((header or "") + msg + (footer or "")).format(version=version)
    _logger.debug(message)

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


def get_anchor_link_to_record(model, id, name, action_id=None):
    _validate_model(model)
    if version_gte("saas~17.2"):
        part1 = "action-{}".format(action_id) if action_id else model
        url = "/odoo/{}/{}?debug=1".format(part1, id)
    else:
        url = "/web?debug=1#view_type=form&amp;model={}&amp;action={}&amp;id={}".format(model, action_id or "", id)

    anchor_tag = '<a target="_blank" href="{}">{}</a>'.format(url, html_escape(name))
    if Markup:
        anchor_tag = Markup(anchor_tag)
    return anchor_tag
