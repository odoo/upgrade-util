import operator
import re
import threading
import unittest
import uuid
from ast import literal_eval

from lxml import etree

try:
    from unittest import mock
except ImportError:
    import mock

from odoo.osv.expression import FALSE_LEAF, TRUE_LEAF
from odoo.tools import mute_logger

from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.testing import UnitTestCase, parametrize
from odoo.addons.base.maintenance.migrations.util.domains import _adapt_one_domain
from odoo.addons.base.maintenance.migrations.util.exceptions import MigrationError


class TestAdaptOneDomain(UnitTestCase):
    def setUp(self):
        super(TestAdaptOneDomain, self).setUp()
        self.mock_adapter = mock.Mock()

    def test_change_no_leaf(self):
        # testing plan: updata path of a domain where the last element is not changed

        # no adapter
        domain = [("partner_id.user_id.partner_id.user_id.partner_id", "=", False)]
        match_domain = [("partner_id.friend_id.partner_id.friend_id.partner_id", "=", False)]
        new_domain = _adapt_one_domain(self.cr, "res.partner", "user_id", "friend_id", "res.users", domain)
        self.assertEqual(match_domain, new_domain)

        # with adapter, verify it's not called
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.assertEqual(match_domain, new_domain)
        self.mock_adapter.assert_not_called()

    def test_change_leaf(self):
        # testing plan: update path of a domain where the last element is changed

        # no adapter
        domain = [("partner_id.user_id.partner_id.user_id", "=", False)]
        match_domain = [("partner_id.friend_id.partner_id.friend_id", "=", False)]

        new_domain = _adapt_one_domain(self.cr, "res.partner", "user_id", "friend_id", "res.users", domain)
        self.assertEqual(match_domain, new_domain)

        # with adapter, verify it's called even if nothing was changed on the path
        self.mock_adapter.return_value = domain  # adapter won't update anything
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "user_id", "res.users", domain, adapter=self.mock_adapter
        )  # even if new==old the adapter must be called
        self.mock_adapter.assert_called_once()
        # Ignore `boolean-positional-value-in-call` lint violations in the whole file
        # ruff: noqa: FBT003
        self.mock_adapter.assert_called_with(domain[0], False, False)
        self.assertEqual(None, new_domain)

        # path is changed even if adapter doesn't touch it
        self.mock_adapter.reset_mock()
        match_domain = [("partner_id.friend_id.partner_id.friend_id", "=", False)]
        self.mock_adapter.return_value = domain  # adapter won't update anything
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_once()
        self.mock_adapter.assert_called_with(domain[0], False, False)
        self.assertEqual(match_domain, new_domain)  # updated path even if adapter didn't

    def test_adapter_calls(self):
        # testing plan: ensure the adapter is called with the right parameters

        self.mock_adapter.return_value = [("partner_id.friend_id", "=", 2)]

        # '&' domain
        domain = ["&", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["&", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(domain[1], False, False)
        self.assertEqual(match_domain, new_domain)

        # '|' domain
        self.mock_adapter.reset_mock()
        domain = ["|", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["|", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(domain[1], True, False)
        self.assertEqual(match_domain, new_domain)

        # '!' domain
        self.mock_adapter.reset_mock()
        domain = ["!", ("partner_id.user_id", "=", 1)]
        match_domain = ["!", ("partner_id.friend_id", "=", 2)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(domain[1], False, True)
        self.assertEqual(match_domain, new_domain)

        # '&' '!' domain
        self.mock_adapter.reset_mock()
        domain = ["|", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["|", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(domain[2], True, True)
        self.assertEqual(match_domain, new_domain)

        # '|' '!' domain
        self.mock_adapter.reset_mock()
        domain = ["|", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["|", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(domain[2], True, True)
        self.assertEqual(match_domain, new_domain)

    def test_adapter_more_domains(self):
        # testing plan: check more complex domains

        self.mock_adapter.return_value = [("partner_id.friend_id", "=", 2)]
        term = ("partner_id.user_id", "=", 1)

        # double '!'
        self.mock_adapter.reset_mock()
        domain = ["!", "!", ("partner_id.user_id", "=", 1)]
        match_domain = ["!", "!", ("partner_id.friend_id", "=", 2)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, False)
        self.assertEqual(match_domain, new_domain)

        # triple '!'
        self.mock_adapter.reset_mock()
        domain = ["!", "!", "!", ("partner_id.user_id", "=", 1)]
        match_domain = ["!", "!", "!", ("partner_id.friend_id", "=", 2)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, True)
        self.assertEqual(match_domain, new_domain)

        # '|' double '!'
        self.mock_adapter.reset_mock()
        domain = ["|", "!", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["|", "!", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, True, False)
        self.assertEqual(match_domain, new_domain)

        # '&' double '!'
        self.mock_adapter.reset_mock()
        domain = ["&", "!", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["&", "!", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, False)
        self.assertEqual(match_domain, new_domain)

        # mixed domains
        self.mock_adapter.reset_mock()
        domain = ["|", "&", ("partner_id.user_id", "=", 1), ("name", "=", False), ("name", "=", False)]
        match_domain = ["|", "&", ("partner_id.friend_id", "=", 2), ("name", "=", False), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, False)
        self.assertEqual(match_domain, new_domain)

        self.mock_adapter.reset_mock()
        domain = ["&", "|", ("partner_id.user_id", "=", 1), ("name", "=", False), ("name", "=", False)]
        match_domain = ["&", "|", ("partner_id.friend_id", "=", 2), ("name", "=", False), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, True, False)
        self.assertEqual(match_domain, new_domain)

        self.mock_adapter.reset_mock()
        domain = ["|", "&", "!", ("partner_id.user_id", "=", 1), ("name", "=", False), ("name", "=", False)]
        match_domain = ["|", "&", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, True)
        self.assertEqual(match_domain, new_domain)

        self.mock_adapter.reset_mock()
        domain = ["&", "|", "!", ("partner_id.user_id", "=", 1), ("name", "=", False), ("name", "=", False)]
        match_domain = ["&", "|", "!", ("partner_id.friend_id", "=", 2), ("name", "=", False), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, True, True)
        self.assertEqual(match_domain, new_domain)

    @parametrize(
        [
            # first and last position in path at the same time
            ("partner_id", "res.users"),
            # first and last position in path
            ("partner_id.user_id.partner_id", "res.users"),
            # last position
            ("user_id.partner_id", "res.partner"),
            # middle
            ("user_id.partner_id.user.id", "res.partner"),
            # last position, longer domain
            ("company_id.partner_id.user_id.partner_id", "res.partner"),
        ]
    )
    def test_force_adapt(self, left, model, target_model="res.users", old="partner_id"):
        # simulate the adapter used for removal of a field
        # this is the main use case for force_adapt=True
        self.mock_adapter.return_value = [TRUE_LEAF]
        domain = [(left, "=", False)]
        res = _adapt_one_domain(
            self.cr, target_model, old, "ignored", model, domain, adapter=self.mock_adapter, force_adapt=True
        )
        self.mock_adapter.assert_called_once()
        self.assertEqual(res, self.mock_adapter.return_value)


class TestAdaptDomainView(UnitTestCase):
    def test_adapt_domain_view(self):
        view_form = self.env["ir.ui.view"].create(
            {
                "name": "test_adapt_domain_view_form",
                "model": "res.currency",
                "arch": """\
                <form>
                  <field name="rate_ids">
                    <tree>
                      <field name="company_id" domain="[('email', '!=', False)]"/>
                      <field name="company_id" domain="[('email', 'not like', 'odoo.com')]"/>
                    </tree>
                  </field>
                </form>
            """,
            }
        )

        view_search_1 = self.env["ir.ui.view"].create(
            {
                "name": "test_adapt_domain_view_search",
                "model": "res.company",
                "arch": """\
                <search>
                  <field name="email" string="Mail" filter_domain="[('email', '=', self)]"/>
                </search>
            """,
            }
        )

        view_search_2 = self.env["ir.ui.view"].create(
            {
                "name": "test_adapt_domain_view_search",
                "model": "res.company",
                "arch": """\
                <search>
                  <filter name="mail" string="Mail" domain="[('email', '=', self)]"/>
                </search>
            """,
            }
        )

        util.adapt_domains(self.env.cr, "res.partner", "email", "courriel")
        util.invalidate(view_form | view_search_1 | view_search_2)

        self.assertIn("email", view_form.arch)
        self.assertIn("email", view_search_1.arch)
        self.assertIn("email", view_search_2.arch)

        util.adapt_domains(self.env.cr, "res.company", "email", "courriel")
        util.invalidate(view_form | view_search_1 | view_search_2)

        self.assertIn("courriel", view_form.arch)
        self.assertIn("courriel", view_search_1.arch)
        self.assertIn("courriel", view_search_2.arch)


class TestRemoveFieldDomains(UnitTestCase):
    @parametrize(
        [
            ([("updated", "=", 0)], [TRUE_LEAF]),
            # operator is not relevant
            ([("updated", "!=", 0)], [TRUE_LEAF]),
            # if negate we should end with "not false"
            (["!", ("updated", "!=", 0)], ["!", FALSE_LEAF]),
            # multiple !, we should still end with a true leaf
            (["!", "!", ("updated", ">", 0)], ["!", "!", TRUE_LEAF]),
            # with operator
            ([("updated", "=", 0), ("state", "=", "done")], ["&", TRUE_LEAF, ("state", "=", "done")]),
            (["&", ("updated", "=", 0), ("state", "=", "done")], ["&", TRUE_LEAF, ("state", "=", "done")]),
            (["|", ("updated", "=", 0), ("state", "=", "done")], ["|", FALSE_LEAF, ("state", "=", "done")]),
            # in second operand
            (["&", ("state", "=", "done"), ("updated", "=", 0)], ["&", ("state", "=", "done"), TRUE_LEAF]),
            (["|", ("state", "=", "done"), ("updated", "=", 0)], ["|", ("state", "=", "done"), FALSE_LEAF]),
            # combination with !
            (["&", "!", ("updated", "=", 0), ("state", "=", "done")], ["&", "!", FALSE_LEAF, ("state", "=", "done")]),
            (["|", "!", ("updated", "=", 0), ("state", "=", "done")], ["|", "!", TRUE_LEAF, ("state", "=", "done")]),
            # here, the ! apply on the whole &/| and should not invert the replaced leaf
            (["!", "&", ("updated", "=", 0), ("state", "=", "done")], ["!", "&", TRUE_LEAF, ("state", "=", "done")]),
            (["!", "|", ("updated", "=", 0), ("state", "=", "done")], ["!", "|", FALSE_LEAF, ("state", "=", "done")]),
        ]
    )
    def test_remove_field(self, domain, expected):
        cr = self.env.cr
        cr.execute(
            "INSERT INTO ir_filters(name, model_id, domain, context, sort)"
            "     VALUES ('test', 'base.module.update', %s, '{}', 'id') RETURNING id",
            [str(domain)],
        )
        (filter_id,) = cr.fetchone()

        util.remove_field(cr, "base.module.update", "updated")

        cr.execute("SELECT domain FROM ir_filters WHERE id = %s", [filter_id])
        altered_domain = literal_eval(cr.fetchone()[0])

        self.assertEqual(altered_domain, expected)


class TestIterBrowse(UnitTestCase):
    def test_iter_browse_iter(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for c, in cr.fetchall()]
        chunk_size = 10

        Country = type(self.env["res.country"])
        func = "fetch" if util.version_gte("saas~16.2") else "_read" if util.version_gte("saas~12.5") else "read"
        with mock.patch.object(Country, func, autospec=True, side_effect=getattr(Country, func)) as read:
            for c in util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size):
                c.name  # noqa: B018
        expected = (len(ids) + chunk_size - 1) // chunk_size
        self.assertEqual(read.call_count, expected)

    def test_iter_browse_call(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for c, in cr.fetchall()]
        chunk_size = 10

        Country = type(self.env["res.country"])
        with mock.patch.object(Country, "write", autospec=True, side_effect=Country.write) as write:
            ib = util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size)
            ib.write({"vat_label": "VAT"})

        expected = (len(ids) + chunk_size - 1) // chunk_size
        self.assertEqual(write.call_count, expected)

    def test_iter_browse_create_non_empty(self):
        RPT = self.env["res.partner.title"]
        with self.assertRaises(ValueError):
            util.iter_browse(RPT, [42]).create([{}])

    @parametrize([(True,), (False,)])
    def test_iter_browse_create(self, multi):
        chunk_size = 2
        RPT = self.env["res.partner.title"]

        names = [f"Title {i}" for i in range(7)]
        ib = util.iter_browse(RPT, [], chunk_size=chunk_size)
        records = ib.create([{"name": name} for name in names], multi=multi)
        self.assertEqual([t.name for t in records], names)

    def test_iter_browse_iter_twice(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for c, in cr.fetchall()]
        chunk_size = 10

        ib = util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size)
        for c in ib:
            c.name  # noqa: B018

        with self.assertRaises(RuntimeError):
            for c in ib:
                c.name  # noqa: B018

    def test_iter_browse_call_twice(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for c, in cr.fetchall()]
        chunk_size = 10

        ib = util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size)
        ib.write({"vat_label": "VAT"})

        with self.assertRaises(RuntimeError):
            ib.write({"name": "FAIL"})


class TestPG(UnitTestCase):
    @parametrize(
        [
            ("test", "<p>test</p>"),
            ("<p>test</p>", "<p>test</p>"),
            ("<div>test</div>", "<div>test</div>"),
            # escapings
            ("r&d", "<p>r&amp;d</p>"),
            ("!<(^_^)>!", "<p>!&lt;(^_^)&gt;!</p>"),
            ("'quoted'", "<p>'quoted'</p>"),
            # and with links
            (
                "Go to https://upgrade.odoo.com/?debug=1&version=14.0 and follow the instructions.",
                '<p>Go to <a href="https://upgrade.odoo.com/?debug=1&amp;version=14.0"'
                ' target="_blank" rel="noreferrer noopener">https://upgrade.odoo.com/?debug=1&amp;version=14.0</a> and'
                " follow the instructions.</p>",
            ),
        ]
    )
    def test_pg_text2html(self, value, expected):
        cr = self.env.cr
        uid = self.env.user.id
        cr.execute("UPDATE res_users SET signature=%s WHERE id=%s", [value, uid])
        cr.execute("SELECT {} FROM res_users WHERE id=%s".format(util.pg_text2html("signature")), [uid])
        result = cr.fetchone()[0]
        self.assertEqual(result, expected)

    def test_parallel_rowcount(self):
        cr = self.env.cr
        cr.execute("SELECT count(*) FROM res_lang")
        [expected] = cr.fetchone()

        # util.parallel_execute will `commit` the cursor and create new ones
        # as we are in a test, we should not commit as we are in a subtransaction
        with mock.patch.object(cr, "commit", lambda: ...):
            query = "UPDATE res_lang SET name = name"
            rowcount = util.explode_execute(cr, query, table="res_lang", bucket_size=10)
        self.assertEqual(rowcount, expected)

    def test_parallel_rowcount_threaded(self):
        threading.current_thread().testing = False
        self.test_parallel_rowcount()
        threading.current_thread().testing = True

    def test_create_column_with_fk(self):
        cr = self.env.cr
        self.assertFalse(util.column_exists(cr, "res_partner", "_test_lang_id"))

        with self.assertRaises(ValueError):
            util.create_column(cr, "res_partner", "_test_lang_id", "int4", on_delete_action="SET NULL")

        with self.assertRaises(ValueError):
            util.create_column(
                cr, "res_partner", "_test_lang_id", "int4", fk_table="res_lang", on_delete_action="INVALID"
            )

        # this one should works
        util.create_column(cr, "res_partner", "_test_lang_id", "int4", fk_table="res_lang", on_delete_action="SET NULL")

        target = util.target_of(cr, "res_partner", "_test_lang_id")
        self.assertEqual(target, ("res_lang", "id", "res_partner__test_lang_id_fkey"))

        # code should be reentrant
        util.create_column(cr, "res_partner", "_test_lang_id", "int4", fk_table="res_lang", on_delete_action="SET NULL")

        target = util.target_of(cr, "res_partner", "_test_lang_id")
        self.assertEqual(target, ("res_lang", "id", "res_partner__test_lang_id_fkey"))


class TestORM(UnitTestCase):
    def test_create_cron(self):
        cr = self.env.cr
        util.create_cron(cr, "Test cron creation don't fail", "res.partner", "answer = 42")

        cron_id = util.ref(cr, "__upgrade__.cron_post_upgrade_test_cron_creation_don_t_fail")
        self.assertIsNotNone(cron_id)
        cron = self.env["ir.cron"].browse(cron_id)
        self.assertEqual(cron.code, "answer = 42")


class TestHelpers(UnitTestCase):
    def test_model_table_convertion(self):
        cr = self.env.cr
        for model in self.env.registry:
            if model in ("ir.actions.act_window_close",):
                continue
            table = util.table_of_model(cr, model)
            self.assertEqual(table, self.env[model]._table)
            self.assertEqual(util.model_of_table(cr, table), model)


@unittest.skipIf(
    util.version_gte("saas~17.1"),
    "Starting Odoo 17, the info being stored in the database, the test can't lie about its base version",
)
class TestInherit(UnitTestCase):
    @classmethod
    def setUpClass(cls):
        bv = util.ENVIRON.get("__base_version")
        util.ENVIRON["__base_version"] = util.parse_version("12.0.1.3")
        if bv:
            cls.addClassCleanup(operator.setitem, util.ENVIRON, "__base_version", bv)
        return super().setUpClass()

    @parametrize(
        [
            # simple tests
            ("do.not.exits", []),
            ("account.common.journal.report", ["account.common.report"]),
            # avoid duplicates
            (
                "product.product",
                [
                    "mail.activity.mixin",
                    "mail.thread",
                    "product.template",
                    "rating.mixin",
                    "website.published.multi.mixin",
                    "website.seo.metadata",
                ],
            ),
            # version boundaries
            # ... born after 12.0, should not include it.
            ("report.paperformat", []),
            # ... dead before 12.0. should not be included
            ("delivery.carrier", ["website.published.multi.mixin"]),
            # ... dead between 12.0 and CURRENT_VERSION
            ("crm.lead.convert2task", ["crm.partner.binding"]),
        ]
    )
    def test_inherit_parents(self, model, expected):
        cr = self.env.cr
        result = sorted(util.inherit_parents(cr, model))
        self.assertEqual(result, sorted(expected))

    def test_direct_inherit_parents(self):
        cr = self.env.cr
        result = sorted(util.direct_inherit_parents(cr, "product.product"))
        self.assertEqual(len(result), 3)
        parents, inhs = zip(*result)
        self.assertEqual(parents, ("mail.activity.mixin", "mail.thread", "product.template"))
        self.assertTrue(all(inh.model == "product.product" for inh in inhs))
        self.assertEqual([inh.via for inh in inhs], [None, None, "product_tmpl_id"])


class TestNamedCursors(UnitTestCase):
    @staticmethod
    def exec(cr, which="", args=()):  # noqa: A003
        cr.execute("SELECT * FROM ir_ui_view")
        if which:
            return getattr(cr, which)(*args)
        return None

    @parametrize(
        [
            (None, "dictfetchone"),
            (None, "dictfetchmany", [10]),
            (None, "dictfetchall"),
            (1, "dictfetchone"),
            (1, "dictfetchmany", [10]),
            (1, "dictfetchall"),
            (None, "fetchone"),
            (None, "fetchmany", [10]),
            (None, "fetchall"),
            (1, "fetchone"),
            (1, "fetchmany", [10]),
            (1, "fetchall"),
        ]
    )
    def test_dictfetch(self, itersize, which, args=()):
        expected = self.exec(self.env.cr, which, args)
        with util.named_cursor(self.env.cr, itersize=itersize) as ncr:
            result = self.exec(ncr, which, args)
        self.assertEqual(result, expected)

    def test_iterdict(self):
        expected = self.exec(self.env.cr, "dictfetchall")
        with util.named_cursor(self.env.cr) as ncr:
            result = list(self.exec(ncr, "iterdict"))
        self.assertEqual(result, expected)

    def test_iter(self):
        expected = self.exec(self.env.cr, "fetchall")
        with util.named_cursor(self.env.cr) as ncr:
            result = list(self.exec(ncr, "__iter__"))
        self.assertEqual(result, expected)


class TestRecords(UnitTestCase):
    def test_rename_xmlid(self):
        cr = self.env.cr

        old = self.env["res.currency"].create({"name": "TX1", "symbol": "TX1"})
        new = self.env["res.currency"].create({"name": "TX2", "symbol": "TX2"})
        self.env["ir.model.data"].create({"name": "TX1", "module": "base", "model": "res.currency", "res_id": old.id})
        self.env["ir.model.data"].create({"name": "TX2", "module": "base", "model": "res.currency", "res_id": new.id})

        rate = self.env["res.currency.rate"].create({"currency_id": old.id})
        self.env["ir.model.data"].create(
            {"name": "test_rate_tx1", "module": "base", "model": "res.currency.rate", "res_id": rate.id}
        )

        if hasattr(self, "_savepoint_id"):
            # As the `rename_xmlid` method uses `parallel_execute`, the cursor is commited; which kill
            # the savepoint created by the test setup (since saas~14.1 with the merge of SavepointCase
            # into TransactionCase in odoo/odoo@7f2e168c02a7aea666d34510ed2ed8efacd5654b).
            # Force a new one to avoid this issue.
            # Incidentally, we should also explicitly remove the created records.
            self.addCleanup(cr.execute, f"SAVEPOINT test_{self._savepoint_id}")
            self.addCleanup(old.unlink)
            self.addCleanup(new.unlink)
            self.addCleanup(rate.unlink)

        # Wrong model
        with self.assertRaises(MigrationError):
            util.rename_xmlid(cr, "base.TX1", "base.test_rate_tx1", on_collision="merge")

        # Collision
        with self.assertRaises(MigrationError):
            util.rename_xmlid(cr, "base.TX1", "base.TX2", on_collision="fail")

        # As TX2 is not free, TX1 is merged with TX2
        with mute_logger(util.helpers._logger.name):
            res = util.rename_xmlid(cr, "base.TX1", "base.TX2", on_collision="merge")
        self.assertEqual(res, new.id)
        self.assertEqual(util.ref(cr, "base.TX1"), None)

        # TX1 references moved to TX2
        cr.execute("SELECT currency_id FROM res_currency_rate WHERE id = %s", [rate.id])
        self.assertEqual(cr.fetchall(), [(new.id,)])

        # Nothing left to rename in TX1
        res = util.rename_xmlid(cr, "base.TX1", "base.TX3", on_collision="merge")
        self.assertEqual(res, None)

        # Can rename to empty TX3 without need for merge
        res = util.rename_xmlid(cr, "base.TX2", "base.TX3", on_collision="merge")
        self.assertEqual(res, new.id)

        # Normal rename
        res = util.rename_xmlid(cr, "base.TX3", "base.TX4")
        self.assertEqual(res, new.id)

    def test_update_record_from_xml(self):
        # reset all fields on a <record>
        xmlid = "base.res_partner_title_mister"
        data_after = {"name": "Fortytwo", "shortcut": "42"}
        record = self.env.ref(xmlid)
        data_before = {key: record[key] for key in data_after}
        for key, value in data_after.items():
            record.write({key: value})
            self.assertEqual(record[key], value)

        util.update_record_from_xml(self.env.cr, xmlid, reset_translations=True)
        if util.version_gte("16.0"):
            record.invalidate_recordset(["name"])
        else:
            record.invalidate_cache(["name"], record.ids)
        for key, value in data_before.items():
            self.assertEqual(record[key], value)

    def test_update_record_from_xml_recursive_menuitem(self):
        # reset all fields on a <menuitem>
        xmlid = "base.menu_security"
        data_after = {"name": "ATotallyValidSecurityMenu", "sequence": 112, "parent_id": self.env["ir.ui.menu"]}
        record = self.env.ref(xmlid)
        data_before = {key: record[key] for key in data_after}
        for key, value in data_after.items():
            record.write({key: value})
            self.assertEqual(record[key], value)

        util.update_record_from_xml(self.env.cr, xmlid)
        if util.version_gte("16.0"):
            record.invalidate_recordset(["name"])
        else:
            record.invalidate_cache(["name"], record.ids)
        for key, value in data_before.items():
            self.assertEqual(record[key], value)

    def test_upgrade_record_from_xml_ensure_references(self):
        def change(xmlid):
            cat = self.env.ref(xmlid)
            result = cat.name
            cat.write({"name": str(uuid.uuid4())})
            util.flush(cat)
            util.invalidate(cat)
            return result

        if util.version_gte("saas~13.5"):
            xmlid_tree = [
                "base.module_category_accounting_localizations_account_charts",
                "base.module_category_accounting_localizations",
                "base.module_category_accounting",
            ]
        else:
            xmlid_tree = [
                "base.module_category_localization_account_charts",
                "base.module_category_localization",
            ]

        old_names = [change(xmlid) for xmlid in xmlid_tree]

        util.update_record_from_xml(self.env.cr, xmlid_tree[0], ensure_references=True)

        for xmlid, expected in zip(xmlid_tree, old_names):
            cat = self.env.ref(xmlid)
            self.assertEqual(cat.name, expected)

    def test_update_record_from_xml_template_tag(self):
        # reset all fields on a <template>
        template_xmlid = "base.contact_name"
        record = self.env.ref(template_xmlid)
        non_xpath = etree.XPath("/non")
        data_after = {"name": "42", "arch_db": "<non>sense</non>"}
        data_before = {key: record[key] for key in data_after}
        record.write(data_after)
        for key, value in data_after.items():
            if key == "arch_db":
                tree = etree.fromstring(value)
                [non_element] = non_xpath(tree)
                self.assertEqual(non_element.text, "sense")
            else:
                self.assertEqual(record[key], value)

        util.update_record_from_xml(self.env.cr, template_xmlid, reset_translations=True)
        if util.version_gte("16.0"):
            record.invalidate_recordset(data_after.keys())
        else:
            record.invalidate_cache(data_after.keys(), record.ids)

        # asserting  equality for the full arch_db fails for some versions due to different quotes being used
        self.assertEqual(record.name, data_before["name"])
        tree = etree.fromstring(record.arch_db)
        non_query_result = non_xpath(tree)
        self.assertEqual(len(non_query_result), 0)
        [template_element] = tree.xpath("/t")
        self.assertEqual(template_element.attrib["t-name"], template_xmlid)

    def test_update_record_translations_from_xml(self):
        # reset all translated fields on a <record>
        be_lang = self.env["res.lang"].with_context(active_test=False).search([("code", "=", "fr_BE")])
        be_lang.write({"active": True})

        xmlid = "base.res_partner_title_mister"
        util.update_record_from_xml(self.env.cr, xmlid, reset_translations=True)

        # change the translations to something arbitrary for all installed languages
        langs = self.env["res.lang"].get_installed()
        filter_lang = [code for code, _ in langs]
        self.assertIn(be_lang.code, filter_lang)
        data_after = {"name": "Fortytwo", "shortcut": "42"}
        fieldnames = list(data_after.keys())
        template_record = self.env.ref(xmlid)

        data_before = {}
        for lang in filter_lang:
            data_before[lang] = {fname: template_record.with_context(lang=lang)[fname] for fname in fieldnames}

            # write & assert arbitrary translations
            for fname, value in data_after.items():
                template_record.with_context(lang=lang).write({fname: value})
                self.assertEqual(template_record.with_context(lang=lang)[fname], value)
        util.invalidate(template_record)

        # re-reset all translated fields on a <record>
        util.update_record_from_xml(self.env.cr, xmlid, reset_translations=True)
        util.invalidate(template_record)

        for lang, field_to_value in data_before.items():
            for fname, value in field_to_value.items():
                self.assertEqual(template_record.with_context(lang=lang)[fname], value)

    def test_update_record_from_xml__from_module(self):
        cr = self.env.cr
        if not util.module_installed(cr, "web"):
            self.skip()

        layout = self.env.ref("web.report_layout")
        new_name = str(uuid.uuid4())

        layout.write({"name": new_name})
        util.flush(layout)
        util.invalidate(layout)

        # the record `base.report_layout` does not exists, so we must pass the `force_create`.
        # we only test that it won't update `web.report_layout`.
        util.update_record_from_xml(cr, "base.report_layout", from_module="web", force_create=True)
        util.invalidate(layout)

        # web.layout should NOT have been updated
        self.assertEqual(layout.name, new_name)

    def test_update_record_from_xml_bad_match(self):
        cr = self.env.cr
        if not util.module_installed(cr, "web"):
            self.skip()

        xmlid = "web.login"
        util.update_record_from_xml(cr, xmlid)

        arch = self.env.ref(xmlid).arch_db
        tree = etree.fromstring(arch)
        self.assertIsNotNone(tree.find(".//input[@id='login']"))

    def test_ensure_xmlid_match_record(self):
        cr = self.env.cr
        tx1 = self.env["res.currency"].create({"name": "TX1", "symbol": "TX1"})
        tx2 = self.env["res.currency"].create({"name": "TX2", "symbol": "TX2"})
        self.env["ir.model.data"].create({"name": "TX1", "module": "base", "model": "res.currency", "res_id": tx1.id})
        self.env["ir.model.data"].create({"name": "TX2", "module": "base", "model": "res.currency", "res_id": tx2.id})

        # case: `base.TX1` points to ResCurrency(168) and matches values {'name': 'TX1'}
        ensured_id = util.ensure_xmlid_match_record(cr, "base.TX1", "res.currency", {"name": "TX1"})
        self.assertEqual(ensured_id, tx1.id)
        newtx1 = util.ref(cr, "base.TX1")
        self.assertEqual(newtx1, tx1.id)

        # break one res_id
        cr.execute("UPDATE ir_model_data SET res_id=%s WHERE module='base' AND name='TX1'", [tx2.id])

        # case: `base.TX1` points to ResCurrency(169) but doesn't match values {'name': 'TX3'}; no other matches found.
        ensured_id = util.ensure_xmlid_match_record(cr, "base.TX1", "res.currency", {"name": "TX3"})
        self.assertEqual(ensured_id, tx2.id)

        # check it still point to tx2
        newtx1 = util.ref(cr, "base.TX1")
        self.assertEqual(newtx1, tx2.id)

        # case: `base.TX4` doesn't exist; no match found for values {'name': 'TX4'}
        ensured_id = util.ensure_xmlid_match_record(cr, "base.TX4", "res.currency", {"name": "TX4"})
        self.assertIsNone(ensured_id)

        # case: update `base.TX1` to point to ResCurrency(168) instead of ResCurrency(169); matching values {'name': 'TX1'}
        ensured_id = util.ensure_xmlid_match_record(cr, "base.TX1", "res.currency", {"name": "TX1"})
        self.assertEqual(ensured_id, tx1.id)

        newtx1 = util.ref(cr, "base.TX1")
        self.assertEqual(newtx1, tx1.id)

        # delete model data entry
        cr.execute("DELETE FROM ir_model_data WHERE module='base' AND name='TX1'")
        self.assertIsNone(util.ref(cr, "base.TX1"))

        # case: create `base.TX1` that point to ResCurrency(168); matching values {'name': 'TX1'}
        ensured_id = util.ensure_xmlid_match_record(cr, "base.TX1", "res.currency", {"name": "TX1"})
        self.assertEqual(ensured_id, tx1.id)

        newtx1 = util.ref(cr, "base.TX1")
        self.assertEqual(newtx1, tx1.id)

    @unittest.skipUnless(util.version_gte("16.0"), "Only work on Odoo >= 16")
    def test_replace_in_all_jsonb_values(self):
        test_partner_title = self.env["res.partner.title"].create(
            {"name": r"""object.number '<"x">\y object.numbercombined"""}
        )

        pattern_old = re.compile(r"\b\.number\b")
        pattern_new = re.compile(r"\b\.name\b")
        pattern_notouch = re.compile(r"\b\.numbercombined\b")

        self.assertNotRegex(test_partner_title.name, pattern_new)
        self.assertRegex(test_partner_title.name, pattern_notouch)
        self.assertRegex(test_partner_title.name, pattern_old)

        extra_filter = self.env.cr.mogrify("t.id = %s", (test_partner_title.id,)).decode()
        util.replace_in_all_jsonb_values(self.env.cr, "res_partner_title", "name", ".number", ".name", extra_filter)
        util.replace_in_all_jsonb_values(
            self.env.cr, "res_partner_title", "name", r"""'<"x">\y""", "GONE", extra_filter
        )
        test_partner_title.invalidate_recordset(["name"])

        self.assertRegex(test_partner_title.name, pattern_new)
        self.assertRegex(test_partner_title.name, pattern_notouch)
        self.assertNotRegex(test_partner_title.name, pattern_old)
        # ensure replacing works for patterns that do not start with a valid word start \w
        # also ensure the replace works for multiple embedded quotes
        self.assertEqual(test_partner_title.name, "object.name GONE object.numbercombined")


class TestMisc(UnitTestCase):
    @parametrize(
        [
            ("{a,b}", ["a", "b"]),
            ("head_{a,b}_tail", ["head_a_tail", "head_b_tail"]),
            ("head_only_{a,b}", ["head_only_a", "head_only_b"]),
            ("{a,b}_tail_only", ["a_tail_only", "b_tail_only"]),
            ("{with,more,than,one,comma}", ["with", "more", "than", "one", "comma"]),
            ("head_{one,two,three}_tail", ["head_one_tail", "head_two_tail", "head_three_tail"]),
            ("same_{a,a}", ["same_a", "same_a"]),
            ("empty_part_{a,}", ["empty_part_a", "empty_part_"]),
            ("empty_part_{,b}", ["empty_part_", "empty_part_b"]),
            ("two_empty_{,}", ["two_empty_", "two_empty_"]),
            ("with_cr\n_{a,b}", ["with_cr\n_a", "with_cr\n_b"]),
            ("with_cr_in_{a\nb,c\nd}_end", ["with_cr_in_a\nb_end", "with_cr_in_c\nd_end"]),
        ]
    )
    def test_expand_braces(self, value, expected):
        self.assertEqual(util.expand_braces(value), expected)

    @parametrize(
        [
            (value,)
            for value in [
                "",
                "no_braces",
                "empty_{}",
                "one_{item}",
                "unclosed_{_brace",
                "two_{a,b}_expanses_{x,y}",
                # braces into braces
                "{a,{b,c},d}",
                "{a,{}",
                "{a,b}c}",
                "{a,{b,}",
                "{{}}",
                "{{one}}",
            ]
        ]
    )
    def test_expand_braces_failure(self, value):
        with self.assertRaises(ValueError):
            util.expand_braces(value)


class TestQueryFormat(UnitTestCase):
    @parametrize(
        [
            (
                "SELECT id FROM {table}",
                [],
                {"table": "res_users"},
                'SELECT id FROM "res_users"',
            ),
            (
                "SELECT id FROM {1} WHERE {0} > 2",
                ["id", "res_users"],
                {},
                'SELECT id FROM "res_users" WHERE "id" > 2',
            ),
            (
                "SELECT id FROM {} WHERE {{parallel_filter}}",
                ["res_users"],
                {},
                'SELECT id FROM "res_users" WHERE {parallel_filter}',
            ),
            (
                "SELECT {col} FROM {table}",
                [],
                {"table": "res_users", "col": "id"},
                'SELECT "id" FROM "res_users"',
            ),
            ("{col} = 1", [], {"col": "X; fd"}, '"X; fd" = 1'),
            (
                "{col1} = {col2}",
                [],
                {"col2": "X; fd", "col1": "xxx"},
                '"xxx" = "X; fd"',
            ),
            (
                "WITH {cte} AS (SELECT 1) SELECT 2",
                [],
                {"cte": "some info"},
                'WITH "some info" AS (SELECT 1) SELECT 2',
            ),
            (
                "UPDATE res_users SET id = 2 WHERE {col} = %s",
                [],
                {"col": "Ab"},
                'UPDATE res_users SET id = 2 WHERE "Ab" = %s',
            ),
        ]
    )
    def test_format(self, query, args, kwargs, expected):
        cr = self.env.cr
        self.assertEqual(util.format_query(cr, query, *args, **kwargs), expected)

    def test_format_ColumnList(self):
        cr = self.env.cr

        ignored = ("id", "create_date", "create_uid", "write_date", "write_uid")

        columns = util.get_columns(cr, "ir_config_parameter", ignore=ignored)
        no_columns = util.get_columns(cr, "ir_config_parameter", ignore=(*ignored, "key", "value"))

        self.assertEqual(
            util.format_query(cr, "SELECT id, {c}", c=columns),
            'SELECT id, "key", "value"',
        )

        self.assertEqual(
            util.format_query(cr, "SELECT id {c}", c=columns.using(leading_comma=True)),
            'SELECT id , "key", "value"',
        )
        self.assertEqual(
            util.format_query(cr, "SELECT {c} id", c=columns.using(trailing_comma=True)),
            'SELECT "key", "value", id',
        )
        self.assertEqual(
            util.format_query(cr, "SELECT {c}", c=columns.using(alias="a")),
            'SELECT "a"."key", "a"."value"',
        )
        # leading/trailing comma only if list is not empty
        self.assertEqual(
            util.format_query(cr, "SELECT id {c}", c=no_columns.using(leading_comma=True)),
            "SELECT id ",
        )
        self.assertEqual(
            util.format_query(cr, "SELECT {c} id", c=no_columns.using(trailing_comma=True)),
            "SELECT  id",
        )


class TestReplaceRecordReferences(UnitTestCase):
    def test_m2m_no_conflict(self):
        cr = self.env.cr
        g1 = self.env["res.groups"].create({"name": "G1"})
        g2 = self.env["res.groups"].create({"name": "G2"})
        g3 = self.env["res.groups"].create({"name": "G3"})
        mapping = {g1.id: g3.id, g2.id: g3.id}

        u1 = self.env["res.users"].create({"login": "U1", "name": "U1"})
        u1.groups_id = g1 | g3
        self.assertEqual(u1.groups_id.ids, [g1.id, g3.id])
        util.replace_record_references_batch(cr, mapping, "res.groups")
        util.invalidate(u1)
        self.assertEqual(u1.groups_id.ids, [g3.id])

        u2 = self.env["res.users"].create({"login": "U2", "name": "U2"})
        u2.groups_id = g1 | g2
        self.assertEqual(u2.groups_id.ids, [g1.id, g2.id])
        util.replace_record_references_batch(cr, mapping, "res.groups")
        util.invalidate(u2)
        self.assertEqual(u2.groups_id.ids, [g3.id])
