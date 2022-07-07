import operator
from ast import literal_eval

try:
    from unittest import mock
except ImportError:
    import mock

from odoo.osv.expression import FALSE_LEAF, TRUE_LEAF

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
        func = "_read" if util.version_gte("saas~12.5") else "read"
        with mock.patch.object(Country, func, autospec=True, side_effect=getattr(Country, func)) as read:
            for c in util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size):
                c.name
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
            rowcount = util.parallel_execute(cr, util.explode_query(cr, query))
        self.assertEqual(rowcount, expected)


class TestORM(UnitTestCase):
    def test_create_cron(self):
        cr = self.env.cr
        util.create_cron(cr, "Test cron creation don't fail", "res.partner", "answer = 42")

        cron_id = util.ref(cr, "__upgrade__.cron_test_cron_creation_don_t_fail")
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
        result = sorted(list(util.inherit_parents(cr, model)))
        self.assertEqual(result, sorted(expected))


class TestNamedCursors(UnitTestCase):
    @staticmethod
    def exec(cr, which="", args=()):
        cr.execute("SELECT * FROM ir_ui_view")
        if which:
            return getattr(cr, which)(*args)

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

        country = self.env["res.country"].create({"name": "Test country 1", "currency_id": old.id})
        self.env["ir.model.data"].create(
            {"name": "test_country_1", "module": "base", "model": "res.country", "res_id": country.id}
        )

        # Wrong model
        with self.assertRaises(MigrationError):
            util.rename_xmlid(cr, "base.TX1", "base.test_country_1", on_collision="merge")

        # Collision
        with self.assertRaises(MigrationError):
            util.rename_xmlid(cr, "base.TX1", "base.TX2", on_collision="fail")

        # As TX2 is not free, TX1 is merged with TX2
        res = util.rename_xmlid(cr, "base.TX1", "base.TX2", on_collision="merge")
        self.assertEqual(res, new.id)
        self.assertEqual(util.ref(cr, "base.TX1"), None)

        # TX1 references moved to TX2
        cr.execute("SELECT currency_id FROM res_country WHERE id = %s", [country.id])
        self.assertEqual(cr.fetchall(), [(new.id,)])

        # Nothing left to rename in TX1
        res = util.rename_xmlid(cr, "base.TX1", "base.TX3", on_collision="merge")
        self.assertEqual(res, None)

        # Can rename to empty TX3 withouth need for merge
        res = util.rename_xmlid(cr, "base.TX2", "base.TX3", on_collision="merge")
        self.assertEqual(res, new.id)

        # Normal rename
        res = util.rename_xmlid(cr, "base.TX3", "base.TX4")
        self.assertEqual(res, new.id)
