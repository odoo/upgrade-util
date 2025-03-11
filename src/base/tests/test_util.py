import ast
import operator
import re
import sys
import threading
import unittest
import uuid
from ast import literal_eval
from contextlib import contextmanager

from lxml import etree

try:
    from unittest import mock
except ImportError:
    import mock

from odoo import modules
from odoo.tools import mute_logger
from odoo.tools.safe_eval import safe_eval

from odoo.addons.base.maintenance.migrations import util
from odoo.addons.base.maintenance.migrations.testing import UnitTestCase, parametrize
from odoo.addons.base.maintenance.migrations.util import snippets
from odoo.addons.base.maintenance.migrations.util.domains import (
    FALSE_LEAF,
    TRUE_LEAF,
    _adapt_one_domain,
    _model_of_path,
)
from odoo.addons.base.maintenance.migrations.util.exceptions import MigrationError

USE_ORM_DOMAIN = util.misc.version_gte("saas~18.2")
NOTNOT = () if USE_ORM_DOMAIN else ("!", "!")


@contextmanager
def without_testing():
    thread = threading.current_thread()
    testing = getattr(modules.module, "current_test", False) or getattr(thread, "testing", False)
    try:
        modules.module.current_test = False
        thread.testing = False
        yield
    finally:
        thread.testing = testing
        modules.module.current_test = testing


class TestAdaptOneDomain(UnitTestCase):
    def setUp(self):
        super(TestAdaptOneDomain, self).setUp()
        self.mock_adapter = mock.Mock()

    def test_adapt_renamed_field(self):
        domain = [("user_ids.partner_id.user_ids.partner_id", "=", False)]
        Filter = self.env["ir.filters"]
        filter1 = Filter.create(
            {"name": "Test filter for adapt domain", "model_id": "res.partner", "domain": str(domain)}
        )
        assert domain == ast.literal_eval(filter1.domain)
        util.invalidate(Filter)
        util.rename_field(self.cr, "res.partner", "user_ids", "renamed_user_ids")
        match_domain = [("renamed_user_ids.partner_id.renamed_user_ids.partner_id", "=", False)]
        new_domain = ast.literal_eval(filter1.domain)

        self.assertEqual(match_domain, new_domain)

    @parametrize(
        [
            ("res.currency", [], "res.currency"),
            ("res.currency", ["rate_ids"], "res.currency.rate"),
            ("res.currency", ("rate_ids", "company_id"), "res.company"),
            ("res.currency", ["rate_ids", "company_id", "user_ids"], "res.users"),
            ("res.currency", ("rate_ids", "company_id", "user_ids", "partner_id"), "res.partner"),
            ("res.users", ["partner_id"], "res.partner"),
            ("res.users", ["nonexistent_field"], None),
            ("res.users", ("partner_id", "active"), None),
            ("res.users", ("partner_id", "active", "name"), None),
            ("res.users", ("partner_id", "removed_field"), None),
        ]
    )
    def test_model_of_path(self, model, path, expected):
        cr = self.env.cr
        self.assertEqual(_model_of_path(cr, model, path), expected)

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
        match_domain = [*NOTNOT, ("partner_id.friend_id", "=", 2)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, False)
        self.assertEqual(match_domain, new_domain)

        # triple '!'
        self.mock_adapter.reset_mock()
        domain = ["!", "!", "!", ("partner_id.user_id", "=", 1)]
        match_domain = [*NOTNOT, "!", ("partner_id.friend_id", "=", 2)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, False, True)
        self.assertEqual(match_domain, new_domain)

        # '|' double '!'
        self.mock_adapter.reset_mock()
        domain = ["|", "!", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["|", *NOTNOT, ("partner_id.friend_id", "=", 2), ("name", "=", False)]
        new_domain = _adapt_one_domain(
            self.cr, "res.partner", "user_id", "friend_id", "res.users", domain, adapter=self.mock_adapter
        )
        self.mock_adapter.assert_called_with(term, True, False)
        self.assertEqual(match_domain, new_domain)

        # '&' double '!'
        self.mock_adapter.reset_mock()
        domain = ["&", "!", "!", ("partner_id.user_id", "=", 1), ("name", "=", False)]
        match_domain = ["&", *NOTNOT, ("partner_id.friend_id", "=", 2), ("name", "=", False)]
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

    @parametrize(
        [
            ("partner_id.old", "new"),
            ("partner_id.user_id.partner_id.old", "partner_id.user_id.new"),
            ("partner_id.old.foo", "new.foo"),
            # from another model
            ("user_id.partner_id.old", "user_id.new", "res.partner"),
            # no change expected
            ("old", None),
            ("partner_id", None),
            ("partner_id.name", None),
        ]
    )
    def test_dotted_old(self, left, expected, model="res.users"):
        domain = [(left, "=", "test")]
        new_domain = _adapt_one_domain(self.cr, "res.users", "partner_id.old", "new", model, domain)
        if expected is not None:
            self.assertEqual(new_domain, [(expected, "=", "test")])
        else:
            self.assertIsNone(new_domain)

    @unittest.skipUnless(util.version_gte("17.0"), "`any` operator only supported from Odoo 17")
    def test_any_operator(self):
        domain = [("partner_id", "any", [("complete_name", "=", "Odoo")])]
        expected = [("partner_id", "any", [("full_name", "=", "Odoo")])]

        new_domain = _adapt_one_domain(self.cr, "res.partner", "complete_name", "full_name", "res.company", domain)
        self.assertEqual(new_domain, expected)

        # test it also works recursively
        domain = [("partner_id", "any", [("bank_ids", "not any", [("acc_number", "like", "S.A.")])])]
        expected = [("partner_id", "any", [("bank_ids", "not any", [("acc_nbr", "like", "S.A.")])])]

        new_domain = _adapt_one_domain(self.cr, "res.partner.bank", "acc_number", "acc_nbr", "res.company", domain)
        self.assertEqual(new_domain, expected)


class TestAdaptDomainView(UnitTestCase):
    def test_adapt_domain_view(self):
        tag = "list" if util.version_gte("saas~17.5") else "tree"
        view_form = self.env["ir.ui.view"].create(
            {
                "name": "test_adapt_domain_view_form",
                "model": "res.currency",
                "arch": f"""\
                <form>
                  <field name="rate_ids">
                    <{tag}>
                      <field name="company_id" domain="[('email', '!=', False)]"/>
                      <field name="company_id" domain="[('email', 'not like', 'odoo.com')]"/>
                    </{tag}>
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


@unittest.skipUnless(
    util.version_gte("13.0"), "This test is incompatible with old style odoo.addons.base.maintenance.migrations.util"
)
class TestReplaceReferences(UnitTestCase):
    def setUp(self):
        super().setUp()
        self.env.cr.execute(
            """
            CREATE TABLE dummy_model(
                             id serial PRIMARY KEY,
                             res_id int,
                             res_model varchar,
                             extra varchar,
                             CONSTRAINT uniq_constr UNIQUE(res_id, res_model, extra)
                         );

            INSERT INTO dummy_model(res_model, res_id, extra)
                 VALUES -- the target is there with same res_id
                        ('res.users', 1, 'x'),
                        ('res.partner', 1, 'x'),

                        -- two with same target and the target is there
                        ('res.users', 2, 'x'),
                        ('res.users', 3, 'x'),
                        ('res.partner', 2, 'x'),

                        -- two with same target and the target is not there
                        ('res.users', 4, 'x'),
                        ('res.users', 5, 'x'),

                        -- target is there different res_id
                        ('res.users', 6, 'x'),
                        ('res.partner', 4, 'x')
            """
        )

    def _ir_dummy(self, cr, bound_only=True):
        yield util.IndirectReference("dummy_model", "res_model", "res_id")

    def test_replace_record_references_batch__full_unique(self):
        cr = self.env.cr
        mapping = {1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4}
        with mock.patch("odoo.upgrade.util.records.indirect_references", self._ir_dummy):
            util.replace_record_references_batch(cr, mapping, "res.users", "res.partner")

        cr.execute("SELECT res_model, res_id, extra FROM dummy_model ORDER BY res_id, res_model")
        data = cr.fetchall()
        expected = [
            ("res.partner", 1, "x"),
            ("res.partner", 2, "x"),
            ("res.partner", 3, "x"),
            ("res.partner", 4, "x"),
        ]
        self.assertEqual(data, expected)


class TestRemoveFieldDomains(UnitTestCase):
    @parametrize(
        [
            ([("updated", "=", 0)], [TRUE_LEAF]),
            # operator is not relevant
            ([("updated", "!=", 0)], [TRUE_LEAF]),
            # if negate we should end with "not false"
            (["!", ("updated", "!=", 0)], [TRUE_LEAF] if USE_ORM_DOMAIN else ["!", FALSE_LEAF]),
            # multiple !, we should still end with a true leaf
            (["!", "!", ("updated", ">", 0)], [*NOTNOT, TRUE_LEAF]),
            # with operator
            ([("updated", "=", 0), ("state", "=", "done")], ["&", TRUE_LEAF, ("state", "=", "done")]),
            (["&", ("updated", "=", 0), ("state", "=", "done")], ["&", TRUE_LEAF, ("state", "=", "done")]),
            (["|", ("updated", "=", 0), ("state", "=", "done")], ["|", FALSE_LEAF, ("state", "=", "done")]),
            # in second operand
            (["&", ("state", "=", "done"), ("updated", "=", 0)], ["&", ("state", "=", "done"), TRUE_LEAF]),
            (["|", ("state", "=", "done"), ("updated", "=", 0)], ["|", ("state", "=", "done"), FALSE_LEAF]),
            # combination with !
            (
                ["&", "!", ("updated", "=", 0), ("state", "=", "done")],
                ["&", TRUE_LEAF, ("state", "=", "done")]
                if USE_ORM_DOMAIN
                else ["&", "!", FALSE_LEAF, ("state", "=", "done")],
            ),
            (
                ["|", "!", ("updated", "=", 0), ("state", "=", "done")],
                ["|", FALSE_LEAF, ("state", "=", "done")]
                if USE_ORM_DOMAIN
                else ["|", "!", TRUE_LEAF, ("state", "=", "done")],
            ),
            # here, the ! apply on the whole &/| and should not invert the replaced leaf
            (
                ["!", "&", ("updated", "=", 0), ("state", "=", "done")],
                ["|", FALSE_LEAF, ("state", "!=", "done")]
                if USE_ORM_DOMAIN
                else ["!", "&", TRUE_LEAF, ("state", "=", "done")],
            ),
            (
                ["!", "|", ("updated", "=", 0), ("state", "=", "done")],
                ["&", TRUE_LEAF, ("state", "!=", "done")]
                if USE_ORM_DOMAIN
                else ["!", "|", FALSE_LEAF, ("state", "=", "done")],
            ),
        ]
    )
    def test_remove_field(self, domain, expected):
        cr = self.env.cr
        cr.execute(
            "INSERT INTO ir_filters(name, model_id, domain, context, sort)"
            "     VALUES ('test', 'base.module.update', %s, '{}', '[]') RETURNING id",
            [str(domain)],
        )
        (filter_id,) = cr.fetchone()

        util.remove_field(cr, "base.module.update", "updated")

        cr.execute("SELECT domain FROM ir_filters WHERE id = %s", [filter_id])
        altered_domain = literal_eval(cr.fetchone()[0])

        self.assertEqual(altered_domain, expected)


class TestIrExports(UnitTestCase):
    def setUp(self):
        super().setUp()
        self.export = self.env["ir.exports"].create(
            [
                {
                    "name": "Test currency export",
                    "resource": "res.currency",
                    "export_fields": [
                        (0, 0, {"name": "full_name"}),
                        (0, 0, {"name": "rate_ids/company_id/user_ids/name"}),
                        (0, 0, {"name": "rate_ids/company_id/user_ids/partner_id/user_ids/name"}),
                        (0, 0, {"name": "rate_ids/name"}),
                        (0, 0, {"name": "rate_ids/company_id/user_ids/partner_id/user_ids/.id"}),
                    ],
                }
            ]
        )
        util.flush(self.export)

    def _invalidate(self):
        util.invalidate(self.export.export_fields)
        util.invalidate(self.export)

    def test_rename_field(self):
        util.rename_field(self.cr, "res.partner", "user_ids", "renamed_user_ids")
        self._invalidate()
        self.assertEqual(
            self.export.export_fields[2].name, "rate_ids/company_id/user_ids/partner_id/renamed_user_ids/name"
        )
        self.assertEqual(
            self.export.export_fields[4].name, "rate_ids/company_id/user_ids/partner_id/renamed_user_ids/.id"
        )

        util.rename_field(self.cr, "res.users", "name", "new_name")
        self._invalidate()
        self.assertEqual(self.export.export_fields[1].name, "rate_ids/company_id/user_ids/new_name")

    def test_remove_field(self):
        util.remove_field(self.cr, "res.currency.rate", "company_id")
        self._invalidate()
        self.assertEqual(len(self.export.export_fields), 2)
        self.assertEqual(self.export.export_fields[0].name, "full_name")
        self.assertEqual(self.export.export_fields[1].name, "rate_ids/name")

    def test_rename_model(self):
        util.rename_model(self.cr, "res.currency", "res.currency2")
        self._invalidate()
        self.assertEqual(self.export.resource, "res.currency2")

    def test_remove_model(self):
        util.remove_model(self.cr, "res.currency.rate")
        self._invalidate()
        self.assertEqual(len(self.export.export_fields), 1)
        self.assertEqual(self.export.export_fields[0].name, "full_name")

        util.remove_model(self.cr, "res.currency")
        self.cr.execute("SELECT * FROM ir_exports WHERE id = %s", [self.export.id])
        self.assertFalse(self.cr.fetchall())


class TestBaseImportMappings(UnitTestCase):
    def setUp(self):
        super().setUp()
        self.import_mapping = self.env["base_import.mapping"].create(
            [
                {"res_model": "res.currency", "column_name": "Column name", "field_name": path}
                for path in [
                    "full_name",
                    "rate_ids/company_id/user_ids/name",
                    "rate_ids/company_id/user_ids/partner_id/user_ids/name",
                    "rate_ids/name",
                ]
            ]
        )

        util.flush(self.import_mapping)

    def test_rename_field(self):
        util.rename_field(self.cr, "res.partner", "user_ids", "renamed_user_ids")
        util.invalidate(self.import_mapping)

        self.assertEqual(
            self.import_mapping[2].field_name, "rate_ids/company_id/user_ids/partner_id/renamed_user_ids/name"
        )

        util.rename_field(self.cr, "res.users", "name", "new_name")
        util.invalidate(self.import_mapping)

        self.assertEqual(self.import_mapping[1].field_name, "rate_ids/company_id/user_ids/new_name")

    def test_remove_field(self):
        prev_mappings = self.env["base_import.mapping"].search([])

        util.remove_field(self.cr, "res.currency.rate", "company_id")
        util.invalidate(self.import_mapping)

        removed_mappings = prev_mappings - self.env["base_import.mapping"].search([])
        remaining_mappings = self.import_mapping - removed_mappings

        self.assertEqual(len(removed_mappings), 2)
        self.assertEqual(remaining_mappings[0].field_name, "full_name")
        self.assertEqual(remaining_mappings[1].field_name, "rate_ids/name")

    def test_rename_model(self):
        util.rename_model(self.cr, "res.currency", "res.currency2")
        util.invalidate(self.import_mapping)

        self.assertEqual(self.import_mapping[0].res_model, "res.currency2")

    def test_remove_model(self):
        prev_mappings = self.env["base_import.mapping"].search([])

        util.remove_model(self.cr, "res.currency.rate")
        util.invalidate(self.import_mapping)

        removed_mappings = prev_mappings - self.env["base_import.mapping"].search([])
        remaining_mappings = self.import_mapping - removed_mappings

        self.assertEqual(len(removed_mappings), 3)
        self.assertEqual(remaining_mappings[0].field_name, "full_name")

        util.remove_model(self.cr, "res.currency")
        self.cr.execute("SELECT * FROM base_import_mapping WHERE id = %s", [remaining_mappings.id])
        self.assertFalse(self.cr.fetchall())


class TestIterBrowse(UnitTestCase):
    def test_iter_browse_iter(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for (c,) in cr.fetchall()]
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
        ids = [c for (c,) in cr.fetchall()]
        chunk_size = 10

        Country = type(self.env["res.country"])
        with mock.patch.object(Country, "write", autospec=True, side_effect=Country.write) as write:
            ib = util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size)
            ib.write({"vat_label": "VAT"})

        expected = (len(ids) + chunk_size - 1) // chunk_size
        self.assertEqual(write.call_count, expected)

    def test_iter_browse_create_non_empty(self):
        RP = self.env["res.partner"]
        with self.assertRaises(ValueError):
            util.iter_browse(RP, [42]).create([{}])

    @parametrize([(True,), (False,)])
    def test_iter_browse_create(self, multi):
        chunk_size = 2
        RP = self.env["res.partner"]

        names = [f"Name {i}" for i in range(7)]
        ib = util.iter_browse(RP, [], chunk_size=chunk_size)
        records = ib.create([{"name": name} for name in names], multi=multi)
        self.assertEqual([t.name for t in records], names)

    def test_iter_browse_iter_twice(self):
        cr = self.env.cr
        cr.execute("SELECT id FROM res_country")
        ids = [c for (c,) in cr.fetchall()]
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
        ids = [c for (c,) in cr.fetchall()]
        chunk_size = 10

        ib = util.iter_browse(self.env["res.country"], ids, logger=None, chunk_size=chunk_size)
        ib.write({"vat_label": "VAT"})

        with self.assertRaises(RuntimeError):
            ib.write({"name": "FAIL"})


class TestPG(UnitTestCase):
    def test_alter_column_type(self):
        cr = self.env.cr
        cr.execute(
            """
            ALTER TABLE res_partner_bank ADD COLUMN x bool;

            UPDATE res_partner_bank
               SET x = CASE id % 3
                           WHEN 1 THEN NULL
                           WHEN 2 THEN True
                           ELSE False
                       END
            """
        )
        self.assertEqual(util.column_type(cr, "res_partner_bank", "x"), "bool")
        util.alter_column_type(cr, "res_partner_bank", "x", "int", using="CASE {0} WHEN True THEN 2 ELSE 1 END")
        self.assertEqual(util.column_type(cr, "res_partner_bank", "x"), "int4")
        cr.execute("SELECT id, x FROM res_partner_bank")
        data = cr.fetchall()
        self.assertTrue(
            all(x == 1 or (x == 2 and id_ % 3 == 2) for id_, x in data),
            "Some values where not casted correctly via USING",
        )

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

    def _get_cr(self):
        cr = self.registry.cursor()
        self.addCleanup(cr.close)
        return cr

    def test_explode_mult_filters(self):
        cr = self._get_cr()
        queries = util.explode_query_range(
            cr,
            """
            WITH cte1 AS (
                SELECT id,
                       login
                  FROM res_users
                 WHERE {parallel_filter}
            ), cte2 AS (
                SELECT id,
                       login
                  FROM res_users
                 WHERE {parallel_filter}
            ) SELECT u.login = cte1.login AND u.login = cte2.login
                FROM cte1
           LEFT JOIN cte2
                  ON cte2.id = cte1.id
                JOIN res_users u
                  ON u.id = cte1.id
            """,
            table="res_users",
            bucket_size=4,
        )
        for q in queries:
            cr.execute(q)
            self.assertTrue(all(x for (x,) in cr.fetchall()))

    @mute_logger("odoo.upgrade.util.pg.explode_query_range")
    def test_explode_query_range(self):
        cr = self.env.cr

        cr.execute("SELECT count(id) FROM res_partner_category")
        count = cr.fetchone()[0]
        # ensure there start with at least 10 records
        for _ in range(10 - count):
            count += 1
            self.env["res.partner.category"].create({"name": "x"})

        # set one record with very high id
        tid = self.env["res.partner.category"].create({"name": "x"}).id
        count += 1
        cr.execute("UPDATE res_partner_category SET id = 10000000 WHERE id = %s", [tid])

        qs = util.explode_query_range(cr, "SELECT 1", table="res_partner_category", bucket_size=count)
        self.assertEqual(len(qs), 1)  # one bucket should be enough for all records

        qs = util.explode_query_range(cr, "SELECT 1", table="res_partner_category", bucket_size=count - 1)
        self.assertEqual(len(qs), 1)  # 10% rule for second bucket, 1 <= 0.1(count - 1) since count >= 11

    def test_parallel_rowcount(self):
        cr = self._get_cr()
        cr.execute("SELECT count(*) FROM res_lang")
        [expected] = cr.fetchone()

        # util.parallel_execute will `commit` the cursor and create new ones
        # as we are in a test, we should not commit as we are in a subtransaction
        with mock.patch.object(cr, "commit", lambda: ...):
            query = "UPDATE res_lang SET name = name"
            rowcount = util.explode_execute(cr, query, table="res_lang", bucket_size=10)
        self.assertEqual(rowcount, expected)

    def test_parallel_rowcount_threaded(self):
        with without_testing():
            self.test_parallel_rowcount()

    def test_parallel_execute_retry_on_serialization_failure(self):
        TEST_TABLE_NAME = "_upgrade_serialization_failure_test_table"
        N_ROWS = 10

        cr = self._get_cr()

        cr.execute(
            util.format_query(
                cr,
                """
                DROP TABLE IF EXISTS {table};

                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    other_id INTEGER,
                    FOREIGN KEY (other_id) REFERENCES {table} ON DELETE CASCADE
                );

                INSERT INTO {table} SELECT GENERATE_SERIES(1, %s);

                -- map odd numbers `n` to `n + 1` and viceversa (`n + 1` to `n`)
                UPDATE {table} SET other_id = id + (MOD(id, 2) - 0.5)*2;
                """
                % N_ROWS,
                table=TEST_TABLE_NAME,
            )
        )

        # exploded queries will generate a SerializationFailed error, causing some of the queries to be retried
        with without_testing(), mute_logger(util.pg._logger.name, "odoo.sql_db"):
            util.explode_execute(
                cr, util.format_query(cr, "DELETE FROM {}", TEST_TABLE_NAME), TEST_TABLE_NAME, bucket_size=1
            )

        if hasattr(self, "_savepoint_id"):
            # `explode_execute` causes the cursor to be committed, losing the automatic checkpoint
            # Force a new one to avoid issues when cleaning up
            self.addCleanup(cr.execute, f"SAVEPOINT test_{self._savepoint_id}")
            self.addCleanup(cr.execute, util.format_query(cr, "DROP TABLE IF EXISTS {}", TEST_TABLE_NAME))

        cr.execute(util.format_query(cr, "SELECT 1 FROM {}", TEST_TABLE_NAME))
        self.assertFalse(cr.rowcount)

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

    def test_ColumnList(self):
        cr = self.env.cr

        s = lambda c: c.as_string(cr._cnx)

        columns = util.ColumnList(["a", "A"], ['"a"', '"A"'])
        self.assertEqual(len(columns), 2)

        columns2 = util.ColumnList.from_unquoted(cr, ["a", "A"])
        self.assertEqual(columns2, columns)

        # iterating it yield quoted columns
        self.assertEqual(list(iter(columns)), ['"a"', '"A"'])

        self.assertEqual(list(columns.iter_unquoted()), ["a", "A"])

        self.assertEqual(s(columns), '"a", "A"')

        self.assertEqual(s(columns.using(alias="t")), '"t"."a", "t"."A"')
        self.assertEqual(s(columns.using(leading_comma=True)), ', "a", "A"')
        self.assertEqual(s(columns.using(trailing_comma=True)), '"a", "A",')
        self.assertEqual(s(columns.using(leading_comma=True, trailing_comma=True)), ', "a", "A",')

        self.assertIs(columns.using(), columns)

        ulc = columns.using(leading_comma=True)
        self.assertTrue(s(ulc.using(alias="x")), ', "x"."a", "x"."A"')
        self.assertIs(ulc, ulc.using(leading_comma=True))


class TestORM(UnitTestCase):
    def test_create_cron(self):
        cr = self.env.cr
        util.create_cron(cr, "Test cron creation don't fail", "res.partner", "answer = 42")

        cron_id = util.ref(cr, "__upgrade__.cron_post_upgrade_test_cron_creation_don_t_fail")
        self.assertIsNotNone(cron_id)
        cron = self.env["ir.cron"].browse(cron_id)
        self.assertEqual(cron.code, "answer = 42")


class TestField(UnitTestCase):
    def test_invert_boolean_field(self):
        cr = self.env.cr

        with self.assertRaises(ValueError):
            util.invert_boolean_field(cr, "res.partner", "name", "nom")

        model, old_name, new_name = "ir.model.access", "perm_unlink", "perm_delete"
        table = util.table_of_model(cr, model)

        fltr = self.env["ir.filters"].create(
            {"name": "test", "model_id": model, "domain": str([(old_name, "=", True)])}
        )

        query = """
            SELECT {1}, count(*)
              FROM {0}
          GROUP BY {1}
        """

        cr.execute(util.format_query(cr, query, table, old_name))
        initial_repartition = dict(cr.fetchall())

        # util.parallel_execute will `commit` the cursor and create new ones
        # as we are in a test, we should not commit as we are in a subtransaction
        with mock.patch.object(cr, "commit", lambda: ...):
            util.invert_boolean_field(cr, model, old_name, new_name)

        util.invalidate(fltr)
        expected = ["!", (new_name, "=", True)]
        self.assertEqual(literal_eval(fltr.domain), expected)

        cr.execute(util.format_query(cr, query, table, new_name))
        inverted_repartition = dict(cr.fetchall())

        self.assertEqual(inverted_repartition[False], initial_repartition[True])
        self.assertEqual(inverted_repartition[True], initial_repartition[False] + initial_repartition.get(None, 0))
        self.assertEqual(inverted_repartition.get(None, 0), 0)

        # rename back
        with mock.patch.object(cr, "commit", lambda: ...):
            util.rename_field(cr, model, new_name, old_name)

        util.invalidate(fltr)
        expected = [(old_name, "!=", True)] if USE_ORM_DOMAIN else ["!", (old_name, "=", True)]
        self.assertEqual(literal_eval(fltr.domain), expected)

        # invert with same name; will invert domains and data
        with mock.patch.object(cr, "commit", lambda: ...):
            util.invert_boolean_field(cr, model, old_name, old_name)

        util.invalidate(fltr)
        expected = ["!", (old_name, "!=", True)] if USE_ORM_DOMAIN else ["!", "!", (old_name, "=", True)]
        self.assertEqual(literal_eval(fltr.domain), expected)

        cr.execute(util.format_query(cr, query, table, old_name))
        back_repartition = dict(cr.fetchall())

        # merge None into False in the initial repartition
        initial_repartition[False] += initial_repartition.pop(None, 0)
        self.assertEqual(back_repartition, initial_repartition)


class TestHelpers(UnitTestCase):
    def test_model_table_conversion(self):
        cr = self.env.cr
        for model in self.env.registry:
            if model in ("ir.actions.act_window_close",):
                continue
            table = util.table_of_model(cr, model)
            self.assertEqual(table, self.env[model]._table)
            self.assertEqual(util.model_of_table(cr, table), model)

    def test_resolve_model_fields_path(self):
        cr = self.env.cr

        # test with provided paths
        model, path = "res.currency", ["rate_ids", "company_id", "user_ids", "partner_id"]
        expected_result = [
            util.FieldsPathPart("res.currency", "rate_ids", "res.currency.rate"),
            util.FieldsPathPart("res.currency.rate", "company_id", "res.company"),
            util.FieldsPathPart("res.company", "user_ids", "res.users"),
            util.FieldsPathPart("res.users", "partner_id", "res.partner"),
        ]
        result = util.resolve_model_fields_path(cr, model, path)
        self.assertEqual(result, expected_result)

        model, path = "res.users", ("partner_id", "removed_field", "user_id")
        expected_result = [util.FieldsPathPart("res.users", "partner_id", "res.partner")]
        result = util.resolve_model_fields_path(cr, model, path)
        self.assertEqual(result, expected_result)


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
    def exec(cr, which="", args=()):
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
            # As the `rename_xmlid` method uses `parallel_execute`, the cursor is committed; which kill
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
        xmlid = "base.res_partner_industry_A"
        data_after = {"name": "42", "full_name": "Fortytwo"}
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

        xmlid = "base.res_partner_industry_A"
        util.update_record_from_xml(self.env.cr, xmlid, reset_translations=True)

        # change the translations to something arbitrary for all installed languages
        langs = self.env["res.lang"].get_installed()
        filter_lang = [code for code, _ in langs]
        self.assertIn(be_lang.code, filter_lang)
        data_after = {"name": "42", "full_name": "Fortytwo"}
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
        if not util.module_installed(cr, "mail"):
            self.skipTest("module `mail` not installed")

        xmlid = "base.action_attachment"
        action = self.env.ref(xmlid)
        new_help = "<p>test_update_record_from_xml__from_module</p>"

        action.write({"help": new_help})
        util.flush(action)
        util.invalidate(action)

        util.update_record_from_xml(cr, xmlid, from_module="mail")
        util.invalidate(action)

        self.assertNotEqual(action.help, new_help)
        # the `mail` module overwrite the record to remove the second paragraphe of the help message.
        # ensure it actually update the record from the `mail` module.
        self.assertEqual(action.help.count("</p>"), 1)

    def test_update_record_from_xml_bad_match(self):
        cr = self.env.cr
        if not util.module_installed(cr, "web"):
            self.skipTest("module `web` not installed")

        xmlid = "web.login"
        util.update_record_from_xml(cr, xmlid)

        arch = self.env.ref(xmlid).arch_db
        tree = etree.fromstring(arch)
        self.assertIsNotNone(tree.find(".//input[@id='login']"))

    def test_update_record_from_xml__fields(self):
        cr = self.env.cr
        xmlid = "base.USD"

        usd = self.env.ref(xmlid)
        usd.write({"name": "XXX", "symbol": "¤"})
        util.flush(usd)
        util.invalidate(usd)

        util.update_record_from_xml(cr, xmlid, fields=("symbol"))
        util.invalidate(usd)

        self.assertEqual(usd.symbol, "$")
        self.assertEqual(usd.name, "XXX")

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
        test_partner_category = self.env["res.partner.category"].create(
            {"name": r"""object.number '<"x">\y object.numbercombined"""}
        )

        pattern_old = re.compile(r"\b\.number\b")
        pattern_new = re.compile(r"\b\.name\b")
        pattern_notouch = re.compile(r"\b\.numbercombined\b")

        self.assertNotRegex(test_partner_category.name, pattern_new)
        self.assertRegex(test_partner_category.name, pattern_notouch)
        self.assertRegex(test_partner_category.name, pattern_old)

        extra_filter = self.env.cr.mogrify("t.id = %s", (test_partner_category.id,)).decode()
        util.replace_in_all_jsonb_values(self.env.cr, "res_partner_category", "name", ".number", ".name", extra_filter)
        util.replace_in_all_jsonb_values(
            self.env.cr, "res_partner_category", "name", r"""'<"x">\y""", "GONE", extra_filter
        )
        test_partner_category.invalidate_recordset(["name"])

        self.assertRegex(test_partner_category.name, pattern_new)
        self.assertRegex(test_partner_category.name, pattern_notouch)
        self.assertNotRegex(test_partner_category.name, pattern_old)
        # ensure replacing works for patterns that do not start with a valid word start \w
        # also ensure the replace works for multiple embedded quotes
        self.assertEqual(test_partner_category.name, "object.name GONE object.numbercombined")

    def test_replace_record_references_batch__uniqueness(self):
        c1 = self.env["res.country"].create(
            {"name": "TEST1", "code": "T1", "state_ids": [(0, 0, {"name": "STATE1", "code": "STATE"})]}
        )
        c2 = self.env["res.country"].create(
            {"name": "TEST2", "code": "T2", "state_ids": [(0, 0, {"name": "STATE2", "code": "STATE"})]}
        )

        # `sale_subscription` as foreign key on `res_country`
        # ignore it to avoid the logging of an error in `model_of_table` when upgrading to v16 where the model
        # `sale.subscription` as been removed but the table kept.
        ignores = ["sale_subscription"]
        util.replace_record_references_batch(self.env.cr, {c2.id: c1.id}, "res.country", ignores=ignores)
        self.env.cr.execute("SELECT count(1) FROM res_country_state WHERE country_id=%s", [c1.id])
        [count] = self.env.cr.fetchone()
        self.assertEqual(count, 1)

    def _prepare_test_delete_unused(self):
        def create_cat():
            name = f"test_{uuid.uuid4().hex}"
            cat = self.env["res.partner.category"].create({"name": name})
            self.env["ir.model.data"].create(
                {"name": name, "module": "base", "model": "res.partner.category", "res_id": cat.id}
            )
            return cat

        cat_1 = create_cat()
        cat_2 = create_cat()
        cat_3 = create_cat()

        # `category_id` is a m2m, so in ON DELETE CASCADE. We need a m2o.
        self.env.cr.execute(
            "ALTER TABLE res_partner ADD COLUMN _cat_id integer REFERENCES res_partner_category(id) ON DELETE SET NULL"
        )
        p1 = self.env["res.partner"].create({"name": "test delete_unused"})

        # set the `_cat_id` value in SQL as it is not know by the ORM
        self.env.cr.execute("UPDATE res_partner SET _cat_id=%s WHERE id=%s", [cat_1.id, p1.id])

        if hasattr(self, "_savepoint_id"):
            self.addCleanup(self.env.cr.execute, f"SAVEPOINT test_{self._savepoint_id}")
            self.addCleanup(cat_1.unlink)
            self.addCleanup(cat_2.unlink)
            self.addCleanup(cat_3.unlink)
            self.addCleanup(p1.unlink)

        self.addCleanup(self.env.cr.execute, "ALTER TABLE res_partner DROP COLUMN _cat_id")

        return cat_1, cat_2, cat_3

    def test_delete_unused_base(self):
        tx = self.env["res.currency"].create({"name": "TX1", "symbol": "TX1"})
        self.env["ir.model.data"].create({"name": "TX1", "module": "base", "model": "res.currency", "res_id": tx.id})

        deleted = util.delete_unused(self.env.cr, "base.TX1")
        self.assertEqual(deleted, ["base.TX1"])
        self.assertFalse(tx.exists())

    def test_delete_unused_cascade(self):
        cat_1, cat_2, cat_3 = self._prepare_test_delete_unused()
        deleted = util.delete_unused(self.env.cr, f"base.{cat_1.name}", f"base.{cat_2.name}", f"base.{cat_3.name}")

        self.assertEqual(set(deleted), {f"base.{cat_2.name}", f"base.{cat_3.name}"})
        self.assertTrue(cat_1.exists())
        self.assertFalse(cat_2.exists())
        self.assertFalse(cat_3.exists())

    def test_delete_unused_tree(self):
        cat_1, cat_2, cat_3 = self._prepare_test_delete_unused()

        cat_1.parent_id = cat_2.id
        cat_2.parent_id = cat_3.id
        util.flush(cat_1)
        util.flush(cat_2)

        deleted = util.delete_unused(self.env.cr, f"base.{cat_3.name}")

        self.assertEqual(deleted, [])
        self.assertTrue(cat_1.exists())
        self.assertTrue(cat_2.exists())
        self.assertTrue(cat_3.exists())

    def test_delete_unused_multi_cascade_fk(self):
        """
        When there are multiple children, the hierarchy can be build from different columns

            cat_3
              | via `_test_id`
            cat_2
              | via `parent_id`
            cat_1
        """
        cat_1, cat_2, cat_3 = self._prepare_test_delete_unused()

        self.env.cr.execute(
            "ALTER TABLE res_partner_category ADD COLUMN _test_id integer REFERENCES res_partner_category(id) ON DELETE CASCADE"
        )
        self.addCleanup(self.env.cr.execute, "ALTER TABLE res_partner_category DROP COLUMN _test_id")

        cat_1.parent_id = cat_2.id
        util.flush(cat_1)
        self.env.cr.execute("UPDATE res_partner_category SET _test_id = %s WHERE id = %s", [cat_3.id, cat_2.id])

        deleted = util.delete_unused(self.env.cr, f"base.{cat_3.name}")

        self.assertEqual(deleted, [])
        self.assertTrue(cat_1.exists())
        self.assertTrue(cat_2.exists())
        self.assertTrue(cat_3.exists())


class TestEditView(UnitTestCase):
    @parametrize(
        [
            (True, True, True),
            (False, True, False),
        ]
    )
    def test_active_auto(self, initial_value, by_xmlid, by_view_id):
        cr = self.env.cr
        xmlid = "base.view_view_form"
        view_id = util.ref(cr, xmlid)

        cr.execute("UPDATE ir_ui_view SET active = %s WHERE id = %s", [initial_value, view_id])

        # call by xmlid
        with util.edit_view(cr, xmlid=xmlid, skip_if_not_noupdate=False, active="auto"):
            pass

        cr.execute("SELECT active FROM ir_ui_view WHERE id = %s", [view_id])
        self.assertEqual(cr.fetchone()[0], by_xmlid)

        # reset value
        cr.execute("UPDATE ir_ui_view SET active = %s WHERE id = %s", [initial_value, view_id])

        # call by view_id
        with util.edit_view(cr, view_id=view_id, active="auto"):
            pass

        cr.execute("SELECT active FROM ir_ui_view WHERE id = %s", [view_id])
        self.assertEqual(cr.fetchone()[0], by_view_id)

    @parametrize(
        [
            (True, True, True),
            (True, False, False),
            (True, None, True),
            (False, True, True),
            (False, False, False),
            (False, None, False),
        ]
    )
    def test_active_explicit(self, initial_value, value, expected_value):
        cr = self.env.cr
        xmlid = "base.view_view_form"
        view_id = util.ref(cr, xmlid)

        cr.execute("UPDATE ir_ui_view SET active = %s WHERE id = %s", [initial_value, view_id])

        # call by xmlid
        with util.edit_view(cr, xmlid=xmlid, skip_if_not_noupdate=False, active=value):
            pass

        cr.execute("SELECT active FROM ir_ui_view WHERE id = %s", [view_id])
        self.assertEqual(cr.fetchone()[0], expected_value)

        # reset value
        cr.execute("UPDATE ir_ui_view SET active = %s WHERE id = %s", [initial_value, view_id])

        # call by view_id
        with util.edit_view(cr, view_id=view_id, active=value):
            pass

        cr.execute("SELECT active FROM ir_ui_view WHERE id = %s", [view_id])
        self.assertEqual(cr.fetchone()[0], expected_value)


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

    @parametrize(
        [
            (value, value)
            for value in [
                "a",
                "a.b",
                "a.b()",
                "a.b(c)",
                "a[b]",
                "context['company_id']",
                "[('company_id', 'in', company_ids)]",
                "[]",
            ]
        ]
        + [(f"a {op} 4", f"(a {op} 4)") for op in ["+", "-", "*", "/", "//", "%", "**"]]
        + [(f"4 {op} b", f"(4 {op} b)") for op in ["+", "-", "*", "/", "//", "%", "**"]]
        + [
            ("a+b*c", "(a + (b * c))"),
            ("a+b/c-d", "((a + (b / c)) - d)"),
            ("(a+b) * c", "((a + b) * c)"),
            ("+a", "+(a)"),
            ("-a", "-(a)"),
            ("-(a+b)", "-((a + b))"),
        ]
    )
    def test_SelfPrint(self, value, expected):
        evaluated = safe_eval(value, util.SelfPrintEvalContext(), nocopy=True)
        self.assertEqual(str(evaluated), expected, "Self printed result differs")

        replaced_value, ctx = util.SelfPrintEvalContext.preprocess(value)
        evaluated = safe_eval(replaced_value, ctx, nocopy=True)
        self.assertEqual(str(evaluated), expected, "Prepared self printed result differs")

    @parametrize(
        [
            (value, value)
            for value in [
                # splat
                "[('company_id', 'in', [*company_ids, False])]",
                "[('company_id', 'in', [False, *company_ids])]",
                # bool conversions
                "not a",
                "a and b",
                "a or b",
            ]
        ]
    )
    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    def test_SelfPrint_prepare(self, value, expected):
        replaced_value, ctx = util.SelfPrintEvalContext.preprocess(value)
        evaluated = safe_eval(replaced_value, ctx, nocopy=True)
        # extra fallback for old unparse from astunparse package
        self.assertIn(str(evaluated), [expected, "({})".format(expected)])

    @parametrize(
        [
            (value,)
            for value in [
                # iterators
                "[a.b for a in b]",
                "4 in b",
            ]
        ]
    )
    def test_SelfPrint_failure(self, value):
        # note: `safe_eval` will re-raise a ValueError
        with self.assertRaises(ValueError):
            safe_eval(value, util.SelfPrintEvalContext(), nocopy=True)

    @parametrize(
        [
            ("[('company_id','in',allowed_company_ids)]", "[('company_id', 'in', companies.active_ids)]"),
            (
                "[('company_id','in',allowed_company_ids or [False])]",
                "[('company_id', 'in', companies.active_ids or [False])]",
                "[('company_id', 'in', (companies.active_ids or [False]))]",
            ),
            (
                "[('company_id','in',    user.other.allowed_company_ids)]",
                # note it keeps the original spacing since no match should happen!
                "[('company_id','in',    user.other.allowed_company_ids)]",
            ),
            (
                "[('group_id','in', user.groups_id.ids)]",
                "[('group_id', 'in', user.all_group_ids.ids)]",
            ),
            (
                "[('group_id','in', [g.id for g in user.groups_id])]",
                "[('group_id', 'in', user.all_group_ids.ids)]",
            ),
            (
                "[(1, '=', 0), (1, '=', 1)]",
                "[(0, '=', 1), (1, '=', 1)]",
            ),
        ]
    )
    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    def test_literal_replace(self, orig, expected, old_unparse_fallback=None):
        repl = util.literal_replace(
            orig,
            {
                "allowed_company_ids": "companies.active_ids",
                "user.groups_id.ids": "user.all_group_ids.ids",
                "[g.id for g in user.groups_id]": "user.all_group_ids.ids",
                "(1, '=', 0)": "(0, '=', 1)",
            },
        )
        if old_unparse_fallback:
            self.assertIn(repl, [expected, old_unparse_fallback])
        else:
            self.assertEqual(repl, expected)

    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    @mute_logger("odoo.upgrade.util.misc")
    def test_literal_replace_error(self):
        # this shouldn't raise a syntax error
        res = util.literal_replace("[1,2", {"1": "3"})
        self.assertEqual(res, "[1,2")

    @parametrize(
        [
            ("x[1 ]", "x[1]"),
            ("{x for x in y }, [ {1   }, {1 : 2},x[1],y[ 1:2:3 ] ]", "{x for x in y},[{1},{1:2},x[1],y[1:2:3]]"),
            (
                "[1 if True   else 2, * z] + [x for x in y if w], {x:x for x in y} ",
                "[1 if True else 2,*z]+[x for x in y if w],{x:x for x in y}",
            ),
            ("1 <  2 <3, x or z  and y   or x", "1<2<3,x or z and y or x"),
        ]
    )
    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    def test_literal_replace_full(self, text, orig):
        # check each grammar piece
        repl = util.literal_replace(text, {orig: "gone"})
        self.assertEqual(repl, "gone")

        # minimal change should invalidate the replace
        text = text.replace("x", "xx")
        repl = util.literal_replace(text, {orig: "gone"})
        self.assertEqual(text, repl)

    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    def test_literal_replace_callable(self):
        def adapter(node):
            return ast.parse("this.get('{}')".format(node.attr), mode="eval").body

        repl = util.literal_replace(
            "result = this. x if that . y == 2 else this  .z",
            {ast.Attribute(ast.Name("this", None), util.literal_replace.WILDCARD, None): adapter},
        )
        self.assertIn(
            repl,
            [
                "result = this.get('x') if that.y == 2 else this.get('z')",
                # fallback for older unparse
                "result = (this.get('x') if (that.y == 2) else this.get('z'))",
            ],
        )

    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    @unittest.skipUnless(hasattr(ast, "Constant"), "`ast.Constant` available from Python3.6")
    def test_literal_replace_callable2(self):
        def adapter2(node):
            return ast.parse(
                "{} / 42".format(node.left.value if hasattr(node.left, "value") else node.left.n), mode="eval"
            ).body

        def adapter3(node):
            return ast.parse("y == 42", mode="eval").body

        repl = util.literal_replace(
            "16 * w or y == 2",
            {
                ast.BinOp(ast.Constant(16), ast.Mult(), ast.Name(util.literal_replace.WILDCARD, None)): adapter2,
                ast.Compare(ast.Name("y", None), [ast.Eq()], [ast.Constant(util.literal_replace.WILDCARD)]): adapter3,
            },
        )
        # Check with fallback for older unparse
        self.assertIn(repl, ["16 / 42 or y == 42", "((16 / 42) or (y == 42))"])

        def adapter4(node):
            return ast.parse(
                "{}.get({})".format(util.ast_unparse(node.value), util.ast_unparse(node.slice)), mode="eval"
            ).body

        repl = util.literal_replace(
            "  x[ 'a' ]+y[b   ] -  z[None]",
            {
                ast.Subscript(
                    ast.Name(util.literal_replace.WILDCARD, None), util.literal_replace.WILDCARD, None
                ): adapter4
            },
        )
        # Check with fallback for older unparse
        self.assertIn(repl, ["x.get('a') + y.get(b) - z.get(None)", "((x.get('a') + y.get(b)) - z.get(None))"])

    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    @unittest.skipUnless(hasattr(ast, "Constant"), "`ast.Constant` available from Python3.6")
    def test_literal_replace_wildcards(self):
        repl = util.literal_replace(
            "x+1 - z* 18 + [1,2,3]",
            {
                ast.Name(util.literal_replace.WILDCARD, None): "y",
                (ast.Constant if sys.version_info > (3, 9) else ast.Num)(util.literal_replace.WILDCARD): "2",
                ast.List([util.literal_replace.WILDCARD], None): "[4,5]",
            },
        )
        self.assertIn(repl, ["y + 2 - y * 2 + [4, 5]", "(((y + 2) - (y * 2)) + [4, 5])"])

    @parametrize(
        [
            (ast.Constant(util.literal_replace.WILDCARD, kind=None), "*"),
            (ast.Name(util.literal_replace.WILDCARD, None), "*"),
            (ast.Attribute(ast.Name(util.literal_replace.WILDCARD, None), util.literal_replace.WILDCARD, None), "*.*"),
            (
                ast.Subscript(
                    ast.Name(util.literal_replace.WILDCARD, None), ast.Name(util.literal_replace.WILDCARD, None), None
                ),
                "*[*]",
            ),
            (
                ast.BinOp(
                    ast.Name(util.literal_replace.WILDCARD, None),
                    ast.Add(),
                    ast.Name(util.literal_replace.WILDCARD, None),
                ),
                "* + *",
                "(* + *)",
            ),
            (ast.List([util.literal_replace.WILDCARD], None), "[*]"),
            (ast.Tuple([util.literal_replace.WILDCARD], None), "(*,)"),
        ]
    )
    @unittest.skipUnless(util.ast_unparse is not None, "`ast.unparse` available from Python3.9")
    def test_literal_replace_wildcard_unparse(self, orig, expected, old_unparse_fallback=None):
        res = util.ast_unparse(orig)
        if old_unparse_fallback:
            self.assertIn(res, [expected, old_unparse_fallback])
        else:
            self.assertEqual(res, expected)


def not_doing_anything_converter(el):
    return True


class TestHTMLFormat(UnitTestCase):
    def testsnip(self):
        view_arch = """
            <html>
                <div class="fake_class_not_doing_anything"><br/></div>
                <script>
                (event) =&gt; {
                };
                </script>
            </html>
        """
        view_id = self.env["ir.ui.view"].create(
            {
                "name": "not_for_anything",
                "type": "qweb",
                "mode": "primary",
                "key": "test.htmlconvert",
                "arch_db": view_arch,
            }
        )
        cr = self.env.cr
        snippets.convert_html_content(
            cr,
            snippets.html_converter(
                not_doing_anything_converter, selector="//*[hasclass('fake_class_not_doing_anything')]"
            ),
        )
        util.invalidate(view_id)
        res = self.env["ir.ui.view"].search_read([("id", "=", view_id.id)], ["arch_db"])
        self.assertEqual(len(res), 1)
        oneline = lambda s: re.sub(r"\s+", " ", s.strip())
        self.assertEqual(oneline(res[0]["arch_db"]), oneline(view_arch))


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
        groups = "group_ids" if util.version_gte("saas~18.2") else "groups_id"
        u1[groups] = g1 | g3
        self.assertEqual(u1[groups].ids, [g1.id, g3.id])
        util.replace_record_references_batch(cr, mapping, "res.groups")
        util.invalidate(u1)
        self.assertEqual(u1[groups].ids, [g3.id])

        u2 = self.env["res.users"].create({"login": "U2", "name": "U2"})
        u2[groups] = g1 | g2
        self.assertEqual(u2[groups].ids, [g1.id, g2.id])
        util.replace_record_references_batch(cr, mapping, "res.groups")
        util.invalidate(u2)
        self.assertEqual(u2[groups].ids, [g3.id])


class TestConvertFieldToHtml(UnitTestCase):
    def test_convert_field_to_html(self):
        cr = self.env.cr

        model = self.env["ir.model"].search([("model", "=", "res.partner")])
        f1 = self.env["ir.model.fields"].create(
            {"name": "x_testx", "model": "res.partner", "ttype": "text", "model_id": model.id, "translate": True}
        )
        partner = self.env["res.partner"].create({"name": "test Pxtner", "x_testx": "test partner field"})
        default = self.env["ir.default"].create({"field_id": f1.id, "json_value": '"Test text"'})
        util.convert_field_to_html(cr, "res.partner", "x_testx")
        util.invalidate(default)

        self.assertEqual(default.json_value, '"<p>Test text</p>"')
        self.assertEqual(partner.x_testx, "<p>test partner field</p>")


class TestRemoveView(UnitTestCase):
    def test_remove_view(self):
        test_view_1 = self.env["ir.ui.view"].create(
            {
                "name": "test_view_1",
                "type": "qweb",
                "key": "base.test_view_1",
                "arch": """
                <t t-name="base.test_view_1">
                    <div>Test View 1 Content</div>
                </t>
                """,
            }
        )
        self.env["ir.model.data"].create(
            {"name": "test_view_1", "module": "base", "model": "ir.ui.view", "res_id": test_view_1.id}
        )
        test_view_2 = self.env["ir.ui.view"].create(
            {
                "name": "test_view_2",
                "type": "qweb",
                "key": "base.test_view_2",
                "arch": """
                <t t-name="base.test_view_2">
                    <t t-call="base.test_view_1"/>
                    <div>Test View 2 Content</div>
                </t>
                """,
            }
        )
        test_view_3 = self.env["ir.ui.view"].create(
            {
                "name": "test_view_3",
                "type": "qweb",
                "key": "base.test_view_3",
                "arch": """
                <t t-name="base.test_view_3">
                    <t t-call="base.test_view_1"/>
                    <t t-call="base.test_view_2"/>
                </t>
                """,
            }
        )
        self.env["ir.model.data"].create(
            {"name": "test_view_3", "module": "base", "model": "ir.ui.view", "res_id": test_view_3.id}
        )

        # call by xml_id
        util.remove_view(self.env.cr, xml_id="base.test_view_1")
        util.invalidate(test_view_2)
        util.invalidate(test_view_3)
        self.assertFalse(test_view_1.exists())
        self.assertNotIn('t-call="base.test_view_1"', test_view_2.arch_db)
        self.assertNotIn('t-call="base.test_view_1"', test_view_3.arch_db)

        # call by view_id
        util.remove_view(self.env.cr, view_id=test_view_2.id)
        util.invalidate(test_view_3)
        self.assertFalse(test_view_2.exists())
        self.assertNotIn('t-call="base.test_view_2"', test_view_3.arch_db)


class TestRenameXMLID(UnitTestCase):
    def test_rename_xmlid(self):
        test_view_1 = self.env["ir.ui.view"].create(
            {
                "name": "test_view_1",
                "type": "qweb",
                "key": "base.test_view_1",
                "arch": """
                <t t-name="base.test_view_1">
                    <div>Test View 1 Content</div>
                </t>
                """,
            }
        )
        self.env["ir.model.data"].create(
            {"name": "test_view_1", "module": "base", "model": "ir.ui.view", "res_id": test_view_1.id}
        )
        test_view_2 = self.env["ir.ui.view"].create(
            {
                "name": "test_view_2",
                "type": "qweb",
                "key": "base.test_view_2",
                "arch": """
                <t t-name="base.test_view_2">
                    <t t-call="base.test_view_1"/>
                    <div>Test View 2 Content</div>
                </t>
                """,
            }
        )
        util.rename_xmlid(self.env.cr, "base.test_view_1", "base.rename_view")
        util.invalidate(test_view_2)
        self.assertIn('t-call="base.rename_view"', test_view_2.arch_db)
        self.assertIn('t-name="base.rename_view"', test_view_1.arch_db)
