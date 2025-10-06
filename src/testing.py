r"""
There are two main classes used testing during the upgrade.

* :class:`~odoo.upgrade.testing.UpgradeCase` for testing upgrade scripts,
* :class:`~odoo.upgrade.testing.UpgradeCase` for testing invariants across versions.

Subclasses must implement:

* For ``UpgradeCase``:

  - ``prepare`` method: prepare data before upgrade,
  - ``check`` method: check data was correctly upgraded.

* For ``IntegrityCase``:

  - ``invariant`` method: compute an invariant to check.

Put your test classes in a ``tests`` Python module (folder) in any of the folders
containing the upgrade scripts of your modules. The script containing your tests must
have a `test_` prefix. The ``tests`` module must contain an ``__init__.py`` file to be
detected by Odoo.

Example directory structure::

   myupgrades/
   └── mymodule1/
       ├── 18.0.1.1.2/
       │   └── pre-myupgrade.py
       └── tests/
           ├── __init__.py
           └── test_myupgrade.py

.. note::

   The tests in the example above will be loaded only if ``mymodule1`` is being
   **upgraded.**

Running Upgrade Tests
---------------------

After receiving an upgraded database with all standard Odoo modules already upgraded to
their target version, you can test the upgrade of custom modules by following a three-step
process:

1. Prepare the test data

   .. code-block:: bash

      $ ~/odoo/$version/odoo-bin -d DB --test-tags=$prepare_test_tag \
        --upgrade-path=~/upgrade-util/src,~/myupgrades \
        --addons=~/odoo/$version/addons,~/enterprise/$version,~/mymodules --stop

2. Upgrade the modules

   .. code-block:: bash

      $ ~/odoo/$version/odoo-bin -d DB -u mymodule1,mymodule2 \
        --upgrade-path=~/upgrade-util/src,~/myupgrades \
        --addons=~/odoo/$version/addons,~/enterprise/$version,~/mymodules --stop

3. Check the upgraded data

   .. code-block:: bash

      $ ~/odoo/$version/odoo-bin -d DB --test-tags=$check_test_tag \
        --upgrade-path=~/upgrade-util/src,~/myupgrades \
        --addons=~/odoo/$version/addons,~/enterprise/$version,~/mymodules --stop

The example above assumes that ``$version`` is the target version of your upgrade (e.g.
``18.0``), ``DB`` is the name of your database, and ``mymodule1,mymodule2`` are the
modules you want to upgrade. The directory structure assumes that ``~/odoo/$version`` and
``~/enterprise/$version`` contain the Community and Enterprise source code for the target
Odoo version, respectively. ``~/mymodules`` contains the code of your custom modules
(``mymodule1``, ...), ``~/myupgrades`` contains your custom upgrade scripts, and
``~/upgrade-util`` contains the `upgrade utils <https://github.com/odoo/upgrade-util/>`_
repo.

The variables ``$prepare_test_tag`` and ``$check_test_tag`` must be set according to:

.. list-table::
   :header-rows: 1
   :stub-columns: 1

   * - Variable
     - ``UpgradeCase``
     - ``IntegrityCase``
   * - ``$prepare_test_tag``
     - ``upgrade.test_prepare``
     - ``integrity_case.test_prepare``
   * - ``$check_test_tag``
     - ``upgrade.test_check``
     - ``integrity_test.test_check``

.. note::

   `upgrade.test_prepare` also runs ``IntegrityCase`` tests, so you can prepare data
   for both ``UpgradeCase`` and ``IntegrityCase`` tests with only this tag.

.. warning::

   Do **not** run any ``prepare`` method of an ``UpgradeCase`` before sending your
   database for a **production** upgrade to `upgrade.odoo.com
   <https://upgrade.odoo.com>`_. Doing so may risk your upgrade being blocked and marked
   as failed.

API documentation
-----------------
"""

import functools
import inspect
import logging
import os
import re
from contextlib import contextmanager

import odoo
from odoo import api, release
from odoo.modules.registry import Registry
from odoo.tests.common import BaseCase, TransactionCase, get_db_name
from odoo.tools import config
from odoo.tools.parse_version import parse_version

try:
    from odoo.tests.common import MetaCase
except ImportError:
    MetaCase = None

try:
    from odoo.api import SUPERUSER_ID
except ImportError:
    from odoo import SUPERUSER_ID

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from . import util
from .util import json

_logger = logging.getLogger(__name__)

DATA_TABLE = "upgrade_test_data"
VERSION_RE = re.compile(r"^(saas[-~])?(\d+).(\d+)$")


def parametrize(argvalues):
    """
    Parametrize a test function.

    Decorator for upgrade test functions to parametrize and generate multiple tests from
    it.

    Usage::

        @parametrize([(1, 2), (2, 4), (-1, -2), (0, 0)])
        def test_double(self, input, expected):
            self.assertEqual(input * 2, expected)

    Works by injecting test functions in the containing class.
    Inspired by the `parameterized <https://pypi.org/project/parameterized/>`_ package.
    """

    def make_func(func, name, args):
        @functools.wraps(func)
        def wrapped(self):
            return func(self, *args)

        wrapped.__name__ = name
        return wrapped

    def decorator(func):
        frame_locals = inspect.currentframe().f_back.f_locals

        digits = len(str(len(argvalues)))
        for i, args in enumerate(argvalues):
            new_name = f"{func.__name__}__{i:0>{digits}}"
            # inject new function in the parent frame
            frame_locals[new_name] = make_func(func, new_name, args)

    return decorator


def _create_meta(sequence: int, *tags: str) -> type:
    if MetaCase:

        class UpgradeMetaCase(MetaCase):
            def __init__(self, name, bases, attrs, **kwargs):
                # Setting test_tags in __init_subclass__ could work, but BaseCase will override them in __init__.
                # we need to set test_tags after BaseCase __init__
                super().__init__(name, bases, attrs)
                self.test_sequence = sequence
                self.test_tags = {"post_install", "upgrade"} | set(tags)
                self.test_class = name

                if self.__module__.startswith("odoo.upgrade."):
                    self.test_module = self.__module__.split(".")[2]
                elif self.__module__.startswith("odoo.addons.base.maintenance.migrations"):
                    self.test_module = self.__module__.split(".")[5]

        return UpgradeMetaCase("UpgradeMetaCase", (), {})
    else:

        class UpgradeMetaCase(BaseCase):
            def __init_subclass__(cls):
                super().__init_subclass__()

                if cls.__module__.startswith("odoo.upgrade."):
                    cls.test_module = cls.__module__.split(".")[2]
                elif cls.__module__.startswith("odoo.addons.base.maintenance.migrations"):
                    cls.test_module = cls.__module__.split(".")[5]
                else:
                    return

                cls.test_tags = {"post_install", "upgrade"} | set(tags)
                cls.test_sequence = sequence

        return UpgradeMetaCase


class UnitTestCase(TransactionCase, _create_meta(10, "upgrade_unit")):
    """:meta private: exclude from online docs."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if "__base_version" not in util.ENVIRON:
            bv = os.getenv("ODOO_BASE_VERSION", release.series)
            util.ENVIRON["__base_version"] = parse_version(bv)

    @contextmanager
    def assertNotUpdated(self, table, ids=None, msg=None):
        cr = self.env.cr
        cr.execute(util.format_query(cr, "DROP TRIGGER IF EXISTS no_update ON {}", table))
        cr.execute(
            """
              DROP TABLE IF EXISTS _upg_test_no_upd_id;
            CREATE UNLOGGED TABLE _upg_test_no_upd_id(id int PRIMARY KEY, record json);
            CREATE OR REPLACE
          FUNCTION fail_assert_not_updated() RETURNS TRIGGER AS $$
             BEGIN
                INSERT INTO _upg_test_no_upd_id VALUES (NEW.id, row_to_json(NEW, true))
                    ON CONFLICT DO NOTHING;
                RETURN NEW;
               END
            $$ LANGUAGE PLPGSQL
            """,
        )
        cr.execute(
            util.format_query(
                cr,
                """
                CREATE TRIGGER no_update
                BEFORE {when}
                    ON {table}
                   FOR EACH ROW {cond} EXECUTE
              FUNCTION fail_assert_not_updated()
                """,
                when=util.SQLStr("UPDATE" if ids is not None else "UPDATE or INSERT"),
                table=table,
                cond=util.SQLStr("WHEN (new.id = ANY(%s))" if ids else ""),
            ),
            [list(ids) if ids is not None else None],
        )
        self.addCleanup(cr.execute, "DROP TABLE IF EXISTS _upg_test_no_upd_id")
        self.addCleanup(cr.execute, util.format_query(cr, "DROP TRIGGER IF EXISTS no_update ON {}", table))
        yield
        cr.execute("SELECT record FROM _upg_test_no_upd_id")
        updated_records = [r[0] for r in cr.fetchall()]
        if updated_records:
            raise AssertionError(msg or "Some {} records were updated {}".format(table, updated_records))

    @contextmanager
    def assertUpdated(self, table, ids=None, msg=None):
        cr = self.env.cr
        cr.execute(util.format_query(cr, "DROP TRIGGER IF EXISTS assert_update ON {}", table))
        cr.execute(
            """
              DROP TABLE IF EXISTS _upg_test_upd_id;
            CREATE UNLOGGED TABLE _upg_test_upd_id(id int PRIMARY KEY);
            CREATE OR REPLACE
          FUNCTION save_updated() RETURNS TRIGGER AS $$
             BEGIN
                INSERT INTO _upg_test_upd_id VALUES (NEW.id)
                    ON CONFLICT DO NOTHING;
                RETURN NEW;
               END
            $$ LANGUAGE PLPGSQL
            """,
        )
        cr.execute(
            util.format_query(
                cr,
                """
                CREATE TRIGGER assert_update
                BEFORE {when}
                    ON {table}
                   FOR EACH ROW {cond} EXECUTE
              FUNCTION save_updated()
                """,
                when=util.SQLStr("UPDATE" if ids is not None else "UPDATE or INSERT"),
                table=table,
                cond=util.SQLStr("WHEN (NEW.id = ANY(%s))" if ids else ""),
            ),
            [list(ids) if ids is not None else None],
        )
        self.addCleanup(cr.execute, "DROP TABLE IF EXISTS _upg_test_upd_id")
        self.addCleanup(cr.execute, util.format_query(cr, "DROP TRIGGER IF EXISTS assert_update ON {}", table))
        yield
        cr.execute("SELECT id FROM _upg_test_upd_id")
        updated_ids = [r[0] for r in cr.fetchall()]
        if not ids:
            self.assertTrue(updated_ids, msg or "No record was updated.")
        else:
            self.assertEqual(set(updated_ids), set(ids), msg or "Records were not updated.")


class UpgradeCommon(BaseCase):
    """:meta private: exclude from online docs."""

    __initialized = False

    change_version = (None, None)
    _abstract = True
    allow_inherited_tests_method = True

    @property
    def key(self):
        return "%s.%s" % (".".join(self.__class__.__module__.split(".")[-3:]), self.__class__.__name__)

    def _set_value(self, key, value):
        self._init_db()
        value = json.dumps(value, sort_keys=True)
        klass = None
        for mro in type(self).mro():
            if mro.__module__ in ("odoo.upgrade.testing", "odoo.addons.base.maintenance.migrations.testing"):
                klass = mro.__name__
                break

        cr = self._data_table_cr
        query = util.format_query(
            cr,
            """
            INSERT INTO {} (key, class, value) VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, class=EXCLUDED.class
            """,
            DATA_TABLE,
        )
        cr.execute(query, (key, klass, value))
        cr._cnx.commit()

    def _get_value(self, key):
        self._init_db()
        cr = self._data_table_cr
        query = util.format_query(cr, "SELECT value FROM {} WHERE key = %s", DATA_TABLE)
        cr.execute(query, [key])
        result = cr.fetchone()
        if not result:
            raise KeyError(key)
        return result[0]

    def _key_exists(self, key):
        self._init_db()
        cr = self._data_table_cr
        query = util.format_query(cr, "SELECT 1 FROM {} WHERE key = %s", DATA_TABLE)
        cr.execute(query, [key])
        return bool(cr.rowcount)

    def _init_db(self):
        if not UpgradeCommon.__initialized:
            cr = self._data_table_cr
            cr.execute("SELECT 1 FROM pg_class WHERE relname=%s", [DATA_TABLE])
            if not cr.rowcount:
                _logger.info("Creating table %s", DATA_TABLE)
                query = util.format_query(
                    cr,
                    """
                    CREATE TABLE {} (
                        key VARCHAR(255) PRIMARY KEY,
                        class varchar,
                        value JSONB NOT NULL
                    )
                    """,
                    DATA_TABLE,
                )
                cr.execute(query)
            else:
                # upgrade existing table
                util.create_column(cr, DATA_TABLE, "class", "varchar")
            cr._cnx.commit()
            UpgradeCommon.__initialized = True

    def _setup_registry(self):
        self.registry = Registry(get_db_name())
        self._data_table_cr = (
            self.registry.cursor()
        )  # use to commit in upgrade_test_data, dont use it for anything else
        self.addCleanup(self._data_table_cr.close)

    def setUp(self):
        super().setUp()
        self._setup_registry()
        self.cr = self.registry.cursor()
        self.env = api.Environment(self.cr, SUPERUSER_ID, {})
        self.addCleanup(self.env.clear)
        self.addCleanup(self.cr.close)

    # could be reworked that to either call prepare or check in a unique test_method
    # -> but in this case impossible to filter on prepare or check with test_tags
    def test_prepare(self):
        if self._abstract:
            self.skipTest("abstract test class")
            return
        (version, sub_version) = self.change_version
        if version is not None:
            current_version = parse_version(release.series)
            if current_version >= parse_version("%s.%s" % self.change_version):
                self.skipTest("out of bounds version (>)")
                return
            if current_version < parse_version("%s.%s" % get_previous_major(version, sub_version)):
                self.skipTest("out of bounds version (<)")
                return

        key = self.key
        if self._key_exists(key):
            _logger.warning("key %s already exists, skipping", key)
            # do we want to warn and skip, or update key?
            # for upgrade case, avoid to prepare twice in all cases. For integrity, maybe update value
            self.skipTest("duplicated key")
            return

        _logger.info("Calling %s.prepare", self.__class__.__name__)
        value = self.prepare()
        self._set_value(self.key, value)  # prepare has been called, even if value is null

    def test_check(self):
        if self._abstract:
            self.skipTest("abstract test class")
            return
        (version, sub_version) = self.change_version
        if version is not None:
            current_version = parse_version(release.series)
            if current_version < parse_version("%s.%s" % self.change_version):
                self.skipTest("out of bounds version (<)")
                return
            if current_version > parse_version("%s.%s" % get_next_major(version, sub_version)):
                self.skipTest("out of bounds version (>)")
                return

        key = self.key
        try:
            value = self._get_value(key)
        except KeyError:
            _logger.info("No value found for %s, skipping check", key)
            # we don't want to check is corresponding prepare was not executed
            # Example: change_version 13.1, testing from 13.2 to 13.3
        else:
            _logger.info("Calling %s.check", self.__class__.__name__)
            self.check(value)

    def convert_check(self, value):
        return json.loads(json.dumps(value, sort_keys=True))


def change_version(version_str):
    """
    Class decorator to specify the version on which a test is relevant.

    Using ``@change_version(version)`` indicates:

    * ``test_prepare`` will only run if the current Odoo version is in the range
      ``[next_major_version-1, version)``.
    * ``test_check`` will only run if the current Odoo version is in the range ``[version,
      next_major_version)``.

    ``next_major_version`` is the next major version after ``version``, e.g. for
    ``saas~17.2`` it is ``18.0``.

    .. note::

       Do not use this decorator if your upgrade is in the same major version. Otherwise,
       your tests will not run.
    """

    def version_decorator(obj):
        match = VERSION_RE.match(version_str)
        if not match:
            raise ValueError("change_version decorator must be in format [saas(-|~)<int:version>.<int:subversion:int>]")
        (_, version, sub_version) = match.groups()
        obj.change_version = (int(version), int(sub_version))
        return obj

    return version_decorator


# helpers to get the version on which a test is expected to run depending on the value specified with the `change_version` decorator
FAKE_MAJORS = [(12, 3)]  # non dot-zero versions which will run tests


def get_next_major(major, minor):
    for fake in FAKE_MAJORS:
        if major == fake[0] and minor < fake[1]:
            return fake
    if minor != 0:
        major += 1
    return major, 0


def get_previous_major(major, minor):
    if minor == 0:
        major -= 1

    for fake in FAKE_MAJORS:
        if major == fake[0] and (minor == 0 or minor > fake[1]):
            return fake

    return major, 0


# pylint: disable=inherit-non-class
class UpgradeCase(UpgradeCommon, _create_meta(10, "upgrade_case")):
    """
    Test case to verify that the upgrade scripts correctly upgrade data.

    Override:

    * ``prepare`` to set up data,
    * ``check`` to assert expectations after the upgrade.

    The ORM can be used in these methods to perform the functional flow under test. The
    return value of ``prepare`` is persisted and passed as an argument to ``check``. It
    must be JSON-serializable.

    .. note::

       Since ``prepare`` injects or modifies data, this type of test is intended **only
       for development**. Use it to test upgrade scripts while developing them. **Do not**
       run these tests for a production upgrade. To verify that upgrades preserved
       important invariants in production, use ``IntegrityCase`` tests instead.

    .. example::

       .. code-block:: python

          from odoo.upgrade.testing import UpgradeCase, change_version


          class DeactivateBobUsers(UpgradeCase):

              def prepare(self):
                  u = self.env["res.users"].create({"login": "bob", "name": "Bob"})
                  return u.id  # will be passed to check

              def check(self, uid):  # uid is the value returned by prepare
                  self.env.cr.execute(
                      "SELECT * FROM res_users WHERE id=%s AND NOT active", [uid]
                  )
                  self.assertEqual(self.env.cr.rowcount, 1)
    """

    def __init_subclass__(cls, abstract=False):
        cls._abstract = abstract
        if not abstract and (not hasattr(cls, "prepare") or not hasattr(cls, "check")):
            _logger.error("%s (UpgradeCase) must define prepare and check methods", cls.__name__)

    def test_prepare(self):
        super(UpgradeCase, self).test_prepare()
        self.cr.commit()


# pylint: disable=inherit-non-class
class IntegrityCase(UpgradeCommon, _create_meta(20, "integrity_case")):
    """
    Test case for validating invariants across upgrades.

    Override:

    * ``invariant`` to return a JSON-serializable value representing
      the invariant to check.

    The ``invariant`` method is called both before and after the upgrade,
    and the results are compared.


    .. example::

       .. code-block:: python

          from odoo.upgrade.testing import IntegrityCase


          class NoNewUsers(IntegrityCase):
              def invariant(self):
                  return self.env["res.users"].search_count([])
    """

    message = "Invariant check fail"

    def __init_subclass__(cls, abstract=False):
        cls._abstract = abstract
        if not abstract and not hasattr(cls, "invariant"):
            _logger.error("%s (IntegrityCase) must define an invariant method", cls.__name__)

    # IntegrityCase should not alter the database:
    # TODO give a test cursor, don't commit after prepare, use a protected cursor to set_value

    def prepare(self):
        return self.invariant()

    def check(self, value):
        self.assertEqual(value, self.convert_check(self.invariant()), self.message)

    def _setup_registry(self):
        super(IntegrityCase, self)._setup_registry()
        cr = self.registry.cursor()
        self.addCleanup(cr.close)
        if hasattr(self, "registry_enter_test_mode"):
            self.registry_enter_test_mode(cr=cr)
        else:
            self.registry.enter_test_mode(cr)
            self.addCleanup(self.registry.leave_test_mode)

    def setUp(self):
        """:meta private: exclude from online docs."""
        super(IntegrityCase, self).setUp()

        def commit(self):
            if self.dbname == config["log_db"].split("/")[-1]:
                self._cnx.commit()
            else:
                raise RuntimeError("Commit is forbidden in integrity cases")

        patcher = patch.object(odoo.sql_db.Cursor, "commit", commit)
        patcher.start()
        self.addCleanup(patcher.stop)

    def skip_if_demo(self):
        self.env.cr.execute("SELECT 1 FROM ir_module_module WHERE name='base' AND demo")
        if self.env.cr.rowcount:
            self.skipTest("This invariant is not guaranteed with demo data.")
