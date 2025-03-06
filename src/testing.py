import functools
import inspect
import logging
import os
import re

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

    Decorator for UnitTestCase test functions to parametrize the decorated test.

    Usage:
    ```python
    @parametrize([
        (1, 2),
        (2, 4),
        (-1, -2),
        (0, 0),
    ])
    def test_double(self, input, expected):
        self.assertEqual(input * 2, expected)
    ```

    It works by injecting test functions in the containing class.
    Idea taken from the `parameterized` package (https://pypi.org/project/parameterized/).
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
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if "__base_version" not in util.ENVIRON:
            bv = os.getenv("ODOO_BASE_VERSION", release.series)
            util.ENVIRON["__base_version"] = parse_version(bv)


class UpgradeCommon(BaseCase):
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
        query = """
            INSERT INTO {} (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """.format(DATA_TABLE)
        self._data_table_cr.execute(query, (key, value))
        self._data_table_cr._cnx.commit()

    def _get_value(self, key):
        self._init_db()
        query = "SELECT value FROM {} WHERE key = %s".format(DATA_TABLE)
        self._data_table_cr.execute(query, [key])
        result = self._data_table_cr.fetchone()
        if not result:
            raise KeyError(key)
        return result[0]

    def _key_exists(self, key):
        self._init_db()
        query = "SELECT 1 FROM {} WHERE key = %s".format(DATA_TABLE)
        self._data_table_cr.execute(query, [key])
        return bool(self._data_table_cr.rowcount)

    def _init_db(self):
        if not UpgradeCommon.__initialized:
            self._data_table_cr.execute("SELECT 1 FROM pg_class WHERE relname=%s", [DATA_TABLE])
            if not self._data_table_cr.rowcount:
                _logger.info("Creating table %s", DATA_TABLE)
                query = """ CREATE TABLE {} (
                    key VARCHAR(255) PRIMARY KEY,
                    value JSONB NOT NULL
                )""".format(DATA_TABLE)
                self._data_table_cr.execute(query)
                self._data_table_cr._cnx.commit()
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
    Test case to modify data in origin version, and assert in target version.

    User must define a "prepare" and a "check" method.
    - prepare method can write in database, return value will be stored in a dedicated table and
      passed as argument to check.
    - check method can assert that the received argument is the one expected,
      executing any code to retrieve equivalent information in migrated database.
      Note: check argument is a loaded json dump, meaning that tuple are converted to list.
      convert_check can be used to normalise the right part of the comparison.

    check method is only called if corresponding prepared was run in previous version

    prepare and check implementation may contains version conditional code to match api changes.

    using @change_version class decorator can indicate with script version is tested here if any:
    Example: to test a saas~12.3 script, using @change_version('saas-12,3') will only run prepare if
    version in [12.0, 12.3[ and run check if version is in [12.3, 13]

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
    Test case to check invariant through any version.

    User must define a "invariant" method.
    invariant return value will be compared between the two version.

    invariant implementation may contains version conditional code to match api changes.
    """

    message = "Invariant check fail"

    def __init_subclass__(cls, abstract=False):
        cls._abstract = abstract
        if not abstract and not hasattr(cls, "invariant"):
            _logger.error("%s (IntegrityCase) must define an invariant method", cls.__name__)

    # IntegrityCase should not alterate database:
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
