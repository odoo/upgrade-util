import json
import logging
import os
import re

import odoo
from odoo import api, release
from odoo.tests.common import BaseCase, MetaCase, TransactionCase, get_db_name, tagged
from odoo.tools import config
from odoo.tools.parse_version import parse_version

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from . import util

_logger = logging.getLogger(__name__)

DATA_TABLE = "upgrade_test_data"
VERSION_RE = re.compile(r"^(saas[-~])?(\d+).(\d+)$")


@tagged("upgrade")
class UnitTestCase(TransactionCase):
    @classmethod
    def setUpClass(cls):
        if "__base_version" not in util.ENVIRON:
            bv = os.getenv("ODOO_BASE_VERSION", release.series)
            util.ENVIRON["__base_version"] = parse_version(bv)


class UpgradeCommon(BaseCase):
    __initialized = False

    def __init_subclass__(cls):
        cls.change_version = (None, None)

    @property
    def key(self):
        return "%s.%s" % (".".join(self.__class__.__module__.split(".")[-3:]), self.__class__.__name__)

    def _set_value(self, key, value):
        self._init_db()
        value = json.dumps(value, sort_keys=True)
        query = """
            INSERT INTO {} (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """.format(
            DATA_TABLE
        )
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
                )""".format(
                    DATA_TABLE
                )
                self._data_table_cr.execute(query)
                self._data_table_cr._cnx.commit()
            UpgradeCommon.__initialized = True

    def _setup_registry(self):
        self.registry = odoo.registry(get_db_name())
        self._data_table_cr = (
            self.registry.cursor()
        )  # use to commit in upgrade_test_data, dont use it for anything else
        self.addCleanup(self._data_table_cr.close)

    def setUp(self):
        super().setUp()
        self._setup_registry()
        self.cr = self.registry.cursor()
        self.env = api.Environment(self.cr, odoo.SUPERUSER_ID, {})
        self.addCleanup(self.env.clear)
        self.addCleanup(self.cr.close)

    # could be reworked that to either call prepare or check in a unique test_method
    # -> but in this case impossible to filter on prepare or check with test_tags
    def test_prepare(self):
        (version, sub_version) = self.change_version
        if version is not None:
            current_version = parse_version(release.series)
            if current_version >= parse_version("%s.%s" % self.change_version):
                return
            # find previous major version
            if sub_version != 0:  # 12.4 -> 12.0
                sub_version = 0
            else:  # 13.0 -> 12.0
                version -= 1
            if current_version < parse_version("%s.%s" % (version, sub_version)):
                return

        key = self.key
        if self._key_exists(key):
            _logger.warning("key %s already exists, skipping", key)
            # do we want to warn and skip, or update key?
            # for upgrade case, avoid to prepare twice in all cases. For integrity, maybe update value
            return

        _logger.info("Calling %s.prepare", self.__class__.__name__)
        value = self.prepare()
        self._set_value(self.key, value)  # prepare has been called, even if value is null

    def test_check(self):
        (version, sub_version) = self.change_version
        if version is not None:
            current_version = parse_version(release.series)
            if current_version < parse_version("%s.%s" % self.change_version):
                return
            # find the next major version
            if sub_version != 0:  # 12.4 -> 13.0 (non inclusive)
                version += 1
                sub_version = 0
            # else: 13.0 will only be checked in 13.0
            if current_version > parse_version("%s.%s" % (version, sub_version)):
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
            raise Exception("change_version decorator must be in format [saas(-|~)<int:version>.<int:subversion:int>]")
        (_, version, sub_version) = match.groups()
        obj.change_version = (int(version), int(sub_version))
        return obj

    return version_decorator


class UpgradeMetaCase(MetaCase):
    def __init__(self, name, bases, attrs):
        # Setting test_tags in __init_subclass__ could work, but Basecase will ovveride them in __init__.
        # wee need to set test_tags after Base case __init__
        super(UpgradeMetaCase, self).__init__(name, bases, attrs)
        self.test_sequence = 10
        self.test_tags = {"post_install", "upgrade", "upgrade_case"}
        self.test_class = name
        if self.__module__.startswith("odoo.upgrade."):
            self.test_module = self.__module__.split(".")[2]

    def __dir__(self):
        # since UpgradeCase and IntegrityCase are common classes intended to be overloaded,
        # we dont want the default test_prepare and test_check to be executed when imported.
        # This hack avoids tests to be executed if imported
        if self in (UpgradeCase, IntegrityCase):
            return []
        return super().__dir__()


# pylint: disable=inherit-non-class
class UpgradeCase(UpgradeCommon, UpgradeMetaCase("DummyCase", (object,), {})):
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

    def __init_subclass__(cls):  # should be in metaCase for python2.7 compatibility?
        if not hasattr(cls, "prepare") or not hasattr(cls, "check"):
            _logger.error("%s (UpgradeCase) must define prepare and check methods", cls.__name__)

    def test_prepare(self):
        super(UpgradeCase, self).test_prepare()
        self.cr.commit()


class IntegrityMetaCase(UpgradeMetaCase):
    def __init__(self, name, bases, attrs):
        super(IntegrityMetaCase, self).__init__(name, bases, attrs)
        self.test_sequence = 20
        self.test_tags -= {"upgrade_case"}
        self.test_tags |= {"integrity_case"}


# pylint: disable=inherit-non-class
class IntegrityCase(UpgradeCommon, IntegrityMetaCase("DummyCase", (object,), {})):
    """
    Test case to check invariant through any version
    User must define a "invariant" method.
    invariant return value will be compared between the two version.

    invariant implementation may contains version conditional code to match api changes.
    """

    message = "Invariant check fail"

    def __init_subclass__(cls):
        if not hasattr(cls, "invariant"):
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
        self.registry.enter_test_mode(cr)
        self.addCleanup(self.registry.leave_test_mode)

    def setUp(self):
        super(IntegrityCase, self).setUp()

        def commit(self):
            if self.dbname == config["log_db"].split("/")[-1]:
                self._cnx.commit()
            else:
                raise Exception("Commit are forbidden in integrity cases")

        patcher = patch.object(odoo.sql_db.Cursor, "commit", commit)
        patcher.start()
        self.addCleanup(patcher.stop)
