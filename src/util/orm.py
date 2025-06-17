# -*- coding: utf-8 -*-
# ruff: noqa: PLC0415
"""
Utility functions to perform operations via the ORM.

The functions on this module allow to use the ORM safely during upgrades. They enhance or
patch the ORM such that it can handle high volumes of data in a performant way. In some
cases totally different alternatives to the ORM's own functions are provided. The functions
on this module work along the ORM of *all* supported versions.
"""

import logging
import re
from contextlib import contextmanager
from functools import wraps
from itertools import chain
from operator import itemgetter
from textwrap import dedent

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

try:
    try:
        from odoo.api import SUPERUSER_ID
    except ImportError:
        from odoo import SUPERUSER_ID
    from odoo import fields as ofields
    from odoo import modules, release
except ImportError:
    from openerp import SUPERUSER_ID, modules, release

    try:
        from openerp import fields as ofields
    except ImportError:
        # this is to allow v7.0 DBs to import this module without errors
        # note: some functions on this module will fail (like recompute_fields)
        ofields = None

from .const import BIG_TABLE_THRESHOLD
from .exceptions import MigrationError
from .helpers import table_of_model
from .misc import chunks, log_progress, version_between, version_gte
from .pg import column_exists, get_columns

# python3 shims
try:
    basestring  # noqa: B018
except NameError:
    basestring = str

_logger = logging.getLogger(__name__)


def env(cr):
    """
    Create a new environment.

    .. warning::
       This function does *not* empty the cache maintained on the cursor for superuser
       with an empty environment. A call to `invalidate_cache` may be necessary every time
       data is modified directly in the database.

    :return: the new environment
    :rtype: :class:`~odoo.api.Environment`
    """
    try:
        from odoo.api import Environment
    except ImportError:
        try:
            from openerp.api import Environment
        except ImportError:
            v = release.major_version
            raise MigrationError("Hold on! There is not yet `Environment` in %s" % v)
    return Environment(cr, SUPERUSER_ID, {})


def get_admin_channel(cr):
    """
    Retrieve the admin channel or create it if it does not exist.

    :meta private: exclude from online docs
    """
    e = env(cr)
    admin_channel = None
    # mail.channel was renamed to discuss.channel in 16.3
    channel_model_name = "mail.channel" if "mail.channel" in e else "discuss.channel"
    if channel_model_name in e:
        admin_channel = e.ref("mail.channel_admin", raise_if_not_found=False)
        if admin_channel:
            return admin_channel
        # search for old name
        admin_channel = e.ref("__upgrade__.channel_administrators", raise_if_not_found=False)
        if admin_channel:
            # rename it.
            from .records import rename_xmlid  # imported here to avoid import cycles

            rename_xmlid(cr, "__upgrade__.channel_administrators", "mail.channel_admin", noupdate=True)
            return admin_channel

        admin_group = e.ref("base.group_system", raise_if_not_found=False)
        if admin_group:
            search_rules = [
                ("channel_type", "=", "channel"),
                ("group_public_id", "=", admin_group.id),
                ("group_ids", "in", admin_group.id),
            ]
            if "public" in e[channel_model_name]._fields:
                search_rules.append(("public", "=", "groups"))
            admin_channel = next(
                iter(e[channel_model_name].search(search_rules).sorted(lambda c: "admin" not in c.name.lower()) or []),
                None,
            )
            if not admin_channel:
                channel_values = {
                    "name": "Administrators",
                    "channel_type": "channel",
                    "group_public_id": admin_group.id,
                    "group_ids": [(6, 0, [admin_group.id])],
                }
                if "public" in e[channel_model_name]._fields:
                    channel_values["public"] = "groups"
                admin_channel = e[channel_model_name].create(channel_values)

            e["ir.model.data"].create(
                {
                    "name": "channel_admin",
                    "module": "mail",
                    "model": channel_model_name,
                    "res_id": admin_channel.id,
                    "noupdate": True,
                }
            )

    return admin_channel


def guess_admin_id(cr):
    """
    Guess the admin user id of `cr` database.

    :meta private: exclude from online docs
    """
    if not version_gte("12.0"):
        return SUPERUSER_ID
    cr.execute(
        """
        SELECT min(r.uid)
          FROM res_groups_users_rel r
          JOIN res_users u ON r.uid = u.id
         WHERE u.active
           AND r.gid = (SELECT res_id
                          FROM ir_model_data
                         WHERE module = 'base'
                           AND name = 'group_system')
    """
    )
    return cr.fetchone()[0] or SUPERUSER_ID


def create_cron(cr, name, model, code, interval=(1, "hours")):
    # (Cursor, str, str, str, Tuple[int, str]) -> None
    cr.execute("SELECT id FROM ir_model WHERE model = %s", [model])
    model_id = cr.fetchone()[0]
    number, unit = interval
    # TODO handle version <=10.saas-14
    cron = {
        "name": "Post Upgrade: %s" % (name,),
        "model_id": model_id,
        "state": "code",
        "code": dedent(code),
        "interval_number": number,
        "interval_type": unit,
    }
    if not version_gte("saas~17.3"):
        cron["numbercall"] = -1

    xid_name = "cron_" + re.sub(r"\W+", "_", cron["name"].lower())
    xid = "__upgrade__." + xid_name

    e = env(cr)
    data = {"module": "__upgrade__", "xml_id": xid, "values": cron, "noupdate": True}
    if hasattr(e["ir.cron"], "_load_records"):
        e["ir.cron"]._load_records([data])
    else:
        # < saas-11.5
        e["ir.model.data"]._update("ir.cron", **data)

    # Exclude the related server action from CLOC with an extra xmlid
    if not column_exists(cr, "ir_cron", "ir_actions_server_id"):
        return

    columns = get_columns(cr, "ir_model_data", ignore=("id", "module"))
    s_columns = ["s." + c for c in columns]
    query = """
        INSERT INTO ir_model_data(module, {columns})
             SELECT '__cloc_exclude__', {s_columns}
               FROM ir_model_data x
               JOIN ir_cron c ON x.model = 'ir.cron' AND x.res_id = c.id
               JOIN ir_model_data s ON s.model = 'ir.actions.server' AND s.res_id = c.ir_actions_server_id
              WHERE x.module = '__upgrade__'
                AND x.name = %s
                AND s.module = '__upgrade__'
        ON CONFLICT DO NOTHING
    """
    cr.execute(query.format(columns=",".join(columns), s_columns=",".join(s_columns)), [xid_name])


_TRACKING_ATTR = "tracking" if version_gte("saas~12.2") else "track_visibility"


def flush(records):
    ((hasattr(records, "env") and getattr(records.env, "flush_all", None)) or getattr(records, "flush", lambda: None))()


def recompute(records):
    (
        (hasattr(records, "env") and getattr(records.env, "_recompute_all", None))
        or getattr(records, "recompute", lambda: None)
    )()


def invalidate(records, *args):
    env = getattr(records, "env", None)
    expected_args = 2 if env is None else 0
    if len(args) != expected_args:
        raise ValueError("Unexpected arguments: %r" % (args[expected_args:],))
    ((env and getattr(records.env, "invalidate_all", None)) or records.invalidate_cache)(*args)


def no_selection_cache_validation(f=None):
    if not version_gte("8.0"):
        return f
    old_convert = ofields.Selection.convert_to_cache

    def _convert(self, value, record, validate=True):
        if self.model_name == "res.users" and self.name == "lang":
            return old_convert(self, value, record, validate=validate)
        return old_convert(self, value, record, validate=False)

    if f is None:
        return patch(ofields.__name__ + ".Selection.convert_to_cache", _convert)

    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch(ofields.__name__ + ".Selection.convert_to_cache", _convert):
            return f(*args, **kwargs)

    return wrapper


@no_selection_cache_validation
def recompute_fields(cr, model, fields, ids=None, logger=_logger, chunk_size=256, strategy="auto"):
    """
    Recompute field values.

    This function will recompute fields of a model restricted to a set of records - or
    all. The re-computation is not done on all records at the same time. It is split in
    batches (chunks). This avoids performance issues, and, in the worse cases, failures
    due to `MemoryError`. After each chunk is processed the data is sent back to the
    database according to one of the following strategies:

    - *flush*: use the `flush` method of the ORM
    - *commit*: `commit` the cursor - also flush
    - *auto*: pick the best alternative between the two above given the number of records
      to compute and the presence of tracked fields.

    The *commit* strategy is less prone to cause a `MemoryError` for a huge volume of data.

    :param str model: name of the model to recompute
    :param list(str) fields: list of the name of the fields to recompute
    :param list(int) or None ids: list of the IDs of the records to recompute, when `None`
                                  recompute *all* records
    :param logger: logger used to report the progress
    :type logger: :class:`logging.Logger`
    :param int chunk_size: number of records per chunk - used to split the processing
    :param str strategy: strategy used to process the re-computation
    """
    assert strategy in {"flush", "commit", "auto"}
    Model = env(cr)[model] if isinstance(model, basestring) else model
    model = Model._name
    if ids is None:
        cr.execute('SELECT id FROM "%s"' % table_of_model(cr, model))
        ids = tuple(map(itemgetter(0), cr.fetchall()))

    if strategy == "auto":
        big_table = len(ids) > BIG_TABLE_THRESHOLD
        any_tracked_field = any(getattr(Model._fields[f], _TRACKING_ATTR, False) for f in fields)
        strategy = "commit" if big_table and any_tracked_field else "flush"

    size = (len(ids) + chunk_size - 1) / chunk_size
    qual = "%s %d-bucket" % (model, chunk_size) if chunk_size != 1 else model
    for subids in log_progress(chunks(ids, chunk_size, list), logger, qualifier=qual, size=size):
        records = Model.browse(subids)
        for field_name in fields:
            field = records._fields[field_name]
            if hasattr(records, "_recompute_todo"):
                # < 13.0
                records._recompute_todo(field)
            else:
                Model.env.add_to_compute(field, records)

        recompute(records)
        # trigger dependent fields recomputation
        records.modified(fields)
        if strategy == "commit":
            cr.commit()
        else:
            flush(records)
        invalidate(records)


class iter_browse(object):
    """
    Iterate over recordsets.

    The `callable` object returned by this class can be used as an iterator that loads
    records by chunks (into a `recordset`). After each chunk is exhausted their data is
    sent back to the database - flushed or committed - and a new chunk is loaded.

    This class allows to run code like:

    .. code-block:: python

        for record in env['my.model'].browse(ids):
            record.field1 = val

        env['my.model'].browse(ids)._compute_field2()
        env['my.model'].create(values)

    in a performant way while also avoiding `MemoryError`, even when `ids` or `values`
    have millions of entries. The alternative using this class would be:

    .. example::
        .. code-block:: python

            MyModel = util.env(cr)['my.model']
            for record in util.iter_browse(MyModel, ids):
                record.field1 = val

            util.iter_browse(MyModel, ids)._compute_field2()
            util.iter_browse(MyModel, ids).create(values)

    :param model: the model to iterate
    :type model: :class:`odoo.model.Model`
    :param list(int) ids: list of IDs of the records to iterate
    :param int chunk_size: number of records to load in each iteration chunk, `200` by
                           default
    :param logger: logger used to report the progress, by default
                   :data:`~odoo.upgrade.util.orm._logger`
    :type logger: :class:`logging.Logger`
    :param str strategy: whether to `flush` or `commit` on each chunk, default is `flush`
    :return: the object returned by this class can be used to iterate, or call any model
             method, safely on millions of records.

    See also :func:`~odoo.upgrade.util.orm.env`
    """

    __slots__ = ("_chunk_size", "_cr_uid", "_it", "_logger", "_model", "_patch", "_size", "_strategy")

    def __init__(self, model, *args, **kw):
        assert len(args) in [1, 3]  # either (cr, uid, ids) or (ids,)
        self._model = model
        self._cr_uid = args[:-1]
        ids = args[-1]
        self._size = len(ids)
        self._chunk_size = kw.pop("chunk_size", 200)  # keyword-only argument
        self._logger = kw.pop("logger", _logger)
        self._strategy = kw.pop("strategy", "flush")
        assert self._strategy in {"flush", "commit"}
        if kw:
            raise TypeError("Unknown arguments: %s" % ", ".join(kw))

        self._patch = None
        self._it = chunks(ids, self._chunk_size, fmt=self._browse)

    def _browse(self, ids):
        next(self._end(), None)
        args = self._cr_uid + (list(ids),)
        if not self._patch:
            self._patch = no_selection_cache_validation()
        self._patch.start()

        return self._model.browse(*args)

    def _end(self):
        if self._strategy == "commit":
            self._model.env.cr.commit()
        else:
            flush(self._model)
        invalidate(self._model, *self._cr_uid)
        if self._patch:
            self._patch.stop()
        if 0:
            yield

    def __iter__(self):
        if self._it is None:
            raise RuntimeError("%r ran twice" % (self,))

        it = chain.from_iterable(self._it)
        if self._logger:
            it = log_progress(it, self._logger, qualifier=self._model._name, size=self._size)
        self._it = None
        return chain(it, self._end())

    def __getattr__(self, attr):
        if self._it is None:
            raise RuntimeError("%r ran twice" % (self,))

        if not callable(getattr(self._model, attr)):
            raise TypeError("The attribute %r is not callable" % attr)

        it = self._it
        if self._logger:
            sz = (self._size + self._chunk_size - 1) // self._chunk_size
            qualifier = "%s[:%d]" % (self._model._name, self._chunk_size)
            it = log_progress(it, self._logger, qualifier=qualifier, size=sz)

        def caller(*args, **kwargs):
            args = self._cr_uid + args
            return [getattr(chnk, attr)(*args, **kwargs) for chnk in chain(it, self._end())]

        self._it = None
        return caller

    def create(self, values, **kw):
        """
        Create records.

        An alternative to the default `create` method of the ORM that is safe to use to
        create millions of records.

        :param list(dict) values: list of values of the records to create
        :param bool multi: whether to use the multi version of `create`, by default is
                           `True` from Odoo 12 and above
        """
        multi = kw.pop("multi", version_gte("saas~11.5"))
        if kw:
            raise TypeError("Unknown arguments: %s" % ", ".join(kw))

        if not values:
            raise ValueError("`create` cannot be called with an empty `values` argument")

        if self._size:
            raise ValueError("`create` can only called on empty `browse_record` objects.")

        ids = []
        size = len(values)
        it = chunks(values, self._chunk_size, fmt=list)
        if self._logger:
            sz = (size + self._chunk_size - 1) // self._chunk_size
            qualifier = "env[%r].create([:%d])" % (self._model._name, self._chunk_size)
            it = log_progress(it, self._logger, qualifier=qualifier, size=sz)

        self._patch = no_selection_cache_validation()
        for sub_values in it:
            self._patch.start()

            if multi:
                ids += self._model.create(sub_values).ids
            elif not self._cr_uid:
                ids += [self._model.create(sub_value).id for sub_value in sub_values]
            else:
                # old API, `create` directly return the id
                ids += [self._model.create(*(self._cr_uid + (sub_value,))) for sub_value in sub_values]

            next(self._end(), None)
        args = self._cr_uid + (ids,)
        return iter_browse(
            self._model, *args, chunk_size=self._chunk_size, logger=self._logger, strategy=self._strategy
        )


@contextmanager
def custom_module_field_as_manual(env, rollback=True, do_flush=False):
    """
    Mark fields of custom modules as `manual`.

    Helper to be used with a Python `with` statement, to perform an operation with models and fields coming
    from Python modules acting as `manual` models/fields, while restoring back the state of these models
    and fields once the operation done.
    e.g.
     - validating views coming from custom modules, with the fields loaded as the custom source code was there,
     - crawling the menus as the models/fields coming from custom modules were available.

    !!! Rollback might be deactivated with the `rollback` parameter but for internal purpose ONLY !!!

    :meta private: exclude from online docs
    """
    # 1. Convert models which are not in the registry to `manual` models
    #    and list the models that were converted, to restore them back afterwards.
    models = list(env.registry.models)
    # Get all models that seem to come from uninstalled odoo modules, they won't be loaded
    # but we cannot consider them as custom models
    env.cr.execute(
        """
        SELECT m.model
          FROM ir_model m
          JOIN ir_model_data d
            ON d.res_id = m.id
           AND d.model = 'ir.model'
          JOIN ir_module_module x
            ON x.name = d.module
         WHERE d.module IN %s
         GROUP BY m.model
        HAVING bool_and(COALESCE(x.state, 'uninstalled') = 'uninstalled')
        """,
        [tuple(modules.get_modules())],
    )
    models_uninstalled_standard_modules = [r[0] for r in env.cr.fetchall()]
    if models_uninstalled_standard_modules:
        _logger.warning("Leftover models from uninstalled standard modules: %s", models_uninstalled_standard_modules)
    env.cr.execute(
        """
        UPDATE ir_model
           SET state = 'manual'
         WHERE state = 'base'
           AND model not in %s
     RETURNING id, model
    """,
        (tuple(models + models_uninstalled_standard_modules),),
    )
    updated_models = env.cr.fetchall()
    updated_model_ids, custom_models = zip(*updated_models) if updated_models else [[], []]

    mock_x_email_fields = []
    if updated_model_ids and version_gte("14.0") and "mail.thread.blacklist" in env:
        # 1.1 Add a mock x_email field for custom mail.blacklist models
        env.cr.execute(
            """
            INSERT INTO ir_model_fields(name, model, model_id, field_description,
                                        state, compute, store, ttype)
                 SELECT 'x_email', model, id, jsonb_build_object('en_US', 'Mock email field for custom model'){},
                        'manual', 'for record in records: record["x_email"] = "upgrade@example.com"', false, 'char'
                   FROM ir_model
                  WHERE id IN %s
                    AND is_mail_blacklist
            ON CONFLICT DO NOTHING
              RETURNING id
            """.format("" if version_gte("16.0") else "->>'en_US'"),
            [tuple(updated_model_ids)],
        )
        mock_x_email_fields = [r[0] for r in env.cr.fetchall()]

    temp_ir_model_access_ids = []
    if version_gte("14.0") and custom_models:
        # 1.2 Version 14.0 and above requires new access rights for transient models,
        # and the rights are yet to be added post standard upgrade for custom modules.
        # To avoid access errors, create some temporary access rights for these models.
        env.cr.execute(
            """
                INSERT INTO ir_model_access (name, model_id, active, perm_read)
                     SELECT 'tmp_access', ir_model.id, true, true
                       FROM ir_model
                  LEFT JOIN ir_model_access
                         ON ir_model_access.model_id = ir_model.id
                      WHERE ir_model.model IN %s
                        AND ir_model.transient
                        AND ir_model_access.id IS NULL
                  RETURNING id
            """,
            [custom_models],
        )
        temp_ir_model_access_ids = [r[0] for r in env.cr.fetchall()]

    # 2. Convert fields which are not in the registry to `manual` fields
    # and list the fields that were converted, to restore them back afterwards.
    # Also temporarily disable rules (ir.rule) that come from custom modules.
    updated_field_ids = []

    # 2.1 Convert fields not in the registry of models already in the registry.
    # defuse constraint for manual fields, ref: odoo/odoo@3d2f766
    env.cr.execute("SELECT 1 FROM pg_constraint WHERE conname = 'ir_model_fields_name_manual_field'")
    have_constraint = bool(env.cr.rowcount)
    if have_constraint:
        env.cr.execute("ALTER TABLE ir_model_fields DROP CONSTRAINT ir_model_fields_name_manual_field")
    # In the past, some models added the reserved word `env` as field (e.g. `payment.acquirer`)
    # if the field was not correctly removed from the database during past upgrades, the field remains in the database.
    reserved_words = ["env"]
    ignores = {"ir.actions.server": ["condition"], "ir.ui.view": ["page"]}
    for model in models:
        model_fields = tuple(list(env.registry[model]._fields) + reserved_words + ignores.get(model, []))
        env.cr.execute(
            """
            UPDATE ir_model_fields
               SET state = 'manual'
             WHERE state = 'base'
               AND model = %s
               AND name not in %s
         RETURNING id
        """,
            [model, model_fields],
        )
        updated_field_ids += [r[0] for r in env.cr.fetchall()]

    # 2.2 Convert fields of custom models, models that were just converted to `manual` models in the previous step.
    for model in custom_models:
        env.cr.execute(
            """
            UPDATE ir_model_fields
               SET state = 'manual'
             WHERE state = 'base'
               AND model = %s
               AND name not in %s
         RETURNING id
        """,
            (model, tuple(reserved_words)),
        )
        updated_field_ids += [r[0] for r in env.cr.fetchall()]

    # 2.3 Temporarily disable rules that come from custom modules
    standard_modules = modules.get_modules()

    env.cr.execute(
        """
        WITH custom_rules AS (
                SELECT r.id
                  FROM ir_rule r
             LEFT JOIN ir_model_data d ON d.model = 'ir.rule' AND d.res_id = r.id
                 WHERE COALESCE(d.module, '') NOT IN %s
                   AND r.active
                 )
        UPDATE ir_rule r
           SET active = false
          FROM custom_rules c
         WHERE r.id = c.id
     RETURNING r.id
         """,
        (tuple(standard_modules),),
    )
    disabled_ir_rule_ids = [r[0] for r in env.cr.fetchall()]

    # 3. Alter fields which won't work by just changing their `state` to `manual`,
    #    because information from their Python source is missing.
    #    List them and what was altered, to restore them back afterwards.

    # 3.1. Loading of base selection fields which have been converted to manual,
    #     for which we don't have the possible values.
    updated_selection_fields = []
    # For Odoo <= 12.0.
    # From 13.0, there is a model `ir.model.fields.selection` holding the values,
    # and `ir.model.fields.selection` becomes a computed field.
    if not env.registry["ir.model.fields"]._fields["selection"].compute:
        env.cr.execute(
            """
               UPDATE ir_model_fields
                  SET selection = '[]'
                WHERE state = 'manual'
                  AND COALESCE(selection, '') = ''
            RETURNING id, selection
            """
        )
        updated_selection_fields += env.cr.fetchall()

    # 3.2. Loading of base many2one fields converted to manual, set to `required` and `on_delete` to `set null` in db,
    #     which is not accepted by the ORM:
    #     https://github.com/odoo/odoo/blob/2a7e06663c1281f0cf75f72fc491bc2cc39ef81c/odoo/fields.py#L2404-L2405
    env.cr.execute(
        """
           UPDATE ir_model_fields
              SET on_delete = 'restrict'
            WHERE state = 'manual'
              AND ttype = 'many2one'
              AND required
              AND on_delete = 'set null'
        RETURNING id, on_delete
        """
    )
    updated_many2one_fields = env.cr.fetchall()

    # 3.4. models `_rec_name` are not reloaded correctly.
    #      If the model has no `_rec_name` and there is a manual field `name` or `x_name`,
    #      the `_rec_name` becomes this name field. But then, when we convert back the manual fields to base field,
    #      and we reload the registry, the `_rec_name` is not reset by the ORM,
    #      and there is an assert raising in `models.py`: `assert cls._rec_name in cls._fields`.
    rec_names = {key: model._rec_name for key, model in env.registry.models.items()}

    # 3.5 patches
    patches = []  # Ideally a contextlib.ExitStack but we need to support here Python2.7

    @contextmanager
    def all_patches():
        for p in patches:
            p.start()
        yield
        for p in reversed(patches):
            p.stop()

    if version_gte("9.0"):
        # 3.5.1 `_build_model` calls `check_pg_name` even if the table is not created/altered, and in some cases
        # models that have been converted to manual have a too long name, and we dont have the `_table` info.
        patches.append(patch("odoo.models.check_pg_name", lambda name: None))

    if version_between("9.0", "saas~14.3"):
        # 3.5.2: `display_name` is added automatically, as a base field, and depends on the field `name`
        # Sometimes, a custom model has no `name` field or it couldn't be loaded (e.g. an invalid `related`)
        # Mark it as manual so its skipped on loading fail.
        from odoo.models import BaseModel

        # Since saas-14.4, _add_magic_fields() no longer exists.  Moreover,
        # '_rec_name' is automatically fixed when the field it refers to is
        # dropped from the model's class.  Therefore, 'display_name' no
        # longer needs to become manual.
        origin_add_magic_fields = BaseModel._add_magic_fields

        def _add_magic_fields(self):
            res = origin_add_magic_fields(self)
            if self._custom and "display_name" in self._fields:
                self._fields["display_name"].manual = True
            return res

        patches.append(patch.object(BaseModel, "_add_magic_fields", _add_magic_fields))

    if version_gte("saas~16.4"):
        # 3.5.3 allow loading manual fields
        patches.append(patch("odoo.addons.base.models.ir_model.IrModelFields._is_manual_name", lambda self, name: True))

    with all_patches():
        # 4. Reload the registry with the models and fields converted to manual.
        setup_models = (
            env.registry._setup_models__ if hasattr(env.registry, "_setup_models__") else env.registry.setup_models
        )
        setup_models(env.cr)

    # 5. Do the operation.
    yield

    if rollback:
        if do_flush:
            flush(env["base"])

        # 6. Restore back models and fields converted from `base` to `manual`.
        if updated_model_ids:
            env.cr.execute("UPDATE ir_model SET state = 'base' WHERE id IN %s", (tuple(updated_model_ids),))
            if mock_x_email_fields:
                # Remove mock x_email field for custom mail.blacklist models
                env.cr.execute("DELETE FROM ir_model_fields WHERE id IN %s", [tuple(mock_x_email_fields)])

        if updated_field_ids:
            env.cr.execute("UPDATE ir_model_fields SET state = 'base' WHERE id IN %s", (tuple(updated_field_ids),))
        if have_constraint:
            # restore constraint for manual fields, ref: odoo/odoo@3d2f766
            env.cr.execute(
                r"""
                ALTER TABLE ir_model_fields
                  ADD CONSTRAINT ir_model_fields_name_manual_field
                CHECK (state != 'manual' OR name LIKE 'x\_%')
                """
            )
        if disabled_ir_rule_ids:
            env.cr.execute("UPDATE ir_rule SET active = 't' WHERE id IN %s", (tuple(disabled_ir_rule_ids),))
        if temp_ir_model_access_ids:
            env.cr.execute("DELETE FROM ir_model_access WHERE id IN %s", [tuple(temp_ir_model_access_ids)])
        for field_id, selection in updated_selection_fields:
            env.cr.execute("UPDATE ir_model_fields SET selection = %s WHERE id = %s", (selection, field_id))
        for field_id, on_delete in updated_many2one_fields:
            env.cr.execute("UPDATE ir_model_fields SET on_delete = %s WHERE id = %s", [on_delete, field_id])
        for model, rec_name in rec_names.items():
            env.registry[model]._rec_name = rec_name

        # 7. Reload the registry as before
        env.clear()
        setup_models = (
            env.registry._setup_models__ if hasattr(env.registry, "_setup_models__") else env.registry.setup_models
        )
        setup_models(env.cr)
