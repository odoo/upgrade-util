# -*- coding: utf-8 -*-
import logging
import re
from contextlib import contextmanager
from itertools import chain
from operator import itemgetter
from textwrap import dedent

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

try:
    from odoo import SUPERUSER_ID, modules, release
except ImportError:
    from openerp import SUPERUSER_ID, release, modules

from .const import BIG_TABLE_THRESHOLD
from .exceptions import MigrationError
from .helpers import table_of_model
from .misc import chunks, log_progress, version_gte
from .pg import column_exists

# python3 shims
try:
    basestring
except NameError:
    basestring = str

_logger = logging.getLogger(__name__)


def env(cr):
    """
    Creates a new environment from cursor.

    ATTENTION: This function does NOT empty the cache maintained on the cursor
    for superuser with and empty environment. A call to invalidate_cache will
    most probably be necessary every time you directly modify something in database
    """
    try:
        from odoo.api import Environment
    except ImportError:
        try:
            from openerp.api import Environment
        except ImportError:
            v = release.major_version
            raise MigrationError("Hold on! There is not yet `Environment` in %s" % v)  # noqa: B904
    return Environment(cr, SUPERUSER_ID, {})


def get_admin_channel(cr):
    e = env(cr)
    admin_channel = None
    if "mail.channel" in e:
        admin_group = e.ref("base.group_system", raise_if_not_found=False)
        if admin_group:
            admin_channel = e["mail.channel"].search(
                [
                    ("public", "=", "groups"),
                    ("group_public_id", "=", admin_group.id),
                    ("group_ids", "in", admin_group.id),
                ]
            )
            if not admin_channel:
                admin_channel = e["mail.channel"].create(
                    {
                        "name": "Administrators",
                        "public": "groups",
                        "group_public_id": admin_group.id,
                        "group_ids": [(6, 0, [admin_group.id])],
                    }
                )
    return admin_channel


def guess_admin_id(cr):
    """guess the admin user id of `cr` database"""
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
    xid = "__upgrade__.cron_" + re.sub(r"\W+", "_", name.lower())
    number, unit = interval
    # TODO handle version <=10.saas-14
    cron = {
        "name": "Post Upgrade: %s" % (name,),
        "model_id": model_id,
        "state": "code",
        "code": dedent(code),
        "interval_number": number,
        "interval_type": unit,
        "numbercall": -1,
    }

    e = env(cr)
    data = dict(module="__upgrade__", xml_id=xid, values=cron, noupdate=True)
    if hasattr(e["ir.cron"], "_load_records"):
        e["ir.cron"]._load_records([data])
    else:
        # < saas-11.5
        e["ir.model.data"]._update("ir.cron", **data)


_TRACKING_ATTR = "tracking" if version_gte("saas~12.2") else "track_visibility"


def recompute_fields(cr, model, fields, ids=None, logger=_logger, chunk_size=256, strategy="auto"):
    assert strategy in {"flush", "commit", "auto"}
    Model = env(cr)[model] if isinstance(model, basestring) else model
    model = Model._name
    flush = getattr(Model, "flush", lambda: None)
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
        records.recompute()
        if strategy == "commit":
            cr.commit()
        else:
            flush()
        records.invalidate_cache()


class iter_browse(object):
    __slots__ = ("_model", "_cr_uid", "_size", "_chunk_size", "_logger", "_strategy", "_flush", "_it")

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
            raise TypeError("Unknow arguments: %s" % ", ".join(kw))

        self._flush = getattr(model, "flush", lambda: None)

        self._it = chunks(ids, self._chunk_size, fmt=self._browse)

    def _browse(self, ids):
        next(self._end(), None)
        args = self._cr_uid + (list(ids),)
        return self._model.browse(*args)

    def _end(self):
        if self._strategy == "commit":
            self._model.env.cr.commit()
        else:
            self._flush()
        self._model.invalidate_cache(*self._cr_uid)
        if 0:
            yield

    def __iter__(self):
        it = chain.from_iterable(self._it)
        if self._logger:
            it = log_progress(it, self._logger, qualifier=self._model._name, size=self._size)
        return chain(it, self._end())

    def __getattr__(self, attr):
        if not callable(getattr(self._model, attr)):
            raise AttributeError("The attribute %r is not callable" % attr)

        it = self._it
        if self._logger:
            sz = (self._size + self._chunk_size - 1) // self._chunk_size
            qualifier = "%s[:%d]" % (self._model._name, self._chunk_size)
            it = log_progress(it, self._logger, qualifier=qualifier, size=sz)

        def caller(*args, **kwargs):
            return [getattr(chnk, attr)(*args, **kwargs) for chnk in chain(it, self._end())]

        return caller

    def create(self, values):
        ids = []
        for sub_values in chunks(values, self._chunk_size, fmt=list):
            ids += self._model.create(sub_values).ids
            next(self._end(), None)
        args = self._cr_uid + (ids,)
        return iter_browse(
            self._model, *args, chunk_size=self._chunk_size, logger=self._logger, strategy=self._strategy
        )


@contextmanager
def custom_module_field_as_manual(env, rollback=True):
    """
    Helper to be used with a Python `with` statement,
    to perform an operation with models and fields coming from Python modules acting as `manual` models/fields,
    while restoring back the state of these models and fields once the operation done.
    e.g.
     - validating views coming from custom modules, with the fields loaded as the custom source code was there,
     - crawling the menus as the models/fields coming from custom modules were available.

    !!! Rollback might be deactivated with the `rollback` parameter but for internal purpose ONLY !!!
    """
    # 1. Convert models which are not in the registry to `manual` models
    #    and list the models that were converted, to restore them back afterwards.
    models = list(env.registry.models)
    env.cr.execute(
        """
        UPDATE ir_model
           SET state = 'manual'
         WHERE state = 'base'
           AND model not in %s
     RETURNING id, model
    """,
        (tuple(models),),
    )
    updated_models = env.cr.fetchall()
    updated_model_ids, custom_models = zip(*updated_models) if updated_models else [[], []]

    # 2. Convert fields which are not in the registry to `manual` fields
    # and list the fields that were converted, to restore them back afterwards.
    # Also temporarily disable rules (ir.rule) that come from custom modules.
    updated_field_ids = []

    # 2.1 Convert fields not in the registry of models already in the registry.
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

    # 3.4. Custom model set as mail thread will try to load all the fields of the `mail.thread`,
    #      even if the column in database as not been created yet because the -u on the custom module did not occur yet.
    #      Fields of the mails threads are in the `ir_model_fields` table, so not need to automatically add them anyway
    #      e.g. `message_main_attachment_id` is added between 12.0 and 13.0.
    updated_mixin_ids = {}
    for mixin, mixin_column in [
        ("mail.thread", "is_mail_thread"),
        ("mail.activity.mixin", "is_mail_activity"),
        ("mail.thread.blacklist", "is_mail_blacklist"),
    ]:
        if column_exists(env.cr, "ir_model", mixin_column):
            env.cr.execute(
                """
                       UPDATE ir_model
                          SET %(column)s = false
                        WHERE state = 'manual'
                          AND %(column)s
                    RETURNING id
                """
                % {"column": mixin_column}
            )
            ids = [r[0] for r in env.cr.fetchall()]
            if ids:
                updated_mixin_ids[mixin_column] = ids
                env.cr.execute(
                    """
                           UPDATE ir_model_fields
                              SET state = 'manual'
                            WHERE state = 'base'
                              AND model_id IN %s
                              AND name IN %s
                        RETURNING id
                    """,
                    [tuple(ids), tuple(env[mixin]._fields.keys())],
                )
                updated_field_ids += [r[0] for r in env.cr.fetchall()]

    # 3.4. models `_rec_name` are not reloaded correctly.
    #      If the model has no `_rec_name` and there is a manual field `name` or `x_name`,
    #      the `_rec_name` becomes this name field. But then, when we convert back the manual fields to base field,
    #      and we reload the registry, the `_rec_name` is not reset by the ORM,
    #      and there is an assert raising in `models.py`: `assert cls._rec_name in cls._fields`.
    rec_names = {key: model._rec_name for key, model in env.registry.models.items()}

    # 3.5 patches
    # 3.5.1 `_build_model` calls `check_pg_name` even if the table is not created/altered, and in some cases
    # models that have been converted to manual have a too long name, and we dont have the `_table` info.
    if version_gte("9.0"):
        with patch("odoo.models.check_pg_name", lambda name: None):
            # 3.5.2: `display_name` is added automatically, as a base field, and depends on the field `name`
            # Sometimes, a custom model has no `name` field or it couldn't be loaded (e.g. an invalid `related`)
            # Mark it as manual so its skipped on loading fail.
            from odoo.models import BaseModel

            try:
                origin_add_magic_fields = BaseModel._add_magic_fields

                def _add_magic_fields(self):
                    res = origin_add_magic_fields(self)
                    if self._custom and "display_name" in self._fields:
                        self._fields["display_name"].manual = True
                    return res

                def patch_display_name():
                    return patch.object(BaseModel, "_add_magic_fields", _add_magic_fields)

            except AttributeError:
                # Since saas-14.4, _add_magic_fields() no longer exists.  Moreover,
                # '_rec_name' is automatically fixed when the field it refers to is
                # dropped from the model's class.  Therefore, 'display_name' no
                # longer needs to become manual.
                @contextmanager
                def patch_display_name():
                    yield

            with patch_display_name():
                # 4. Reload the registry with the models and fields converted to manual.
                env.registry.setup_models(env.cr)

    # 5. Do the operation.
    yield

    if rollback:
        # 6. Restore back models and fields converted from `base` to `manual`.
        if updated_model_ids:
            env.cr.execute("UPDATE ir_model SET state = 'base' WHERE id IN %s", (tuple(updated_model_ids),))
        if updated_field_ids:
            env.cr.execute("UPDATE ir_model_fields SET state = 'base' WHERE id IN %s", (tuple(updated_field_ids),))
        if disabled_ir_rule_ids:
            env.cr.execute("UPDATE ir_rule SET active = 't' WHERE id IN %s", (tuple(disabled_ir_rule_ids),))
        for field_id, selection in updated_selection_fields:
            env.cr.execute("UPDATE ir_model_fields SET selection = %s WHERE id = %s", (selection, field_id))
        for field_id, on_delete in updated_many2one_fields:
            env.cr.execute("UPDATE ir_model_fields SET on_delete = %s WHERE id = %s", [on_delete, field_id])
        for mixin_column, ids in updated_mixin_ids.items():
            env.cr.execute("UPDATE ir_model SET %s = true WHERE id IN %%s" % mixin_column, (tuple(ids),))
        for model, rec_name in rec_names.items():
            env.registry[model]._rec_name = rec_name

        # 7. Reload the registry as before
        env.clear()
        env.registry.setup_models(env.cr)
