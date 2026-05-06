import collections
import multiprocessing
import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from odoo import sql_db

from .. import json, pg
from ..misc import make_pickleable_callback

MEMORY_CAP = 2 * 10**8  # 200MB
COUNT_CAP = 1000


def _search_ids(cr, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    cr.execute(
        pg.format_query(
            cr,
            """
            WITH filtered AS (
                SELECT id,
                       LENGTH(commands) AS commands_length
                  FROM spreadsheet_revision
                 WHERE commands LIKE {condition} (%(like)s::text[])
            ), smaller AS (
                SELECT id,
                       sum(commands_length) OVER (ORDER BY id) / %(mem_cap)s AS num
                  FROM filtered
                 WHERE commands_length <= %(mem_cap)s
            ),
            aggregated AS (
                SELECT array_agg(id ORDER BY id) as arr
                  FROM smaller
              GROUP BY num

                 UNION ALL

                SELECT ARRAY[id] as arr
                  FROM filtered
                 WHERE commands_length > %(mem_cap)s
            ),
            chunked AS (
                SELECT arr[i : i + %(count_cap)s -1] as chunk
                  FROM aggregated,
                       LATERAL generate_series(1, cardinality(arr), %(count_cap)s) AS i
            )
            SELECT chunk
              FROM chunked
          ORDER BY chunk[1]
            """,
            condition=pg.SQLStr("ALL" if like_all else "ANY"),
        ),
        {
            "like": list(like_any or like_all),
            "mem_cap": MEMORY_CAP,
            "count_cap": COUNT_CAP,
        },
    )
    for (ids,) in cr.fetchall():
        yield ids


def _iter_ids(cr, ids):
    cr.execute("SELECT id, commands FROM spreadsheet_revision WHERE id=ANY(%s)", [list(ids)])
    for revision_id, data in cr.fetchall():
        data_loaded = json.loads(data)
        if "commands" not in data_loaded:
            continue
        data_old = json.dumps(data_loaded, sort_keys=True)

        changed = yield data_loaded["commands"]
        if changed is None:
            changed = data_old != json.dumps(data_loaded, sort_keys=True)

        if changed:
            cr.execute(
                "UPDATE spreadsheet_revision SET commands=%s WHERE id=%s",
                [json.dumps(data_loaded), revision_id],
            )


def iter_commands(cr, like_all=(), like_any=()):
    warnings.warn(
        "`iter_commands` is deprecated; use `multiprocess_commands` instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return _iter_commands(cr, like_all, like_any)


def _iter_commands(cr, like_all=(), like_any=()):
    for ids in _search_ids(cr, like_all, like_any):
        yield from _iter_ids(cr, ids)


def _transform(callback, gen):
    try:
        cmd = next(gen)
        while True:
            changed = callback(cmd)
            cmd = gen.send(changed)

    except StopIteration:
        pass


def process_commands(cr, callback, *args, **kwargs):
    warnings.warn(
        "`process_commands` is deprecated; use `multiprocess_commands` instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    _transform(callback, _iter_commands(cr, *args, **kwargs))


def _mp_callback(dbname, callback, ids):
    with sql_db.db_connect(dbname).cursor() as cr:
        _transform(callback, _iter_ids(cr, ids))


def multiprocess_commands(cr, callback, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    def init_worker_process():
        sql_db._Pool = None

    callback = make_pickleable_callback(callback)

    cr.commit()
    with ProcessPoolExecutor(
        max_workers=pg.get_max_workers(),
        initializer=init_worker_process,
        mp_context=multiprocessing.get_context("fork"),
    ) as executor:
        gen = _search_ids(cr, like_all=like_all, like_any=like_any)
        # consume the submitted tasks
        collections.deque(
            executor.map(partial(_mp_callback, cr.dbname, callback), gen),
            maxlen=0,
        )
    # Another commit() is necessary as we are in `REPEATABLE READ`, so the new transaction started by `_search_ids`, won't see the
    # changes done by the workers. Note that creating `gen` before the commit above won't help as it's code won't be executed before
    # the first iteration.
    cr.commit()
