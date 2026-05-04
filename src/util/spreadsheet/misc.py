import collections
import logging
import multiprocessing
import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from odoo import sql_db

from .. import json
from ..misc import log_progress, make_pickleable_callback
from ..pg import SQLStr, format_query, get_max_workers

MEMORY_CAP = 2 * 10**8  # 200MB
COUNT_CAP = 1000
_logger = logging.getLogger(__name__)


def _search_ids(cr, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    cr.execute(
        format_query(
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
            condition=SQLStr("ALL" if like_all else "ANY"),
        ),
        {
            "like": list(like_any or like_all),
            "mem_cap": MEMORY_CAP,
            "count_cap": COUNT_CAP,
        },
    )
    return [ids for (ids,) in cr.fetchall()]


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


def multiprocess_commands(cr, callback, like_all=(), like_any=(), logger=_logger):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    def init_worker_process():
        sql_db._Pool = None

    callback = make_pickleable_callback(callback)

    it = _search_ids(cr, like_all=like_all, like_any=like_any)

    # Commit here achieves two goals:
    # 1. Commit transaction such that the workers can see changes.
    # 2. Ensure first query after the call to `multiprocess_commands` starts a new transaction
    #    and thus can _see_ the changes from the workers.
    cr.commit()
    with ProcessPoolExecutor(
        max_workers=get_max_workers(),
        initializer=init_worker_process,
        mp_context=multiprocessing.get_context("fork"),
    ) as executor:
        tasks = executor.map(partial(_mp_callback, cr.dbname, callback), it)
        if logger is not None:
            tasks = log_progress(tasks, logger, qualifier="spreadsheet revisions chunk", size=len(it))
        # consume the submitted tasks
        collections.deque(tasks, maxlen=0)
