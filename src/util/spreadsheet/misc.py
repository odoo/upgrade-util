from .. import json, pg

MEMORY_CAP = 2 * 10**8  # 200MB


def iter_commands(cr, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    with pg.named_cursor(cr, itersize=1) as ncr:
        ncr.execute(
            pg.format_query(
                cr,
                """
                WITH filtered AS (
                    SELECT id,
                           commands,
                           LENGTH(commands) AS commands_length
                      FROM spreadsheet_revision
                     WHERE commands LIKE {condition} (%s::text[])
                ), smaller AS (
                    SELECT id,
                           commands,
                           sum(commands_length) OVER (ORDER BY id) / %s AS num
                      FROM filtered
                     WHERE commands_length <= %s
                )
                SELECT array_agg(id ORDER BY id),
                       array_agg(commands ORDER BY id),
                       min(id) AS sort_key
                  FROM smaller
              GROUP BY num

                 UNION ALL

                SELECT ARRAY[id],
                       ARRAY[commands],
                       id AS sort_key
                  FROM filtered
                 WHERE commands_length > %s
              ORDER BY sort_key
                """,
                condition=pg.SQLStr("ALL" if like_all else "ANY"),
            ),
            [list(like_any or like_all), MEMORY_CAP, MEMORY_CAP, MEMORY_CAP],
        )
        for ids, commands, _ in ncr:
            for revision_id, data in zip(ids, commands):
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


def process_commands(cr, callback, *args, **kwargs):
    gen = iter_commands(cr, *args, **kwargs)
    try:
        cmd = next(gen)
        while True:
            changed = callback(cmd)
            cmd = gen.send(changed)

    except StopIteration:
        pass
