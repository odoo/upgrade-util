from .. import json, pg

MEMORY_CAP = 2 * 10**8  # 200Mo


def iter_commands(cr, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")

    cr.execute(
        """
        WITH RECURSIVE
        start_bucket AS (
            SELECT 1 AS bucket
        ),
        ordered_rows AS (
            SELECT id,
                   LENGTH(commands) AS length,
                   ROW_NUMBER() OVER (ORDER BY LENGTH(commands), id) AS rownum
              FROM spreadsheet_revision
             WHERE commands LIKE {}(%s::text[])
        ),
        assign AS (
            SELECT o.id AS id,
                   o.length,
                   o.rownum,
                   sb.bucket AS bucket,
                   o.length AS sum
              FROM ordered_rows o, start_bucket sb
             WHERE o.rownum = 1

             UNION ALL

            SELECT o.id AS id,
                   o.length,
                   o.rownum,
                   CASE
                        WHEN a.sum + o.length > {memory_cap} THEN a.bucket + 1
                        ELSE a.bucket
                   END AS bucket,
                   CASE
                        WHEN a.sum + o.length > {memory_cap} THEN o.length
                        ELSE a.sum + o.length
                   END AS sum
              FROM assign a
              JOIN ordered_rows o
                ON o.rownum = a.rownum + 1
        )
        SELECT count(*),ARRAY_AGG(id)
          FROM assign
      GROUP BY bucket
      ORDER BY bucket;
        """.format("ALL" if like_all else "ANY", memory_cap=MEMORY_CAP),
        [list(like_all or like_any)],
    )
    buckets = cr.fetchall()
    if not buckets:
        return

    with pg.named_cursor(cr) as ncr:
        ncr.execute(
            """
            SELECT id,
                   commands
              FROM spreadsheet_revision
             WHERE id IN %s
          ORDER BY LENGTH(commands), id
            """,
            [tuple([i for bucket in buckets for i in bucket[1]])],
        )
        for size, _ in buckets:
            for revision_id, data in ncr.fetchmany(size=size):
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
