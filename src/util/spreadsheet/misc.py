from .. import json


def iter_commands(cr, like_all=(), like_any=()):
    if not (bool(like_all) ^ bool(like_any)):
        raise ValueError("Please specify `like_all` or `like_any`, not both")
    cr.execute(
        """
        SELECT id,
               commands
          FROM spreadsheet_revision
         WHERE commands LIKE {}(%s::text[])
        """.format("ALL" if like_all else "ANY"),
        [list(like_all or like_any)],
    )
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
                "UPDATE spreadsheet_revision SET commands=%s WHERE id=%s", [json.dumps(data_loaded), revision_id]
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
