import json
from collections import namedtuple
from copy import deepcopy
from typing import Iterable

from odoo.upgrade import util

CommandAdapter = namedtuple("CommandAdapter", ["command_type", "adapt"])

Drop = object()

# not used
def transform_revisions(cr, revisions, *adapters: Iterable[CommandAdapter]):
    if not len(adapters):
        return
    if not util.table_exists(cr, "spreadsheet_revision"):
        return
    cmd_types = [x[0] for x in adapters]
    cr.execute(
        """
        SELECT id, commands
          FROM spreadsheet_revision
         WHERE commands SIMILAR TO %s
        """,
        (f"%{'%|%'.join(cmd_types)}%",),
    )
    for revision_id, data in cr.fetchall():
        data = json.loads(data)
        commands = data.get("commands", [])
        if not commands:
            continue
        new_commands = transform_commands(commands, *adapters)
        if new_commands is None:
            continue
        data["commands"] = new_commands
        cr.execute(
            """
            UPDATE spreadsheet_revision
                SET commands=%s
                WHERE id=%s
            """,
            [json.dumps(data), revision_id],
        )

def transform_revisions_data(revisions, *adapters: Iterable[CommandAdapter]):
    for data in revisions:
        commands = data.get("commands", [])
        if not commands:
            continue
        new_commands = transform_commands(commands, *adapters)
        if new_commands is None:
            continue
        # ensures it's jsonyfiable (converts tuples to lists) (probably should be removed)
        data["commands"] = json.loads(json.dumps(new_commands))
    return revisions


def transform_commands(commands, *adapters: Iterable[CommandAdapter]):
    to_update = False
    new_commands = []
    for command in commands:
        adapted_command = command
        for adapter_type, adapter in adapters:
            if command["type"] == adapter_type and adapted_command is not Drop:
                to_adapt = deepcopy(adapted_command)
                result = adapter(to_adapt) or to_adapt
                if result is Drop:
                    to_update = True
                    adapted_command = Drop
                    continue
                if result != adapted_command:
                    to_update = True
                adapted_command = result
        if adapted_command is not Drop:
            new_commands.append(adapted_command)
    return new_commands if to_update else None
