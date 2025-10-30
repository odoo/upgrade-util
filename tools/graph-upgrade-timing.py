#!/usr/bin/env -S uv run --script --quiet
# ruff: noqa: RET503

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pygal[png]",
# ]
# ///

import argparse
import os
import re
import sys
from datetime import datetime

import pygal  # also need cairosvg for png output

DT_PAT = r"((\d{4}-\d{2}-\d{2} \d{2}:\d{2}):\d{2},\d{3}) "


def process(options):
    pie = pygal.Pie()
    dt = None
    others = 0.0
    end_st = None
    end_mod = None
    for line in sys.stdin.readlines():
        if dt is None:
            match = re.match(DT_PAT, line)
            if match:
                dt = match.group(2)

        match = re.search(r"Module ([a-zA-Z0-9_]+) loaded in (\d+\.\d\d)s, \d+ queries", line)
        if match:
            time = float(match.group(2))
            if time > options.min_time:
                pie.add(match.group(1), time)
            else:
                others += time
            continue

        match = re.search(r"module ([a-zA-Z0-9_]+): Running migration \[\$", line)
        if match:
            mod = match.group(1)
            match = re.match(DT_PAT, line)
            if end_mod is None:
                end_mod = mod
                end_st = match.group(1)
            elif mod != end_mod:
                time = (datetime.fromisoformat(match.group(1)) - datetime.fromisoformat(end_st)).total_seconds()
                if time > options.min_time:
                    pie.add(f"{end_mod} [end]", time)
                else:
                    others += time
                end_mod = mod
                end_st = match.group(1)

        elif end_st:
            if " odoo.modules.loading:" in line or " odoo.addons.base.models.ir_model: Deleting " in line:
                # last `end-` script finished.
                match = re.match(DT_PAT, line)
                time = (datetime.fromisoformat(match.group(1)) - datetime.fromisoformat(end_st)).total_seconds()
                if time > options.min_time:
                    pie.add(f"{end_mod} [end]", time)
                else:
                    others += time
                end_st = None

    if options.min_time and others:
        pie.add("Other modules", others)

    title = f"{dt}"
    if options.min_time:
        title = f"{title} • Modules loaded in more than {options.min_time} seconds"
    pie.title = title

    if options.format == "png":
        return pie.render_to_png()
    elif options.format == "svg":
        return pie.render()


def main():
    # cat migration-14.0-latest.log | python3 graph-upgrade-timing.py -m 15 > graph.svg
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--format", type=str, choices=["svg", "png"], default="svg")
    parser.add_argument("-m", "--min-time", dest="min_time", type=float, default=0.0)

    options = parser.parse_args()
    with os.fdopen(sys.stdout.fileno(), "wb") as fp:
        fp.write(process(options))

    return 0


if __name__ == "__main__":
    sys.exit(main())
