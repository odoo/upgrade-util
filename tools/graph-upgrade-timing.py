#!/usr/bin/env python3
# ruff: noqa: RET503 RET505
import argparse
import os
import re
import sys

import pygal  # also need cairosvg for png output


def process(options):
    pie = pygal.Pie()
    dt = None
    others = 0.0
    for line in sys.stdin.readlines():
        if dt is None:
            match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}):\d{2},\d{3} ", line)
            if match:
                dt = match.group(1)

        match = re.search(r"Module ([a-z_]+) loaded in (\d+\.\d\d)s, \d+ queries", line)
        if match:
            time = float(match.group(2))
            if time > options.min_time:
                pie.add(match.group(1), time)
            else:
                others += time

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
