#!/usr/bin/env python3
# ruff: noqa: T201
import subprocess
import sys
from pathlib import PurePath
from shutil import which

py2_only_patterns = []
py2_files = []

py3_only_patterns = [
    "tools/*.py",
    # tests are only run from version 12. python2 compatibility is not needed.
    "src/testing.py",
    "src/util/jinja_to_qweb.py",
    "src/util/snippets.py",
    "src/util/convert_bootstrap.py",
    "src/*/tests/*.py",
    "src/*/17.0.*/*.py",
]
py3_files = []

rc = 0

for filename in sys.argv[1:]:
    p = PurePath(filename)
    if p.suffix != ".py":
        continue

    if not filename.islower():
        print(f"filename {filename!r} is not lowercase")
        rc = 1

    if any(p.match(pattern) for pattern in py2_only_patterns):
        py2_files.append(filename)
    elif any(p.match(pattern) for pattern in py3_only_patterns):
        py3_files.append(filename)
    else:
        # not an explicit match to a python version. Test against both versions.
        py2_files.append(filename)
        py3_files.append(filename)


if py2_files:
    if which("python2"):
        s = subprocess.run(["python2", "-m", "compileall", "-f", "-q", *py2_files], check=False)
        if s.returncode:
            rc = 1
    else:
        lines = [
            "WARNING: `python2` hasn't been found in $PATH",
            "You must ensure the following files are compatible with python2:",
            *[f" - {f}" for f in py2_files],
        ]
        width = max(map(len, lines))
        message = "\n".join(f"@ {line: <{width}s} @" for line in lines)
        extra = "@" * (width + 4)
        print(f"{extra}\n{message}\n{extra}", file=sys.stderr)

if py3_files:
    s = subprocess.run(["python3", "-m", "compileall", "-f", "-q", *py3_files], check=False)
    if s.returncode:
        rc = 1

sys.exit(rc)
