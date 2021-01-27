#!/usr/bin/env python3
import subprocess
import sys
from pathlib import PurePath

py2_patterns = [
    "src/*.py",
    "src/*/0.0.0/*.py",
]
py2_files = []

py3_patterns = [
    "tools/*.py",
    "src/*.py",
    "src/*/tests/*.py",
    "src/*/0.0.0/*.py",
]
py3_files = []

rc = 0

for filename in sys.argv[1:]:
    p = PurePath(filename)
    if p.suffix != ".py":
        continue

    if not filename.islower():
        print(f"filename {filename!r} is not lowecase")
        rc = 1

    if any(p.match(pattern) for pattern in py2_patterns):
        py2_files.append(filename)
    if any(p.match(pattern) for pattern in py3_patterns):
        py3_files.append(filename)


if py2_files:
    s = subprocess.run(["python2", "-m", "compileall", "-f", "-q", *py2_files], check=False)
    if s.returncode:
        rc = 1

if py3_files:
    s = subprocess.run(["python3", "-m", "compileall", "-f", "-q", *py3_files], check=False)
    if s.returncode:
        rc = 1

sys.exit(rc)
