#!/usr/bin/env python3

from ast import literal_eval
from collections import defaultdict
from dataclasses import dataclass, field
from functools import total_ordering
import io
import itertools
from pathlib import Path
import subprocess
import sys
import tokenize
from typing import NamedTuple, Dict

import black


MODELS = ["osv", "osv_memory", "Model", "TransientModel", "AbstractModel"]
MODELS += [".".join(x).lstrip(".") for x in itertools.product(["openerp", "odoo", ""], ["osv", "models"], MODELS)]


class Repo(NamedTuple):
    name: str

    @property
    def remote(self):
        return f"git@github.com:odoo/{self.name}.git"


REPOSITORIES = [
    Repo("odoo"),
    Repo("enterprise"),
    Repo("design-themes"),
]


@total_ordering
@dataclass(eq=False)
class Version:
    name: str

    @property
    def fqn(self):
        if "." in self.name:
            return self.name.replace("-", "~")
        major, minor = self.ints
        return f"{major}.saas~{minor}"

    def __repr__(self):
        # This is hacky.
        # It will only be used when outputing the generated file.
        return f"parse_version({self.fqn!r})"

    @property
    def ints(self):
        s = list(map(int, self.name.replace("saas-", "").split(".")))
        if len(s) == 1:
            # < 11.0
            major = {range(1, 6): 7, range(6, 7): 8, range(7, 14): 9, range(14, 19): 10}
            for m, n in major.items():
                if s[0] in m:
                    return (n, s[0])
            raise ValueError(self.name)
        return tuple(s)

    def __eq__(self, other):
        return self.name == other.name

    def __lt__(self, other):
        return self.ints < other.ints

    def __hash__(self):
        return hash(self.name)  # Only name is relevant


VERSIONS = {Version(f"{major}.0") for major in range(7, 14)}
VERSIONS |= {Version(f"saas-{saas}") for saas in range(1, 19)}
VERSIONS |= {Version(f"saas-{major}.{minor}") for major in range(11, 14) for minor in range(1, 6)}

VERSIONS = sorted(VERSIONS)

IGNORED_FILES = [
    # defines `_name = LITERAL % CONSTANT`
    # does not have _inherit(s)
    "odoo/addons/google_calendar/google_calendar.py",
    "odoo/addons/google_calendar/models/google_calendar.py",
    "odoo/addons/website_version/models/google_management.py",
    "enterprise/website_version/models/google_management.py",
]


@dataclass(order=True)
class Inherit:
    model: str
    born: str  # inclusive
    dead: str = None  # non-inclusive


# from lib2to3.refactor.RefactoringTool class
def _read_python_source(filename):
    """
    Do our best to decode a Python source file correctly.
    """
    try:
        f = open(filename, "rb")
    except OSError:
        return None, None
    try:
        encoding = tokenize.detect_encoding(f.readline)[0]
    finally:
        f.close()
    with io.open(filename, "r", encoding=encoding, newline="") as f:
        return f.read(), encoding


@dataclass
class Visitor(black.Visitor):
    inh: Dict[str, set] = field(default_factory=lambda: defaultdict(set))

    def to_str(self, node):
        if isinstance(node, black.Node):
            return "".join(self.to_str(c) for c in node.children)
        return node.value

    def visit_classdef(self, node):
        classparent = None

        children = iter(node.children)
        child = next(children)
        while child.type != black.token.COLON:
            if child.type == black.token.LPAR:
                classparent = self.to_str(next(children))
            child = next(children)

        if classparent in MODELS:
            suite = next(children)
            name = None
            inh = []
            for child in suite.children:
                if child.type == black.syms.simple_stmt:
                    expr_stmt = child.children[0]
                    if expr_stmt.type != black.syms.expr_stmt:
                        continue
                    attr = self.to_str(expr_stmt.children[0])
                    if attr == "_name":
                        name = literal_eval(self.to_str(expr_stmt.children[2]))
                    elif attr == "_inherit":
                        node = expr_stmt.children[2]
                        if node.type == black.token.NAME and node.value == "_name":
                            inh.append(name)
                        else:
                            val = literal_eval(self.to_str(node))
                            if isinstance(val, str):
                                val = [val]
                            inh.extend(val)
                    elif attr == "_inherits":
                        val = literal_eval(self.to_str(expr_stmt.children[2]))
                        inh.extend(val.keys())
                    else:
                        # handle Many2one with delegate=True attribute
                        if (
                            len(expr_stmt.children) == 3
                            and expr_stmt.children[1].type == black.token.EQUAL
                            and expr_stmt.children[2].type == black.syms.power
                        ):
                            pw = expr_stmt.children[2]
                            if (self.to_str(pw.children[0]) + self.to_str(pw.children[1])) == "fields.Many2one":
                                arglist = pw.children[2].children[1]
                                comodel = None
                                delegate = False
                                for arg in arglist.children:
                                    if arg.type == black.token.STRING and comodel is None:
                                        comodel = literal_eval(self.to_str(arg))
                                    elif arg.type == black.syms.argument:
                                        if (
                                            self.to_str(arg.children[0]) == "delegate"
                                            and self.to_str(arg.children[2]) == "True"
                                        ):
                                            delegate = True
                                        if (
                                            self.to_str(arg.children[0]) == "comodel_name"
                                            and arg.children[2].type == black.token.STRING
                                        ):
                                            comodel = literal_eval(self.to_str(arg.children[2]))
                                if delegate and comodel:
                                    inh.append(comodel)

            if name:
                for i in inh:
                    if i != name:
                        self.inh[i].add(name)

        return []


def init_repos(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        p = path / repo.name
        if not p.exists():
            subprocess.run(
                ["git", "clone", repo.remote, repo.name], cwd=str(path), check=True,
            )
        else:
            subprocess.run(["git", "fetch", "-q"], cwd=str(p), check=True)


def checkout(wd: Path, repo: Repo, version: Version) -> bool:
    gitdir = str(wd / repo.name)

    hasref = subprocess.run(["git", "show-ref", "-q", "--verify", f"refs/remotes/origin/{version.name}"], cwd=gitdir)
    if hasref.returncode != 0:
        return False  # unknow branch
    subprocess.run(
        ["git", "checkout", "-q", "--force", "-B", version.name, f"origin/{version.name}"], cwd=gitdir, check=True
    )
    return True


def main():
    wd = Path("/tmp/inh")  # TODO make it configurable
    init_repos(wd)

    result = defaultdict(list)

    for version in VERSIONS:
        # if version.name != "saas-3":
        #     continue
        visitor = Visitor()
        for repo in REPOSITORIES:
            if not checkout(wd, repo, version):
                continue
            print(f"🔎 Process {repo.name} at version {version.name}", file=sys.stderr, flush=True)
            r = wd / repo.name
            for pyfile in r.glob("**/*.py"):
                fname = str(pyfile.relative_to(wd))
                if fname in IGNORED_FILES or "test" in fname:
                    continue
                code, _ = _read_python_source(pyfile)
                node = black.lib2to3_parse(code)
                try:
                    list(visitor.visit(node))
                except Exception:
                    print(f"💥 Cannot parse {pyfile} ({repo.name} {version.name})", file=sys.stderr, flush=True)
                    raise

        if not visitor.inh:
            continue

        for model, children in result.items():
            for child in children:
                if child.model not in visitor.inh[model] and not child.dead:
                    child.dead = version

        for model, children in visitor.inh.items():
            for child in children:
                for inh in result[model]:
                    if inh.model == child and not inh.dead:
                        break
                else:
                    result[model].append(Inherit(model=child, born=version))

    result = {m: sorted(result[m]) for m in sorted(result)}
    output = f"""\
# This file is auto-generated by `{sys.argv[0]}`. Edits may be lost.

from collections import namedtuple

try:
    from odoo.tools.misc import frozendict
    from odoo.tools.parse_version import parse_version
except ImportError:
    from openerp.tools.parse_version import parse_version
    try:
        from openerp.tools.misc import frozendict
    except ImportError:
        # frozendict only appears with new api in 8.0
        frozendict = dict

Inherit = namedtuple("Inherit", "model born dead")  # NOTE: dead is non-inclusive

inheritance_data = frozendict({result!r})
"""

    # TODO read length from pyproject.toml
    mode = black.FileMode(target_versions={black.TargetVersion.PY27}, line_length=120)
    print(black.format_str(output, mode=mode), end="")


# def debug():
#     code, _ = _read_python_source("/tmp/inh/odoo/addons/website_theme_install/models/ir_module_module.py")
#     node = black.lib2to3_parse(code)

#     black.DebugVisitor.show(node)
#     # v = Visitor()
#     # list(v.visit(node))
#     # print(v.inh)


if __name__ == "__main__":
    # debug()
    main()
