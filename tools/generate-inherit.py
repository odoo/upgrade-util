#!/usr/bin/env python3

import io
import itertools
import logging
import subprocess
import sys
import tokenize
from ast import literal_eval
from collections import defaultdict
from dataclasses import dataclass, field
from functools import total_ordering
from pathlib import Path
from typing import Dict, NamedTuple, Optional, Set, Tuple

import black
import tomli

try:
    from black.nodes import Visitor
except ImportError:
    # old black version
    from black import Visitor


logging.basicConfig(
    level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
if sys.stderr.isatty():
    logging.addLevelName(logging.INFO, "\033[1;32m\033[1;49mINFO\033[0m")
    logging.addLevelName(logging.CRITICAL, "\033[1;37m\033[1;41mCRITICAL\033[0m")

logger = logging.getLogger(__name__)

if int(black.__version__.split(".")[0]) >= 22:
    logger.critical("Too recent version of `black`. Please install version 21.12b0 in order to parse python2 code.")
    sys.exit(1)

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


@dataclass(order=True)
class Inherit:
    model: str
    born: Version  # inclusive
    dead: Optional[Version] = None  # non-inclusive
    via: Optional[str] = None  # Many2one field to parent in case of `_inherits`

    def apply_on(self, version: Version) -> bool:
        if self.dead is None:
            return self.born <= version
        return self.born <= version < self.dead


_NEXT_MAJOR = 16
_VERSIONS = {Version(f"{major}.0") for major in range(7, _NEXT_MAJOR)}
_VERSIONS |= {Version(f"saas-{saas}") for saas in range(1, 19)}
_VERSIONS |= {Version(f"saas-{major}.{minor}") for major in range(11, _NEXT_MAJOR) for minor in range(1, 6)}

VERSIONS = sorted(_VERSIONS)

IGNORED_FILES = [
    # defines `_name = LITERAL % CONSTANT`
    # does not have _inherit(s)
    "odoo/addons/google_calendar/google_calendar.py",
    "odoo/addons/google_calendar/models/google_calendar.py",
    "odoo/addons/website_version/models/google_management.py",
    "enterprise/website_version/models/google_management.py",
]

# Sometimes, new modules are added during a version lifetime and not forward-ported to dead saas~* version.
# Theses versions being dead and no upgrade to these versions being made, we can consider it contains some models
# Without it, we would end with holes in inherit tree.
VIRTUAL_INHERITS = {
    "account.report": [
        Inherit("account.cash.flow.report", born=Version("saas-11.1"), dead=Version("saas-12.5")),
        Inherit("l10n.lu.report.partner.vat.intra", born=Version("saas-13.1"), dead=Version("saas-13.2")),
    ],
    "l10n_cl.edi.util": [
        Inherit("stock.picking", born=Version("14.0"), dead=Version("saas-14.2")),
        Inherit(model="l10n_cl.daily.sales.book", born=Version("14.0"), dead=Version("saas-14.3"), via=None),
    ],
    "l10n_es.sii.account.tax.mixin": [
        Inherit(model="account.tax", born=Version("14.0"), dead=Version("saas-14.4"), via=None),
        Inherit(model="account.tax.template", born=Version("14.0"), dead=Version("saas-14.4"), via=None),
    ],
    "l10n_mx.trial.report": [
        Inherit("l10n_mx.trial.closing.report", born=Version("saas-11.1"), dead=Version("saas-12.2")),
    ],
    "l10n_mx_edi.pac.sw.mixin": [
        Inherit("account.invoice", born=Version("saas-11.1"), dead=Version("saas-12.5")),
        Inherit("account.payment", born=Version("saas-11.1"), dead=Version("saas-12.2")),
    ],
    "mail.activity.mixin": [
        Inherit("l10n_lu.yearly.tax.report.manual", born=Version("13.0"), dead=Version("15.0")),
        Inherit("l10n_uk.vat.obligation", born=Version("saas-15"), dead=Version("12.0")),
    ],
    "mail.thread": [
        Inherit("account.online.link", born=Version("12.0"), dead=Version("14.0")),
        Inherit(model="l10n_cl.daily.sales.book", born=Version("14.0"), dead=Version("saas-14.3"), via=None),
        Inherit("l10n_lu.yearly.tax.report.manual", born=Version("13.0"), dead=Version("15.0")),
        Inherit("l10n_uk.vat.obligation", born=Version("saas-15"), dead=Version("12.0")),
    ],
    "studio.mixin": [
        Inherit(model="ir.default", born=Version("14.0"), dead=Version("saas-14.2")),
    ],
    "google.gmail.mixin": [
        Inherit(model="fetchmail.server", born=Version("12.0"), dead=Version("15.0"), via=None),
        Inherit(model="ir.mail_server", born=Version("12.0"), dead=Version("15.0"), via=None),
    ],
}


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
class OdooVisitor(Visitor):
    inh: Dict[str, Set[Tuple[str, str]]] = field(default_factory=lambda: defaultdict(set))

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
                        node = expr_stmt.children[2]
                        if node.type == black.token.NAME and node.value == "_description":
                            # `_description` being required, some devs uses the following syntax: https://git.io/JUfhO
                            node = expr_stmt.children[4]
                        name = literal_eval(self.to_str(node))
                    elif attr == "_inherit":
                        node = expr_stmt.children[2]
                        if node.type == black.token.NAME and node.value == "_name":
                            inh.append((name, None))
                        else:
                            val = literal_eval(self.to_str(node))
                            if isinstance(val, str):
                                val = [val]
                            inh.extend((v, None) for v in val)
                    elif attr == "_inherits":
                        val = literal_eval(self.to_str(expr_stmt.children[2]))
                        inh.extend(val.items())
                    else:
                        # handle Many2one with delegate=True attribute
                        if (
                            len(expr_stmt.children) == 3
                            and expr_stmt.children[1].type == black.token.EQUAL
                            and expr_stmt.children[2].type == black.syms.power
                        ):
                            pw = expr_stmt.children[2]
                            if (self.to_str(pw.children[0]) + self.to_str(pw.children[1])) == "fields.Many2one":
                                via = self.to_str(expr_stmt.children[0])
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
                                    inh.append((comodel, via))

            if name:
                for i, via in inh:
                    if i != name:
                        self.inh[i].add((name, via))

        return []


def init_repos(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        p = path / repo.name
        if not p.exists():
            subprocess.run(
                ["git", "clone", repo.remote, repo.name],
                cwd=str(path),
                check=True,
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
    logger.info("⚙️  Initialize repositories")
    init_repos(wd)

    result = defaultdict(list)

    for version in VERSIONS:
        # if version.name != "saas-3":
        #     continue
        visitor = OdooVisitor()

        for model, virtuals in VIRTUAL_INHERITS.items():
            for virtual in virtuals:
                if virtual.apply_on(version):
                    visitor.inh[model].add((virtual.model, virtual.via))

        for repo in REPOSITORIES:
            if not checkout(wd, repo, version):
                continue
            logger.info("🔎 Process %s at version %s", repo.name, version.name)
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
                    logger.critical("💥 Cannot parse %s (%s %s)", pyfile, repo.name, version.name)
                    raise

        if not visitor.inh:
            continue

        for model, children in result.items():
            for child in children:
                if (child.model, child.via) not in visitor.inh[model] and not child.dead:
                    child.dead = version

        for model, children in visitor.inh.items():
            for child, via in children:
                for inh in result[model]:
                    if inh.model == child and inh.via == via and not inh.dead:
                        break
                else:
                    result[model].append(Inherit(model=child, born=version, via=via))

    result = {m: sorted(result[m]) for m in sorted(result)}
    me = Path(sys.argv[0])
    pyproject = Path(black.find_pyproject_toml((str(me.parent),)))

    output = f"""\
# This file is auto-generated by `{me.resolve().relative_to(pyproject.parent)}`. Edits will be lost.

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

Inherit = namedtuple("Inherit", "model born dead via")  # NOTE: dead is non-inclusive

inheritance_data = frozendict({result!r})
"""

    with open(pyproject, "r") as fp:
        line_length = tomli.load(fp)["tool"]["black"]["line-length"]
    mode = black.FileMode(target_versions={black.TargetVersion.PY27}, line_length=line_length)
    print(black.format_str(output, mode=mode), end="")


# def debug():
#     code, _ = _read_python_source("/tmp/inh/odoo/addons/website_theme_install/models/ir_module_module.py")
#     node = black.lib2to3_parse(code)

#     black.DebugVisitor.show(node)
#     # v = OdooVisitor()
#     # list(v.visit(node))
#     # print(v.inh)


if __name__ == "__main__":
    # debug()
    main()
