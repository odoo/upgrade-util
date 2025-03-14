"""Miscellaneous standalone functions."""

import ast
import collections
import datetime
import functools
import logging
import os
import re
import uuid
from contextlib import contextmanager
from itertools import chain, islice

try:
    from odoo import release
    from odoo.modules.module import get_module_path
    from odoo.tools.parse_version import parse_version
except ImportError:
    from openerp import release
    from openerp.modules.module import get_module_path
    from openerp.tools.parse_version import parse_version

from .exceptions import SleepyDeveloperError

# python3 shim
try:
    unicode  # noqa: B018
except NameError:
    unicode = str

try:
    from ast import unparse as ast_unparse
except ImportError:
    try:
        from astunparse import unparse as ast_unparse
    except ImportError:
        ast_unparse = None

_logger = logging.getLogger(__name__)


def _cached(func):
    sentinel = object()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = getattr(func, "_result", sentinel)
        if result == sentinel:
            result = func._result = func(*args, **kwargs)
        return result

    return wrapper


# copied from odoo as older OpenERP versions doesn't have it
def str2bool(s, default=None):
    s = unicode(s).lower()
    y = ["y", "yes", "1", "true", "t", "on"]
    n = ["n", "no", "0", "false", "f", "off"]
    if s not in (y + n):
        if default is None:
            raise ValueError("Use 0/1/yes/no/true/false/on/off")
        return bool(default)
    return s in y


def version_gte(version):
    """
    Return whether currently running Odoo version is greater or equal to `version`.

    This function is useful for conditional execution in an upgrade script that applies to
    multiple versions, e.g. `0.0.0` scripts.

    :param str version: Odoo version, must follow the format `[saas~]X.Y` where `X` is the
                        major Odoo version, `saas~` is necessary only when `Y` is nonzero
    :rtype: bool
    """
    if "-" in version:
        raise SleepyDeveloperError("version cannot contains dash")
    return parse_version(release.serie) >= parse_version(version)


def version_between(a, b):
    """
    Return whether currently running Odoo version is in the range `[a,b]`.

    See also :func:`~odoo.upgrade.util.misc.version_gte`

    .. note::
       The bounds are inclusive.

    :param str a: Odoo version, lower bound
    :param str b: Odoo version, upper bound
    :rtype: bool

    """
    if "-" in a + b:
        raise SleepyDeveloperError("version cannot contains dash")
    return parse_version(a) <= parse_version(release.serie) <= parse_version(b)


@_cached
def has_enterprise():
    """
    Return whether the current installation has enterprise addons available.

    :meta private: exclude from online docs
    """
    # NOTE should always return True as customers need Enterprise to migrate or
    #      they are on SaaS, which include enterpise addons.
    #      This act as a sanity check for developers or in case we release the scripts.
    if os.getenv("ODOO_HAS_ENTERPRISE"):
        return True
    # XXX maybe we will need to change this for version > 9
    return bool(get_module_path("delivery_fedex", downloaded=False, display_warning=False))


@_cached
def has_design_themes():
    """
    Return whether the current installation has theme addons available.

    :meta private: exclude from online docs
    """
    if os.getenv("ODOO_HAS_DESIGN_THEMES"):
        return True
    return bool(get_module_path("theme_yes", downloaded=False, display_warning=False))


@_cached
def on_CI():
    return str2bool(os.getenv("MATT", "0")) or str2bool(os.getenv("CI", "0"))


def splitlines(s):
    """
    Yield stripped lines of `s`. Skip empty lines Remove comments (starts with `#`).

    :meta private: exclude from online docs
    """
    return (
        stripped_line for line in s.splitlines() for stripped_line in [line.split("#", 1)[0].strip()] if stripped_line
    )


def expand_braces(s):
    """
    Expand braces in the input.

    .. example::

       .. code-block:: python

          >>> util.expand_braces("a_{this,that}_b")
          ['a_this_b', 'a_that_b']

    :param str s: string to expand, must contain precisely one pair of braces.
    :return: expanded input
    """
    # expand braces (a la bash)
    r = re.compile(r"(.*){((?:[^},]*?)(?:,[^},]*?)+)}(.*)", flags=re.DOTALL)
    m = r.search(s)
    if not m:
        raise ValueError("No expansion braces found")
    head, matches, tail = m.groups()
    if re.search("[}{]", head + matches + tail):
        raise ValueError("Extra braces detected")
    return [head + x + tail for x in matches.split(",")]


def split_osenv(name, default=""):
    return re.split(r"\W+", os.getenv(name, default).strip())


try:
    import importlib.util
    from pathlib import Path

    try:
        import odoo.upgrade
    except ImportError:
        _search_path = [Path(__file__).parent.parent]
    else:
        _search_path = [Path(p) for p in odoo.upgrade.__path__]

    def import_script(path, name=None):
        """
        Import an upgrade script.

        This function allows to import functions from other upgrade scripts into the
        current one.

        .. example::
           In :file:`mymodule/15.0.1.0/pre-migrate.py`

           .. code-block:: python

              def my_util(cr):
                  # do stuff

           In :file:`myothermodule/16.0.1.0/post-migrate.py`

           .. code-block:: python

              from odoo.upgrade import util

              script = util.import_script("mymodule/15.0.1.0/pre-migrate.py")

              def migrate(cr, version):
                  script.my_util(cr)  # reuse the function

           This function returns a Python `module`.

           .. code-block:: python

              >>> util.import_script("base/0.0.0/end-moved0.py", name="my-moved0")
              <module 'my-moved0' from '/home/odoo/src/upgrade-util/src/base/0.0.0/end-moved0.py'>

        :param str path: relative path to the script to import in the form
                         `$module/$version/$script-name`

                         .. note::
                            The script must be available in the upgrade path.

        :param str or None name: name to assign to the returned module, take the name from
                                 the imported file if `None`
        :return: a module created from the imported upgrade script
        """
        if not name:
            name, _ = os.path.splitext(os.path.basename(path))
        for full_path in (sp / path for sp in _search_path):
            if full_path.exists():
                break
        else:
            raise ImportError(path)  # noqa: TRY301
        spec = importlib.util.spec_from_file_location(name, full_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

except ImportError:
    # python2 version
    import imp

    def import_script(path, name=None):
        if not name:
            name, _ = os.path.splitext(os.path.basename(path))
        full_path = os.path.join(os.path.dirname(__file__), "..", path)
        with open(full_path) as fp:
            return imp.load_source(name, full_path, fp)


@contextmanager
def skippable_cm():
    """
    Return a context manager to allow another context manager to not yield.

    See :func:`~odoo.upgrade.util.records.edit_view` for an example usage.
    """
    if not hasattr(skippable_cm, "_msg"):

        @contextmanager
        def _():
            if 0:
                yield

        try:
            with _():
                pass
        except RuntimeError as r:
            skippable_cm._msg = str(r)
    try:
        yield
    except RuntimeError as r:
        if str(r) != skippable_cm._msg:
            raise


def chunks(iterable, size, fmt=None):
    """
    Split `iterable` into chunks of `size` and wrap each chunk using `fmt` function.

    This function is useful for splitting huge input data into smaller chunks that can be
    processed independently.

    .. example::

       .. code-block:: python

          >>> list(chunks(range(10), 4, fmt=tuple))
          [(0, 1, 2, 3), (4, 5, 6, 7), (8, 9)]
          >>> ' '.join(chunks('abcdefghijklm', 3))
          'abc def ghi jkl m'

    :param iterable iterable: iterable object to split
    :param int size: chunk size
    :param function fmt: function to apply to each chunk, when `None` is passed `fmt`
                         becomes `"".join` if `iterable` is a string, otherwise `iter`
    :return: a generator that iterates over the result of `fmt` applied to each chunk
    """
    if fmt is None:
        # fmt:off
        fmt = {
            str: "".join,
            unicode: u"".join,
        }.get(type(iterable), iter)
        # fmt:on

    it = iter(iterable)
    try:
        while True:
            yield fmt(chain((next(it),), islice(it, size - 1)))
    except StopIteration:
        return


def log_progress(it, logger, qualifier="elements", size=None, estimate=True, log_hundred_percent=False):
    if size is None:
        size = len(it)
    t0 = t1 = datetime.datetime.now()
    for i, e in enumerate(it, 1):
        yield e
        t2 = datetime.datetime.now()
        secs_last, secs_start = (t2 - t1).total_seconds(), (t2 - t0).total_seconds()
        if secs_last > 60 or (log_hundred_percent and i == size and secs_start > 10):
            t1 = datetime.datetime.now()
            tdiff = t2 - t0
            j = float(i)
            if estimate:
                tail = " (total estimated time: %s)" % (datetime.timedelta(seconds=tdiff.total_seconds() * size / j),)
            else:
                tail = ""

            logger.info(
                "[%.02f%%] %d/%d %s processed in %s%s",
                (j / size * 100.0),
                i,
                size,
                qualifier,
                tdiff,
                tail,
            )


def log_chunks(it, logger, chunk_size, qualifier="items"):
    tinit = tlog = datetime.datetime.now()

    def log(chunk_num, size=chunk_size):
        now = datetime.datetime.now()
        logger.info(
            "Chunk #%d of %d %s processed in %s (total %s)",
            chunk_num,
            size,
            qualifier,
            now - tlog,
            now - tinit,
        )
        return now

    i = 0
    for i, e in enumerate(it, 1):
        yield e
        tlog = tlog if i % chunk_size else log(i // chunk_size)

    if i == 0:
        # empty iterator
        logger.info("No %s to process", qualifier)
    elif i % chunk_size != 0:
        # log the last partial chunk
        log(i // chunk_size + 1, i % chunk_size)


class SelfPrint(object):
    """
    Class that will return a self representing string. Used to evaluate domains.

    :meta private: exclude from online docs
    """

    def __init__(self, name):
        self.__name = name

    def __getattr__(self, attr):
        return SelfPrint("%r.%s" % (self, attr))

    def __getitem__(self, key):
        return SelfPrint("%r[%r]" % (self, key))

    def __call__(self, *args, **kwargs):
        s = [repr(a) for a in args]
        for k, v in kwargs.items():
            s.append("%s=%r" % (k, v))
        return SelfPrint("%r(%s)" % (self, ", ".join(s)))

    def __add__(self, other):
        return SelfPrint("(%r + %r)" % (self, other))

    def __radd__(self, other):
        return SelfPrint("(%r + %r)" % (other, self))

    def __sub__(self, other):
        return SelfPrint("(%r - %r)" % (self, other))

    def __rsub__(self, other):
        return SelfPrint("(%r - %r)" % (other, self))

    def __mul__(self, other):
        return SelfPrint("(%r * %r)" % (self, other))

    def __rmul__(self, other):
        return SelfPrint("(%r * %r)" % (other, self))

    def __div__(self, other):
        return SelfPrint("(%r / %r)" % (self, other))

    def __rdiv__(self, other):
        return SelfPrint("(%r / %r)" % (other, self))

    def __truediv__(self, other):
        return SelfPrint("(%r / %r)" % (self, other))

    def __rtruediv__(self, other):
        return SelfPrint("(%r / %r)" % (other, self))

    def __floordiv__(self, other):
        return SelfPrint("(%r // %r)" % (self, other))

    def __rfloordiv__(self, other):
        return SelfPrint("(%r // %r)" % (other, self))

    def __mod__(self, other):
        return SelfPrint("(%r %% %r)" % (self, other))

    def __rmod__(self, other):
        return SelfPrint("(%r %% %r)" % (other, self))

    def __pow__(self, other):
        return SelfPrint("(%r ** %r)" % (self, other))

    def __rpow__(self, other):
        return SelfPrint("(%r ** %r)" % (other, self))

    def __pos__(self):
        return SelfPrint("+(%r)" % (self,))

    def __neg__(self):
        return SelfPrint("-(%r)" % (self,))

    def __repr__(self):
        return self.__name

    __str__ = __repr__

    def __iter__(self):
        raise RuntimeError("Cannot self-print iterations")


class SelfPrintEvalContext(collections.defaultdict):
    """
    Evaluation Context that will return a SelfPrint object for all non-literal object.

    :meta private: exclude from online docs
    """

    def __init__(self, *args, **kwargs):
        super(SelfPrintEvalContext, self).__init__(None, *args, **kwargs)

    def __missing__(self, key):
        return SelfPrint(key)

    @classmethod
    def preprocess(klass, expr):
        """
        Prepare `expr` to be evaluated in a self printed context.

        This is necessary to avoid skipping domains that `SelfPrint` cannot print.
        Example: [('company_id', 'in', [*company_ids, False])

        Returns a pair with the new expression and an evaluation context that should
        be used in `safe_eval`.

        ```
        >>> prepared_domain, context = util.SelfPrintEvalContext.preprocess(domain)
        >>> safe_eval(prepared_domain, context, nocopy=True)
        ```

        :meta private: exclude from online docs
        """
        if not ast_unparse:
            return expr, SelfPrintEvalContext()

        class RewriteName(ast.NodeTransformer):
            def __init__(self):
                self.replaces = {}
                super(RewriteName, self).__init__()

            def _replace_node(self, prefix, node):
                uniq_id = prefix + uuid.uuid4().hex[:12]
                unparsed = ast_unparse(node).strip()
                self.replaces[uniq_id] = SelfPrint(unparsed)
                return ast.Name(id=uniq_id, ctx=ast.Load())

            def visit_Starred(self, node):
                return self._replace_node("_upg_Starred", node)

            def visit_BoolOp(self, node):
                return self._replace_node("_upg_BoolOp", node)

            def visit_UnaryOp(self, node):
                return self._replace_node("_upg_UnaryOp_Not", node) if isinstance(node.op, ast.Not) else node

        replacer = RewriteName()
        root = ast.parse(expr.strip(), mode="eval").body
        visited = replacer.visit(root)
        return (ast_unparse(visited).strip(), SelfPrintEvalContext(replacer.replaces))


class _Replacer(ast.NodeTransformer):
    """Replace literal nodes in an AST."""

    def __init__(self, mapping):
        self.mapping = collections.defaultdict(list)
        for key, value in mapping.items():
            key_ast = ast.parse(key, mode="eval").body
            self.mapping[key_ast.__class__.__name__].append((key_ast, value))

    def _no_match(self, left, right):
        return False

    def _match(self, left, right):
        same_ctx = getattr(left, "ctx", None).__class__ is getattr(right, "ctx", None).__class__
        cname = left.__class__.__name__
        same_type = right.__class__.__name__ == cname
        matcher = getattr(self, "_match_" + cname, self._no_match)
        return matcher(left, right) if same_type and same_ctx else False

    def _match_Constant(self, left, right):
        # we don't care about kind for u-strings
        return type(left.value) is type(right.value) and left.value == right.value

    def _match_Num(self, left, right):
        # Dreprecated, for Python <3.8
        return type(left.n) is type(right.n) and left.n == right.n

    def _match_Str(self, left, right):
        # Deprecated, for Python <3.8
        return left.s == right.s

    def _match_Name(self, left, right):
        return left.id == right.id

    def _match_Attribute(self, left, right):
        return left.attr == right.attr and self._match(left.value, right.value)

    def _match_List(self, left, right):
        return len(left.elts) == len(right.elts) and all(
            self._match(left_, right_) for left_, right_ in zip(left.elts, right.elts)
        )

    def _match_Tuple(self, left, right):
        return self._match_List(left, right)

    def _match_ListComp(self, left, right):
        return (
            self._match(left.elt, right.elt)
            and len(left.generators) == len(right.generators)
            and all(self._match(left_, right_) for left_, right_ in zip(left.generators, right.generators))
        )

    def _match_comprehension(self, left, right):
        return (
            # async is not expected in our use cases, just for completeness
            getattr(left, "is_async", 0) == getattr(right, "is_async", 0)
            and len(left.ifs) == len(right.ifs)
            and self._match(left.target, right.target)
            and self._match(left.iter, right.iter)
            and all(self._match(left_, right_) for left_, right_ in zip(left.ifs, right.ifs))
        )

    def visit(self, node):
        if node.__class__.__name__ in self.mapping:
            for target_ast, new in self.mapping[node.__class__.__name__]:
                if self._match(node, target_ast):
                    return ast.parse(new, mode="eval").body
        return super(_Replacer, self).visit(node)


def literal_replace(expr, mapping):
    if ast_unparse is None:
        _logger.critical("AST unparse unavailable")
        return expr
    root = ast.parse(expr.strip(), mode="eval").body
    visited = _Replacer(mapping).visit(root)
    return ast_unparse(visited).strip()
