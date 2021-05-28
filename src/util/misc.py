# -*- coding: utf-8 -*-
"""
Misc standalone functions.
"""

import collections
import datetime
import os
import re
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
    unicode
except NameError:
    unicode = str


def version_gte(version):
    if "-" in version:
        raise SleepyDeveloperError("version cannot contains dash")
    return parse_version(release.serie) >= parse_version(version)


def has_enterprise():
    """Return whernever the current installation has enterprise addons availables"""
    # NOTE should always return True as customers need Enterprise to migrate or
    #      they are on SaaS, which include enterpise addons.
    #      This act as a sanity check for developpers or in case we release the scripts.
    if os.getenv("ODOO_HAS_ENTERPRISE"):
        return True
    # XXX maybe we will need to change this for version > 9
    return bool(get_module_path("delivery_fedex", downloaded=False, display_warning=False))


def has_design_themes():
    """Return whernever the current installation has theme addons availables"""
    if os.getenv("ODOO_HAS_DESIGN_THEMES"):
        return True
    return bool(get_module_path("theme_yes", downloaded=False, display_warning=False))


def splitlines(s):
    """yield stripped lines of `s`.
    Skip empty lines
    Remove comments (starts with `#`).
    """
    return (
        stripped_line for line in s.splitlines() for stripped_line in [line.split("#", 1)[0].strip()] if stripped_line
    )


def expand_braces(s):
    # expand braces (a la bash)
    # only handle one expension of a 2 parts (because we don't need more)
    r = re.compile(r"(.*){([^},]*?,[^},]*?)}(.*)")
    m = r.search(s)
    if not m:
        raise ValueError("No expansion braces found")
    head, match, tail = m.groups()
    a, b = match.split(",")
    first = head + a + tail
    second = head + b + tail
    if r.search(first):  # as the regexp will match the last expansion, we only need to verify first term
        raise ValueError("Multiple expansion braces found")
    return [first, second]


def split_osenv(name):
    return re.split(r"\W+", os.getenv(name, "").strip())


try:
    import importlib.util

    def import_script(path, name=None):
        if not name:
            name, _ = os.path.splitext(os.path.basename(path))
        full_path = os.path.join(os.path.dirname(__file__), "..", path)
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
    """Allow a contextmanager to not yield."""
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
    Split `iterable` into chunks of `size` and wrap each chunk
    using function 'fmt' (`iter` by default; join strings)

    >>> list(chunks(range(10), 4, fmt=tuple))
    [(0, 1, 2, 3), (4, 5, 6, 7), (8, 9)]
    >>> ' '.join(chunks('abcdefghijklm', 3))
    'abc def ghi jkl m'
    >>>

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
        if (t2 - t1).total_seconds() > 60 or (log_hundred_percent and i == size and (t2 - t0).total_seconds() > 10):
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


class SelfPrint(object):
    """Class that will return a self representing string. Used to evaluate domains."""

    def __init__(self, name):
        self.__name = name

    def __getattr__(self, attr):
        return SelfPrint("%r.%s" % (self, attr))

    def __call__(self, *args, **kwargs):
        s = []
        for a in args:
            s.append(repr(a))
        for k, v in kwargs.items():
            s.append("%s=%r" % (k, v))
        return SelfPrint("%r(%s)" % (self, ", ".join(s)))

    def __add__(self, other):
        return SelfPrint("%r + %r" % (self, other))

    def __radd__(self, other):
        return SelfPrint("%r + %r" % (other, self))

    def __sub__(self, other):
        return SelfPrint("%r - %r" % (self, other))

    def __rsub__(self, other):
        return SelfPrint("%r - %r" % (other, self))

    def __mul__(self, other):
        return SelfPrint("%r * %r" % (self, other))

    def __rmul__(self, other):
        return SelfPrint("%r * %r" % (other, self))

    def __div__(self, other):
        return SelfPrint("%r / %r" % (self, other))

    def __rdiv__(self, other):
        return SelfPrint("%r / %r" % (other, self))

    def __floordiv__(self, other):
        return SelfPrint("%r // %r" % (self, other))

    def __rfloordiv__(self, other):
        return SelfPrint("%r // %r" % (other, self))

    def __mod__(self, other):
        return SelfPrint("%r %% %r" % (self, other))

    def __rmod__(self, other):
        return SelfPrint("%r %% %r" % (other, self))

    def __repr__(self):
        return self.__name

    __str__ = __repr__


class SelfPrintEvalContext(collections.defaultdict):
    """Evaluation Context that will return a SelfPrint object for all non-literal object"""

    def __init__(self, *args, **kwargs):
        super(SelfPrintEvalContext, self).__init__(None, *args, **kwargs)

    def __missing__(self, key):
        return SelfPrint(key)
