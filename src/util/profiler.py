import inspect
import logging
import os
import sys
import time
from collections import defaultdict
from functools import partial, wraps
from inspect import getsourcefile
from pathlib import Path

from .const import ENVIRON, NEARLYWARN
from .misc import version_gte

try:
    from odoo.tools.profiler import Profiler, make_session
except ImportError:
    if version_gte("15.0"):
        raise

__all__ = ["profile", "trace"]

_logger = logging.getLogger(__name__)


if version_gte("15.0"):

    def profile(func=None, *, description=None, speedscope_file=None):
        """
        Profile an upgrade script function.

        :param func: The function to be profiled when used as ``@profile``.
                     When used as ``@profile(...)`` this argument is omitted.
        :type func: callable, optional
        :param description: Human-readable label for the profiled section.
                            If omitted, the current time, function's ``__name__`` and
                            source file is used.
        :type description: str, optional
        :param speedscope_file: If provided, the raw profiling data is written
                                to this path in speedscope-compatible JSON format
                                for interactive inspection at https://speedscope.app.
        :type speedscope_file: str or pathlib.Path, optional

        :returns: A wrapped version of *func* that, when invoked, executes the
                  original function under the profiler and store the results into
                  the database.
        :rtype: callable

        Usage
        -----
        Decorate the whole `migrate` function::

            @profile
            def migrate(cr, version):
                util.rename_field(cr, "some.model", "old_name", "new_name")

        You can also store the speedscope result into file on disk. Can be useful when
        the upgrade don't succeed or when the integrated viewer is not available.
        Example::

            @profile(speedscope_file="/tmp/ss_upgrade.json")
            def migrate(cr, version):
                util.rename_field(cr, "some.model", "old_name", "new_name")

        Notes
        -----
        The decorated function SHOULD take the cursor as first argument.
        """
        if func is None:
            return partial(profile, description=description, speedscope_file=speedscope_file)

        if speedscope_file and not Path(speedscope_file).is_absolute():
            raise ValueError("`speedscope_file` should be an absolute path")

        if description is None:
            description = "{} ({})".format(func.__name__, getsourcefile(func))

        @wraps(func)
        def decorator(cr, *args, **kwargs):
            _logger.warning("profiling function %s", func.__qualname__)

            session = ENVIRON.get("_profile_session")
            if session is None:
                session = ENVIRON["_profile_session"] = make_session(
                    "Upgrade {} ({} -> {})".format(
                        cr.dbname,
                        os.getenv("ODOO_UPG_DB_SOURCE_VERSION", "?"),
                        os.getenv("ODOO_UPG_DB_TARGET_VERSION", "?"),
                    )
                )

            profiler = Profiler(
                profile_session=session,
                description=description,
                collectors=["sql", "traces_async"],
                db=cr.dbname,
                params={"trace_async_interval": 0.01},
            )

            with profiler:
                result = func(cr, *args, **kwargs)

            _logger.warning("profiling took %0.6f seconds", profiler.duration)

            if speedscope_file:
                Path(speedscope_file).write_text(profiler.json())

            return result

        return decorator

else:

    def profile(func=None, *, description=None, speedscope_file=None):
        if func is None:
            return partial(profile, description=description, speedscope_file=speedscope_file)
        _logger.error("profiling is only available from Odoo 15")
        return func


if sys.version_info >= (3, 7):
    tick = time.perf_counter_ns
    ms = lambda t: t // 1000000
else:
    tick = time.perf_counter if sys.version_info >= (3, 3) else time.time
    ms = lambda t: int(t * 1000)

if not os.isatty(logging.getLogger().handlers[0].stream.fileno()):

    def _colorize(t):
        return "%d" % (ms(t),)
else:

    def _colorize(t):
        t = ms(t)

        colors = [
            (30000, 9),  # high intensity red
            (10000, 1),  # red
            (5000, 5),  # magenta
            (1000, 3),  # yellow
        ]
        for limit, color in colors:
            if t >= limit:
                return "\033[38;5;%dm%s\033[0m" % (color, t)
        return str(t)


class _Tracer(object):
    # code adapted from the Odoo 14 profiler.
    def __init__(self):
        self._ff = None
        self._events = []
        self._lines = None
        self._firstline = 0
        self._calls = []

    def trace(self, frame, event, arg):
        if self._ff is None:
            self._ff = frame
            self._lines, self._firstline = inspect.getsourcelines(frame)
        if self._ff is not frame or frame.f_code.co_name == "<genexpr>":
            return None
        self._calls.append(
            {
                "l": frame.f_lineno,
                "t": tick(),
            }
        )
        return self.trace

    def report(self):
        report = defaultdict(lambda: defaultdict(int))
        lc = len(self._calls)
        for k, call in enumerate(self._calls):
            if k == lc - 1:
                # ignore last call
                continue
            next_call = self._calls[k + 1]
            report[call["l"]]["nb"] += 1
            report[call["l"]]["d"] += next_call["t"] - call["t"]

        logs = ["calls  ms          code\n------------------------"]
        for no, line in enumerate(self._lines):
            data = report[no + self._firstline]
            d = _colorize(data["d"]) if data else ""
            logs.append("%-7s%-12s%s" % (data.get("nb", ""), d, line[:-1]))

        logs.append("\nTotal time: %sms" % (_colorize(sum(d["d"] for d in report.values())),))
        return "\n".join(logs)


def trace(func):
    """Decorate a function to trace its execution time."""

    @wraps(func)
    def decorator(*args, **kwargs):
        tracer = _Tracer()
        try:
            sys.settrace(tracer.trace)
            ret = func(*args, **kwargs)
        finally:
            sys.settrace(None)

        _logger.log(NEARLYWARN, "tracing function %s:\n%s", func.__name__, tracer.report())

        return ret

    return decorator
