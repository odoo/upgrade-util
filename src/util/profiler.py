import logging
import os
from functools import partial, wraps
from inspect import getsourcefile
from pathlib import Path

from .const import ENVIRON
from .misc import version_gte

try:
    from odoo.tools.profiler import Profiler, make_session
except ImportError:
    if version_gte("15.0"):
        raise

__all__ = ["profile"]

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
