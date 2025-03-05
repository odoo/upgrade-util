# -*- coding: utf-8 -*-
"""Utility functions for interacting with PostgreSQL."""

import collections
import logging
import os
import re
import threading
import time
import uuid
import warnings
from contextlib import contextmanager
from functools import partial, reduce
from multiprocessing import cpu_count

try:
    from concurrent.futures import ThreadPoolExecutor  # noqa: I001
    import concurrent
except ImportError:
    ThreadPoolExecutor = None

try:
    from collections import UserList
except ImportError:
    from UserList import UserList

try:  # noqa: SIM105
    range = xrange  # noqa: A001
except NameError:
    pass

import psycopg2
from psycopg2 import errorcodes, sql

try:
    from odoo.modules import module as odoo_module
    from odoo.sql_db import db_connect
except ImportError:
    from openerp.sql_db import db_connect

    odoo_module = None

from .exceptions import MigrationError, SleepyDeveloperError
from .helpers import _validate_table, model_of_table
from .misc import log_progress

_logger = logging.getLogger(__name__)

ON_DELETE_ACTIONS = frozenset(("SET NULL", "CASCADE", "RESTRICT", "NO ACTION", "SET DEFAULT"))
MAX_BUCKETS = int(os.getenv("MAX_BUCKETS", "150000"))


class PGRegexp(str):
    """
    Wrapper for semantic meaning of parameters: this string is a Postgres regular expression.

    See :func:`~odoo.upgrade.util.records.replace_in_all_jsonb_values`
    """


class SQLStr(str):
    """
    Wrapper for semantic meaning of parameters: this string is a valid SQL snippet.

    See :func:`~odoo.upgrade.util.pg.format_query`
    """


def get_max_workers():
    force_max_worker = os.getenv("MAX_WORKER")
    if force_max_worker:
        if not force_max_worker.isdigit():
            raise MigrationError("wrong parameter: MAX_WORKER should be an integer")
        return int(force_max_worker)
    return min(8, cpu_count())


@contextmanager
def savepoint(cr):
    # NOTE: the `savepoint` method on Cursor only appear in `saas-3`, which mean this function
    #       can't be called when upgrading to saas~1 or saas~2.
    #       I take the bet it won't be problematic...
    with cr.savepoint():
        yield


def _parallel_execute_serial(cr, queries, logger=_logger):
    cnt = 0
    for query in log_progress(queries, logger, qualifier="queries", size=len(queries)):
        cr.execute(query)
        cnt += cr.rowcount
    return cnt


if ThreadPoolExecutor is not None:

    def _parallel_execute_threaded(cr, queries, logger=_logger):
        if not queries:
            return None

        if len(queries) == 1:
            # No need to spawn other threads
            cr.execute(queries[0])
            return cr.rowcount

        max_workers = min(get_max_workers(), len(queries))
        cursor = db_connect(cr.dbname).cursor

        def execute(query):
            with cursor() as tcr:
                tcr.execute(query)
                cnt = tcr.rowcount
                tcr.commit()
                return cnt

        cr.commit()

        CONCURRENCY_ERRORCODES = {
            errorcodes.DEADLOCK_DETECTED,
            errorcodes.SERIALIZATION_FAILURE,
        }
        failed_queries = []
        tot_cnt = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_queries = {executor.submit(execute, q): q for q in queries}
            for future in log_progress(
                concurrent.futures.as_completed(future_queries),
                logger,
                qualifier="queries",
                size=len(queries),
                estimate=False,
                log_hundred_percent=True,
            ):
                try:
                    tot_cnt += future.result() or 0
                except psycopg2.OperationalError as exc:
                    if exc.pgcode not in CONCURRENCY_ERRORCODES:
                        raise

                    # to be retried without concurrency
                    failed_queries.append(future_queries[future])

        if failed_queries:
            logger.warning("Serialize queries that failed due to concurrency issues")
            tot_cnt += _parallel_execute_serial(cr, failed_queries, logger=logger)
            cr.commit()

        return tot_cnt

else:
    _parallel_execute_threaded = _parallel_execute_serial


def parallel_execute(cr, queries, logger=_logger):
    """
    Execute queries in parallel.

    .. example::
       .. code-block:: python

          util.parallel_execute(cr, [util.format_query(cr, "REINDEX TABLE {}", t) for t in tables])

    .. tip::
       If looking to speedup a single query, see :func:`~odoo.upgrade.util.pg.explode_execute`.

    :param list(str) queries: list of queries to execute concurrently
    :param `~logging.Logger` logger: logger used to report the progress
    :return: the sum of `cr.rowcount` for each query run
    :rtype: int

    .. warning::
       - Due to the nature of `cr.rowcount`, the return value of this function may represent an
         underestimate of the real number of affected records. For instance, when some records
         are deleted/updated as a result of an `ondelete` clause, they won't be taken into account.

       - As a side effect, the cursor will be committed.

    .. note::
       If a concurrency issue occurs, the *failing* queries will be retried sequentially.
    """
    parallel_execute_impl = (
        _parallel_execute_serial
        if getattr(threading.current_thread(), "testing", False)
        or (odoo_module is not None and getattr(odoo_module, "current_test", False))
        else _parallel_execute_threaded
    )
    return parallel_execute_impl(cr, queries, logger=_logger)


def format_query(cr, query, *args, **kwargs):
    """
    Safely format a query.

    The `str` arguments to this function are assumed to be SQL identifiers. They are
    wrapped in double quotes before being expanded using :meth:`str.format`. Any other
    `psycopg2.sql.Composable <https://www.psycopg.org/docs/sql.html#psycopg2.sql.Composable>`_
    are also allowed. This includes :class:`~odoo.upgrade.util.pg.ColumnList`, see also
    :func:`~odoo.upgrade.util.pg.get_columns`

    .. example::
       .. code-block:: python

          >>> util.format_query(cr, "SELECT {0} FROM {table}", "id", table="res_users")
          SELECT "id" FROM "res_users"

    :param str query: query to format, can use brackets `{}` as in :func:`str.format`
    """

    def wrap(arg):
        if isinstance(arg, sql.Composable):
            return arg
        elif isinstance(arg, SQLStr):
            return sql.SQL(arg)
        else:
            return sql.Identifier(arg)

    args = tuple(wrap(a) for a in args)
    kwargs = {k: wrap(v) for k, v in kwargs.items()}
    return SQLStr(sql.SQL(query).format(*args, **kwargs).as_string(cr._cnx))


def explode_query(cr, query, alias=None, num_buckets=8, prefix=None):
    """
    Explode a query to multiple queries that can be executed in parallel.

    Use modulo strategy to separate queries in buckets

    :meta private: exclude from online docs
    """
    warnings.warn(
        "`explode_query` has been deprecated in favor of `explode_query_range`. Consider also `explode_execute`.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    if prefix is not None:
        if alias is not None:
            raise ValueError("Cannot use both `alias` and deprecated `prefix` arguments.")
        _logger.getChild("explode_query").warning(
            "The `prefix` argument is deprecated. Use the `alias` argument instead."
        )
    elif alias is not None:
        prefix = alias + "."
    else:
        prefix = ""

    if "{parallel_filter}" not in query:
        if re.search(r"\bOR\b", query, re.I):
            _logger.getChild("explode_query").warning(
                "`OR` found in the query. Please explicitly include the `{parallel_filter}` placeholder in the query."
            )
        sep_kw = " AND " if re.search(r"\sWHERE\s", query, re.M | re.I) else " WHERE "
        query += sep_kw + "{parallel_filter}"

    num_buckets = int(num_buckets)
    if num_buckets < 1:
        raise ValueError("num_buckets should be greater than zero")
    parallel_filter = "mod(abs({prefix}id), %s) = %s".format(prefix=prefix)
    query = query.replace("%", "%%").format(parallel_filter=parallel_filter)
    return [cr.mogrify(query, [num_buckets, index]).decode() for index in range(num_buckets)]


def explode_query_range(cr, query, table, alias=None, bucket_size=10000, prefix=None):
    """
    Explode a query to multiple queries that can be executed in parallel.

    Use between strategy to separate queries in buckets

    :meta private: exclude from online docs
    """
    if prefix is not None:
        if alias is not None:
            raise ValueError("Cannot use both `alias` and deprecated `prefix` arguments.")
        _logger.getChild("explode_query_range").warning(
            "The `prefix` argument is deprecated. Use the `alias` argument instead."
        )
        alias = prefix.rstrip(".")

    alias = alias or table

    if "{parallel_filter}" not in query:
        if re.search(r"\bOR\b", query, re.I):
            _logger.getChild("explode_query_range").warning(
                "`OR` found in the query. Please explicitly include the `{parallel_filter}` placeholder in the query."
            )
        sep_kw = " AND " if re.search(r"\sWHERE\s", query, re.M | re.I) else " WHERE "
        query += sep_kw + "{parallel_filter}"

    cr.execute(format_query(cr, "SELECT min(id), max(id) FROM {}", table))
    min_id, max_id = cr.fetchone()
    if min_id is None:
        return []  # empty table
    count = (max_id + 1 - min_id) // bucket_size
    if count > MAX_BUCKETS:
        _logger.getChild("explode_query_range").warning(
            "High number of queries generated (%s); switching to a precise bucketing strategy", count
        )
        cr.execute(
            format_query(
                cr,
                """
                WITH t AS (
                    SELECT id,
                           mod(row_number() OVER(ORDER BY id) - 1, %s) AS g
                      FROM {table}
                     ORDER BY id
                ) SELECT array_agg(id ORDER BY id) FILTER (WHERE g=0),
                         min(id),
                         max(id)
                    FROM t
                """,
                table=table,
            ),
            [bucket_size],
        )
        ids, min_id, max_id = cr.fetchone()
    else:
        ids = list(range(min_id, max_id + 1, bucket_size))

    assert min_id == ids[0] and max_id + 1 != ids[-1]  # sanity checks
    ids.append(max_id + 1)  # ensure last bucket covers whole range
    # `ids` holds a list of values marking the interval boundaries for all buckets

    if (max_id - min_id + 1) <= 1.1 * bucket_size or (len(ids) == 3 and ids[2] - ids[1] <= 0.1 * bucket_size):
        # If we return one query `parallel_execute` skip spawning new threads. Thus we return only one query if we have
        # only two buckets and the second would have at most 10% of bucket_size records.
        # Still, since the query may only be valid if there is no split, we force the usage of `prefix` in the query to
        # validate its correctness and avoid scripts that pass the CI but fail in production.
        parallel_filter = "{alias}.id IS NOT NULL".format(alias=alias)
        return [query.format(parallel_filter=parallel_filter)]

    parallel_filter = "{alias}.id BETWEEN %(lower-bound)s AND %(upper-bound)s".format(alias=alias)
    query = query.replace("%", "%%").format(parallel_filter=parallel_filter)
    return [
        cr.mogrify(query, {"lower-bound": ids[i], "upper-bound": ids[i + 1] - 1}).decode() for i in range(len(ids) - 1)
    ]


def explode_execute(cr, query, table, alias=None, bucket_size=10000, logger=_logger):
    """
    Execute a query in parallel.

    The query is split by buckets of ids, then processed in parallel by workers. If the
    query does not include the special `{parallel_filter}` value, it is added to the last
    `WHERE` clause, possibly also adding it if none found. When the query already has the
    filter nothing is done. The filter always expands to the splitting strategy. The split
    is done into buckets where no more than `bucket_size` IDs are updated on each
    individual query.

    .. example::
       .. code-block:: python

          util.explode_execute(
              cr,
              '''
              UPDATE res_users u
                 SET active = False
               WHERE (u.login LIKE 'dummy' OR u.login = 'bob')
                 AND {parallel_filter}
              ''',
              table="res_users"
              alias="u",
          )

    :param str query: the query to execute.
    :param str table: name of the *main* table of the query, used to split the processing
    :param str alias: alias used for the main table in the query
    :param int bucket_size: size of the buckets of ids to split the processing
    :param logger: logger used to report the progress
    :type logger: :class:`logging.Logger`
    :return: the sum of `cr.rowcount` for each query run
    :rtype: int

    .. warning::
       It's up to the caller to ensure the queries do not update the same records in
       different buckets. It is advised to never use this function for `DELETE` queries on
       tables with self references due to the potential `ON DELETE` effects.
       For more details see :func:`~odoo.upgrade.util.pg.parallel_execute`.
    """
    return parallel_execute(
        cr,
        explode_query_range(cr, query, table, alias=alias, bucket_size=bucket_size),
        logger=logger,
    )


def pg_array_uniq(a, drop_null=False):
    dn = "WHERE x IS NOT NULL" if drop_null else ""
    return SQLStr("ARRAY(SELECT x FROM unnest({0}) x {1} GROUP BY x)".format(a, dn))


def pg_replace(s, replacements):
    q = lambda s: psycopg2.extensions.QuotedString(s).getquoted().decode("utf-8")
    return SQLStr(reduce(lambda s, r: "replace({}, {}, {})".format(s, q(r[0]), q(r[1])), replacements, s))


def pg_html_escape(s, quote=True):
    """
    Generate the SQL expression to HTML escape a string.

    SQL version of `html.escape`.

    :meta private: exclude from online docs
    """
    replacements = [
        ("&", "&amp;"),  # Must be done first!
        ("<", "&lt;"),
        (">", "&gt;"),
    ]
    if quote:
        replacements += [
            ('"', "&quot;"),
            ("'", "&#x27;"),
        ]
    return pg_replace(s, replacements)


def pg_text2html(s, wrap="p"):
    return SQLStr(
        r"""
        CASE WHEN TRIM(COALESCE({src}, '')) ~ '^<.+</\w+>$' THEN {src}
             ELSE CONCAT(
                '{opening_tag}',
                replace(
                    replace(REGEXP_REPLACE({esc},
                                           -- regex from https://blog.codinghorror.com/the-problem-with-urls/
                                           -- double the %% to allow this code chunk to be used in parameterized queries
                                           'https?://[-A-Za-z0-9+&@#/%%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%%=~_()|]',
                                           '<a href="\&" target="_blank" rel="noreferrer noopener">\&</a>',
                                           'g'),
                            E'\\n',
                            '<br>'),
                    E'\\t',
                    '&Tab;'),
                '{closing_tag}')
         END
        """.format(
            opening_tag="<{}>".format(wrap) if wrap else "",
            closing_tag="</{}>".format(wrap) if wrap else "",
            src=s,
            esc=pg_html_escape(s, quote=False),
        )
    )


def get_value_or_en_translation(cr, table, column):
    fmt = "{}->>'en_US'" if column_type(cr, table, column) == "jsonb" else "{}"
    return format_query(cr, fmt, column)


def _column_info(cr, table, column):
    _validate_table(table)
    cr.execute(
        """
        SELECT COALESCE(bt.typname, t.typname) AS udt_name,
               NOT (a.attnotnull OR t.typtype = 'd' AND t.typnotnull) AS is_nullable,
               (   c.relkind IN ('r','p','v','f')
               AND pg_column_is_updatable(c.oid::regclass, a.attnum, false)
               ) AS is_updatable
          FROM pg_attribute a
          JOIN pg_class c
            ON a.attrelid = c.oid
          JOIN pg_type t
            ON a.atttypid = t.oid
     LEFT JOIN pg_type bt
            ON t.typtype = 'd'
           AND t.typbasetype = bt.oid
         WHERE c.relname = %s
           AND a.attname = %s
        """,
        [table, column],
    )
    return cr.fetchone()


def column_exists(cr, table, column):
    """
    Return whether a column exists.

    :param str table: table to check
    :param str column: column to check
    :rtype: bool
    """
    return _column_info(cr, table, column) is not None


def column_type(cr, table, column):
    """
    Return the type of a column, if it exists.

    :param str table: table to check
    :param str column: column to check
    :rtype: SQL type of the column
    """
    nfo = _column_info(cr, table, column)
    return nfo[0] if nfo else None


def column_nullable(cr, table, column):
    nfo = _column_info(cr, table, column)
    return nfo and nfo[1]


def column_updatable(cr, table, column):
    nfo = _column_info(cr, table, column)
    return nfo and nfo[2]


def create_column(cr, table, column, definition, **kwargs):
    """
    Create a column.

    This function will create the column only if it *doesn't* exist. It will log an error
    if the existing column has different type.  If `fk_table` is set, it will ensure the
    foreign key is setup, updating if necessary, with the right `on_delete_action` if any
    is set.

    :param str table: table of the new column
    :param str column: name of the new column
    :param str definition: column type of the new column
    :param bool default: default value to set on the new column
    :param bool fk_table: if the new column if a foreign key, name of the foreign table
    :param str on_delete_action: `ON DELETE` clause, default `NO ACTION`, only valid if
                                  the column is a foreign key.
    :return: whether the column was created
    :rtype: bool
    """
    # Manual PEP 3102
    no_def = object()
    default = kwargs.pop("default", no_def)
    fk_table = kwargs.pop("fk_table", no_def)
    on_delete_action = kwargs.pop("on_delete_action", no_def)
    if kwargs:
        raise TypeError("create_column() got an unexpected keyword argument %r" % kwargs.popitem()[0])

    fk = ""
    if fk_table is not no_def:
        if on_delete_action is no_def:
            on_delete_action = "NO ACTION"
        elif on_delete_action not in ON_DELETE_ACTIONS:
            raise ValueError("unexpected value for the `on_delete_action` argument: %r" % (on_delete_action,))
        fk = (
            sql.SQL("REFERENCES {}(id) ON DELETE {}")
            .format(sql.Identifier(fk_table), sql.SQL(on_delete_action))
            .as_string(cr._cnx)
        )
    elif on_delete_action is not no_def:
        raise ValueError("`on_delete_action` argument can only be used if `fk_table` argument is set.")

    aliases = {
        "boolean": "bool",
        "smallint": "int2",
        "integer": "int4",
        "bigint": "int8",
        "real": "float4",
        "double precision": "float8",
        "character varying": "varchar",
        "timestamp with time zone": "timestamptz",
        "timestamp without time zone": "timestamp",
    }
    definition = aliases.get(definition.lower(), definition)

    if definition == "bool" and default is no_def:
        default = False

    curtype = column_type(cr, table, column)
    if curtype:
        if curtype != definition:
            _logger.error("%s.%s already exists but is %r instead of %r", table, column, curtype, definition)
        if fk_table is not no_def:
            create_fk(cr, table, column, fk_table, on_delete_action)
        if default is not no_def:
            query = 'UPDATE "{0}" SET "{1}" = %s WHERE "{1}" IS NULL'.format(table, column)
            query = cr.mogrify(query, [default]).decode()
            parallel_execute(cr, explode_query_range(cr, query, table=table))
        return False

    create_query = """ALTER TABLE "%s" ADD COLUMN "%s" %s %s""" % (table, column, definition, fk)
    if default is no_def:
        cr.execute(create_query)
    else:
        cr.execute(create_query + " DEFAULT %s", [default])
        cr.execute("""ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT""" % (table, column))
    return True


def create_fk(cr, table, column, fk_table, on_delete_action="NO ACTION"):
    assert on_delete_action in ON_DELETE_ACTIONS
    current_target = target_of(cr, table, column)
    if current_target:
        if current_target[:2] == (fk_table, "id"):
            # assume the `on_delete_action` is correct
            return
        cr.execute(
            sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                sql.Identifier(table), sql.Identifier(current_target[2])
            )
        )

    query = sql.SQL("ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}(id) ON DELETE {}").format(
        sql.Identifier(table), sql.Identifier(column), sql.Identifier(fk_table), sql.SQL(on_delete_action)
    )
    cr.execute(query)


def remove_column(cr, table, column, cascade=False):
    if column_exists(cr, table, column):
        drop_depending_views(cr, table, column)
        drop_cascade = " CASCADE" if cascade else ""
        cr.execute('ALTER TABLE "{0}" DROP COLUMN "{1}"{2}'.format(table, column, drop_cascade))


def alter_column_type(cr, table, column, type, using=None, where=None, logger=_logger):
    # remove the existing linked `ir_model_fields_selection` recods in case it was a selection field
    if table_exists(cr, "ir_model_fields_selection"):
        cr.execute(
            """
            DELETE FROM ir_model_fields_selection s
                  USING ir_model_fields f
                  WHERE f.id = s.field_id
                    AND f.model = %s
                    AND f.name = %s
            """,
            [model_of_table(cr, table), column],
        )

    drop_depending_views(cr, table, column)

    if not using:
        # if there is a high number of NULL entries, it will be faster to just ignore those
        cr.execute(format_query(cr, "ANALYZE {}({})", table, column))
        cr.execute(
            "SELECT null_frac FROM pg_stats WHERE schemaname = current_schema() AND tablename = %s AND attname = %s",
            [table, column],
        )
        [null_frac] = cr.fetchone() or (0.0,)
        if null_frac <= 0.70:
            # Simple case. Use general SQL syntax
            cr.execute(format_query(cr, "ALTER TABLE {} ALTER COLUMN {} TYPE {}", table, column, sql.SQL(type)))
            return

        using = "{{0}}::{}".format(type)

    # else, create a new column and parallel update queries.
    tmp_column = "_{}_upg".format(column)
    cr.execute(format_query(cr, "ALTER TABLE {} RENAME COLUMN {} TO {}", table, column, tmp_column))
    cr.execute(format_query(cr, "ALTER TABLE {} ADD COLUMN {} {}", table, column, sql.SQL(type)))

    using = format_query(cr, using, tmp_column)
    if where is None:
        where_clause = sql.SQL("")
        if column_type(cr, table, tmp_column) != "bool":
            where_clause = format_query(cr, "WHERE {} IS NOT NULL", tmp_column)
    else:
        where_clause = format_query(cr, "WHERE " + where, tmp_column)

    explode_execute(
        cr,
        format_query(cr, "UPDATE {} SET {} = {} {}", table, column, using, where_clause),
        table=table,
        logger=logger,
    )

    cr.execute(format_query(cr, "ALTER TABLE {} DROP COLUMN {} CASCADE", table, tmp_column))


def table_exists(cr, table):
    _validate_table(table)
    cr.execute(
        """
            SELECT 1
              FROM information_schema.tables
             WHERE table_name = %s
               AND table_type = 'BASE TABLE'
    """,
        [table],
    )
    return cr.fetchone() is not None


def sequence_exists(cr, sequence):
    cr.execute("SELECT 1 FROM information_schema.sequences WHERE sequence_name = %s", [sequence])
    return bool(cr.rowcount)


def view_exists(cr, view):
    _validate_table(view)
    cr.execute("SELECT 1 FROM information_schema.views WHERE table_name=%s", [view])
    return bool(cr.rowcount)


def remove_constraint(cr, table, name, cascade=False, warn=True):
    """
    Remove a table constraint.

    This function removes the constraint `name` from `table`. It also removes records from
    `ir_model_constraint` and its xml_ids. It logs not found constraints.

    .. note::

       If there is no constraint `name`, this function will attempt to remove
       ``{table}_{name}``, the latter is the default name the ORM uses for constraints
       created from `_sql_constraints`.

    :param str table: table from where to remove the constraint
    :param str name: name of the constraint to remove
    :param bool cascade: cascade the constraint removal
    :param bool warn: use warning level when logging not found constraints, otherwise use
                      info level
    :return: whether the constraint was removed
    :rtype: bool
    """
    _validate_table(table)
    log = _logger.warning if warn else _logger.info
    cascade = "CASCADE" if cascade else ""
    cr.execute('ALTER TABLE "{}" DROP CONSTRAINT IF EXISTS "{}" {}'.format(table, name, cascade))
    # Exceptionally remove Odoo records, even if we are in PG land on this file. This is somehow
    # valid because ir.model.constraint are ORM low-level objects that relate directly to table
    # constraints.
    cr.execute("DELETE FROM ir_model_constraint WHERE name = %s RETURNING id", [name])
    if cr.rowcount:
        ids = tuple(c for (c,) in cr.fetchall())
        cr.execute("DELETE FROM ir_model_data WHERE model = 'ir.model.constraint' AND res_id IN %s", [ids])
        return True
    if name.startswith(table + "_"):
        log("%r not found in ir_model_constraint, table=%r", name, table)
        return False
    log("%r not found in ir_model_constraint, attempting to remove with table %r prefix", name, table)
    return remove_constraint(cr, table, "{}_{}".format(table, name), cascade, warn)


def get_fk(cr, table, quote_ident=True):
    """
    Return the list of foreign keys pointing to `table`.

    returns a 4 tuple: (foreign_table, foreign_column, constraint_name, on_delete_action)

    Foreign key deletion action code:
        a = no action, r = restrict, c = cascade, n = set null, d = set default

    :meta private: exclude from online docs
    """
    _validate_table(table)
    funk = "quote_ident" if quote_ident else "concat"
    q = """SELECT {funk}(cl1.relname) as table,
                  {funk}(att1.attname) as column,
                  {funk}(con.conname) as conname,
                  con.confdeltype
             FROM pg_constraint as con, pg_class as cl1, pg_class as cl2,
                  pg_attribute as att1, pg_attribute as att2
            WHERE con.conrelid = cl1.oid
              AND con.confrelid = cl2.oid
              AND array_lower(con.conkey, 1) = 1
              AND con.conkey[1] = att1.attnum
              AND att1.attrelid = cl1.oid
              AND cl2.relname = %s
              AND att2.attname = 'id'
              AND array_lower(con.confkey, 1) = 1
              AND con.confkey[1] = att2.attnum
              AND att2.attrelid = cl2.oid
              AND con.contype = 'f'
    """.format(funk=funk)
    cr.execute(q, (table,))
    return cr.fetchall()


def target_of(cr, table, column):
    """
    Return the target of a foreign key.

    Returns None if there is not foreign key on given column.
    returns a 3-tuple (foreign_table, foreign_column, constraint_name)

    :meta private: exclude from online docs
    """
    cr.execute(
        """
        SELECT quote_ident(cl2.relname) as table,
               quote_ident(att2.attname) as column,
               quote_ident(con.conname) as conname
        FROM pg_constraint con
        JOIN pg_class cl1 ON (con.conrelid = cl1.oid)
        JOIN pg_attribute att1 ON (    array_lower(con.conkey, 1) = 1
                                    AND con.conkey[1] = att1.attnum
                                    AND att1.attrelid = cl1.oid)
        JOIN pg_class cl2 ON (con.confrelid = cl2.oid)
        JOIN pg_attribute att2 ON (    array_lower(con.confkey, 1) = 1
                                    AND con.confkey[1] = att2.attnum
                                    AND att2.attrelid = cl2.oid)
        WHERE cl1.relname = %s
        AND att1.attname = %s
        AND con.contype = 'f'
    """,
        [table, column],
    )
    return cr.fetchone()


class IndexInfo(collections.namedtuple("IndexInfo", "name on isunique isconstraint ispk")):
    """:meta private: exclude from online docs."""

    def drop(self, cr):
        if self.isconstraint:
            remove_constraint(cr, self.on, self.name, warn=False)
        else:
            cr.execute('DROP INDEX "%s"' % self.name)


def get_index_on(cr, table, *columns):
    """
    Return an optional IndexInfo records.

    NOTE: column order is respected

    Prefer primary keys over other indexes as the caller may want to verify PK existence before create one

    :meta private: exclude from online docs
    """
    _validate_table(table)

    if cr._cnx.server_version >= 90500:
        position = "array_position(x.indkey, x.unnest_indkey)"
    else:
        # array_position does not exists prior postgresql 9.5
        position = "strpos(array_to_string(x.indkey::int4[] || 0, ','), x.unnest_indkey::varchar || ',')"
    cr.execute(
        """
        SELECT name, on_, indisunique, indisconstraint, indisprimary
          FROM (SELECT i.relname as name,
                       c.relname as on_,
                       x.indisunique,
                       t.conname IS NOT NULL as indisconstraint,
                       x.indisprimary,
                       array_agg(a.attname::text order by {}) as attrs
                  FROM (select *, unnest(indkey) as unnest_indkey from pg_index) x
                  JOIN pg_class c ON c.oid = x.indrelid
                  JOIN pg_class i ON i.oid = x.indexrelid
                  JOIN pg_attribute a ON (a.attrelid=c.oid AND a.attnum=x.unnest_indkey)
             LEFT JOIN pg_constraint t ON (    t.connamespace = i.relnamespace
                                           AND t.conname = i.relname
                                           AND t.contype IN ('u'::"char", 'p'::"char")
                                           AND x.indisunique AND t.conrelid = c.oid)
                 WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char"]))
                   AND i.relkind = 'i'::"char"
                   AND c.relname = %s
              GROUP BY 1, 2, 3, 4, 5
          ) idx
         WHERE attrs = %s
      ORDER BY indisprimary DESC
         FETCH FIRST ROW ONLY
    """.format(position),
        [table, list(columns)],
    )
    return IndexInfo(*cr.fetchone()) if cr.rowcount else None


def _get_unique_indexes_with(cr, table, *columns):
    # (Cursor, str, *str) -> List[Tuple[str, List[str]]
    """
    Return all unique indexes on at least `columns`.

    return a list of tuple [index_name, list_of_column]
    """
    _validate_table(table)
    assert columns
    cr.execute(
        """
        SELECT name, attrs
          FROM (SELECT quote_ident(i.relname) as name,
                       array_agg(a.attname::text) as attrs
                  FROM (select *, unnest(indkey) as unnest_indkey from pg_index) x
                  JOIN pg_class c ON c.oid = x.indrelid
                  JOIN pg_class i ON i.oid = x.indexrelid
                  JOIN pg_attribute a ON (a.attrelid=c.oid AND a.attnum=x.unnest_indkey)
                 WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char"]))
                   AND i.relkind = 'i'::"char"
                   AND c.relname = %s
                   AND x.indisunique
              GROUP BY 1
          ) idx
         WHERE attrs @> %s
    """,
        [table, list(columns)],
    )
    return cr.fetchall()


def create_index(cr, name, table_name, *columns):
    # create index if table and columns exists and index don't already exists
    _validate_table(table_name)
    if not columns:
        raise SleepyDeveloperError("Missing `columns` for index")
    if all(column_exists(cr, table_name, c) for c in columns) and get_index_on(cr, table_name, *columns) is None:
        cr.execute(
            "CREATE INDEX {index_name} ON {table_name}({columns})".format(
                index_name=name, table_name=table_name, columns=",".join(columns)
            )
        )
        return True
    return False


@contextmanager
def temp_index(cr, table, *columns):
    # create a temporary index that will be removed at the end of the contextmanager
    assert columns
    _validate_table(table)
    name = "_".join(("_upg", table) + columns + (hex(int(time.time() * 1000))[2:],))
    create_index(cr, name, table, *columns)
    try:
        yield
    finally:
        cr.execute('DROP INDEX IF EXISTS "{}"'.format(name))


def get_depending_views(cr, table, column):
    # http://stackoverflow.com/a/11773226/75349
    _validate_table(table)
    q = """
        SELECT distinct quote_ident(dependee.relname), dependee.relkind
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
        JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
        JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
            AND pg_depend.refobjsubid = pg_attribute.attnum
        WHERE dependent.relname = %s
        AND pg_attribute.attnum > 0
        AND pg_attribute.attname = %s
        AND dependee.relkind in ('v', 'm')
    """
    cr.execute(q, [table, column])
    return cr.fetchall()


class ColumnList(UserList, sql.Composable):
    """
    Encapsulate a list of elements that represent column names.

    The resulting object can be rendered as string with leading/trailing comma or an alias.

    :param list(str) list_: list of column names
    :param list(str) quoted: list of quoted column names, it must correspond with the
                            `list_` parameter

    .. example::
        >>> columns = ColumnList(["id", "field_Yx"], ["id", '"field_Yx"'])

        >>> list(columns)
        ['id', '"field_Yx"']

        >>> columns.using(alias="t").as_string(cr._cnx)
        '"t"."id", "t"."field_Yx"'

        >>> columns.using(leading_comma=True).as_string(cr._cnx)
        ', "id", "field_Yx"'

        >>> util.format_query(cr, "SELECT {} t.name FROM table t", columns.using(alias="t", trailing_comma=True))
        'SELECT "t"."id", "t"."field_Yx", t.name FROM table t'

    .. note::
       This class is better used via :func:`~odoo.upgrade.util.pg.get_columns`
    """

    def __init__(self, list_=(), quoted=()):
        assert len(list_) == len(quoted)
        self._unquoted_columns = list(list_)
        super(ColumnList, self).__init__(quoted)
        self._leading_comma = False
        self._trailing_comma = False
        self._alias = None

    def using(self, leading_comma=False, trailing_comma=False, alias=None):
        """
        Set up parameters to render this list as a string.

        :param bool leading_comma: whether to render a leading comma in front of this list
        :param bool trailing_comma: whether to render a trailing comma
        :param str or None alias: alias of the table of the columns, no alias is added if
                                  set to `None`
        :return: a copy of the list with the parameters set
        :rtype: :class:`~odoo.upgrade.util.pg.ColumnList`
        """
        if self._leading_comma is leading_comma and self._trailing_comma is trailing_comma and self._alias == alias:
            return self
        new = ColumnList(self._unquoted_columns, self.data)
        new._leading_comma = leading_comma
        new._trailing_comma = trailing_comma
        new._alias = alias
        return new

    def as_string(self, context):
        """:meta private: exclude from online docs."""
        head = sql.SQL(", " if self._leading_comma and self else "")
        tail = sql.SQL("," if self._trailing_comma and self else "")

        if not self._alias:
            builder = sql.Identifier
        elif hasattr(sql.Identifier, "strings"):
            builder = partial(sql.Identifier, self._alias)
        else:
            # older psycopg2 versions, doesn't support passing multiple strings to the constructor
            builder = lambda elem: sql.SQL(".").join(sql.Identifier(self._alias) + sql.Identifier(elem))

        body = sql.SQL(", ").join(builder(elem) for elem in self._unquoted_columns)
        return sql.Composed([head, body, tail]).as_string(context)

    def iter_unquoted(self):
        """
        Iterate over the raw column names, non quoted.

        This is useful if the quoting is done outside this object. Also to get access to
        raw column names as in Postgres catalog.

        :return: an iterator for the raw column names
        """
        return iter(self._unquoted_columns)


def get_columns(cr, table, ignore=("id",)):
    """
    Return a list of columns in a table.

    :param str table: table name whose columns are retrieved
    :param list(str) ignore: list of column names to ignore in the returning list
    :return: a list of column names present in the table
    :rtype: :class:`~odoo.upgrade.util.pg.ColumnList`
    """
    _validate_table(table)

    cr.execute(
        """
            SELECT coalesce(array_agg(column_name::varchar ORDER BY column_name), ARRAY[]::varchar[]),
                   coalesce(array_agg(quote_ident(column_name) ORDER BY column_name), ARRAY[]::varchar[])
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = %s
               AND column_name != ALL(%s)
        """,
        [table, list(ignore)],
    )
    return ColumnList(*cr.fetchone())


def rename_table(cr, old_table, new_table, remove_constraints=True):
    """
    Rename a table.

    This function renames the table `old_table` into `new_table`, as well as its primary
    key (and sequence), indexes, and foreign keys.

    :param str old_table: name of the table to rename
    :param str new_table: new name of the table
    :para bool remove_constraints: whether to remove the table constraints
    """
    if not table_exists(cr, old_table):
        return

    if table_exists(cr, new_table):
        raise MigrationError(
            "Table {new_table} already exists. Can't rename table {old_table} to {new_table}.".format(**locals())
        )

    cr.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(sql.Identifier(old_table), sql.Identifier(new_table)))

    # rename pkey sequence
    cr.execute(
        sql.SQL("ALTER SEQUENCE IF EXISTS {} RENAME TO {}").format(
            sql.Identifier(old_table + "_id_seq"), sql.Identifier(new_table + "_id_seq")
        )
    )

    # track renamed table
    if table_exists(cr, "upgrade_test_data"):
        cr.execute(
            """
                UPDATE upgrade_test_data
                   SET value = replace(value::text, %s, %s)::jsonb
                 WHERE key = 'base.tests.test_moved0.TestMoved0'
            """,
            ['["{}",'.format(old_table), '["{}",'.format(new_table)],
        )

    # find and rename pkey, may still use an old name from a former migration
    cr.execute(
        """
        SELECT pgc.conname
          FROM pg_constraint pgc
          JOIN pg_index pgi
            ON pgi.indrelid = pgc.conrelid
           AND pgi.indexrelid = pgc.conindid
           AND pgi.indrelid = %s::regclass
           AND pgi.indisprimary
         WHERE pgc.confrelid = 0
        """,
        [new_table],
    )
    if cr.rowcount:
        (old_pkey,) = cr.fetchone()
        new_pkey = new_table + "_pkey"
        cr.execute(sql.SQL("ALTER INDEX {} RENAME TO {}").format(sql.Identifier(old_pkey), sql.Identifier(new_pkey)))
    else:
        new_pkey = ""  # no PK renamed

    # rename indexes (except the pkey's one and those not containing $old_table)
    cr.execute(
        """
        SELECT indexname
          FROM pg_indexes
         WHERE tablename = %s
           AND indexname != %s
           AND indexname LIKE %s
        """,
        [new_table, new_pkey, "%" + old_table.replace("_", r"\_") + r"\_%"],
    )
    for (idx,) in cr.fetchall():
        cr.execute(
            sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                sql.Identifier(idx),
                sql.Identifier(idx.replace(old_table, new_table)),
            )
        )

    if remove_constraints:
        # DELETE all constraints, except Primary/Foreign keys, they will be re-created by the ORM
        # NOTE: Custom constraints will instead be lost
        cr.execute(
            """
            SELECT constraint_name
              FROM information_schema.table_constraints
             WHERE table_name = %s
               AND constraint_name !~ '^[0-9_]+_not_null$'
               AND (  constraint_type NOT IN ('PRIMARY KEY', 'FOREIGN KEY')
                   -- For long table names the constraint name is shortened by PG to fit 63 chars, in such cases
                   -- it's better to drop the constraint, even if it's a foreign key, and let the ORM re-create it.
                   OR (constraint_type = 'FOREIGN KEY' AND constraint_name NOT LIKE %s)
                   )
            """,
            [new_table, old_table.replace("_", r"\_") + r"\_%"],
        )
        for (const,) in cr.fetchall():
            _logger.info("Dropping constraint %s on table %s", const, new_table)
            remove_constraint(cr, new_table, const, warn=False)

    # rename fkeys
    cr.execute(
        """
        SELECT constraint_name
          FROM information_schema.table_constraints
         WHERE table_name = %s
           AND constraint_type = 'FOREIGN KEY'
           AND constraint_name LIKE %s
        """,
        [new_table, old_table.replace("_", r"\_") + r"\_%"],
    )
    old_table_length = len(old_table)
    for (old_fkey,) in cr.fetchall():
        new_fkey = new_table + old_fkey[old_table_length:]
        _logger.info("Renaming FK %r to %r", old_fkey, new_fkey)
        cr.execute(
            sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
                sql.Identifier(new_table), sql.Identifier(old_fkey), sql.Identifier(new_fkey)
            )
        )


def find_new_table_column_name(cr, table, name):
    columns = get_columns(cr, table)
    i = 0
    while name in columns:
        i += 1
        name = "%s_%s" % (name, i)
    return name


def drop_depending_views(cr, table, column):
    """
    Drop views depending on a field to allow the ORM to resize it in-place.

    :meta private: exclude from online docs
    """
    for v, k in get_depending_views(cr, table, column):
        cr.execute("DROP {0} VIEW IF EXISTS {1} CASCADE".format("MATERIALIZED" if k == "m" else "", v))


def create_m2m(cr, m2m, fk1, fk2, col1=None, col2=None):
    if col1 is None:
        col1 = "%s_id" % fk1
    if col2 is None:
        col2 = "%s_id" % fk2

    if table_exists(cr, m2m):
        fixup_m2m(cr, m2m, fk1, fk2, col1, col2)
        return

    cr.execute(
        """
        CREATE TABLE {m2m}(
            {col1} integer NOT NULL REFERENCES {fk1}(id) ON DELETE CASCADE,
            {col2} integer NOT NULL REFERENCES {fk2}(id) ON DELETE CASCADE,
            PRIMARY KEY ({col1}, {col2})
        );
        CREATE INDEX ON {m2m}({col2}, {col1});
    """.format(**locals())
    )


def fixup_m2m(cr, m2m, fk1, fk2, col1=None, col2=None):
    if col1 is None:
        col1 = "%s_id" % fk1
    if col2 is None:
        col2 = "%s_id" % fk2

    if not table_exists(cr, m2m):
        return

    extra_columns = get_columns(cr, m2m, ignore=(col1, col2))
    if extra_columns:
        raise MigrationError("The m2m %r has extra columns: %s" % (m2m, ", ".join(extra_columns)))

    # cleanup
    fixup_m2m_cleanup(cr, m2m, col1, col2)
    cr.execute(
        """
        DELETE FROM "{m2m}" t
              WHERE NOT EXISTS (SELECT id FROM "{fk1}" WHERE id=t."{col1}")
                 OR NOT EXISTS (SELECT id FROM "{fk2}" WHERE id=t."{col2}")
    """.format(**locals())
    )
    deleted = cr.rowcount
    if deleted:
        _logger.debug("%(m2m)s: removed %(deleted)d invalid rows", locals())

    # set not null
    cr.execute('ALTER TABLE "{m2m}" ALTER COLUMN "{col1}" SET NOT NULL'.format(**locals()))
    cr.execute('ALTER TABLE "{m2m}" ALTER COLUMN "{col2}" SET NOT NULL'.format(**locals()))

    # create  missing or bad fk
    target = target_of(cr, m2m, col1)
    if target and target[:2] != (fk1, "id"):
        cr.execute('ALTER TABLE "{m2m}" DROP CONSTRAINT {con}'.format(m2m=m2m, con=target[2]))
        target = None
    if not target:
        _logger.debug("%(m2m)s: add FK %(col1)s -> %(fk1)s", locals())
        cr.execute(
            'ALTER TABLE "{m2m}" ADD FOREIGN KEY ("{col1}") REFERENCES "{fk1}" ON DELETE CASCADE'.format(**locals())
        )

    target = target_of(cr, m2m, col2)
    if target and target[:2] != (fk2, "id"):
        cr.execute('ALTER TABLE "{m2m}" DROP CONSTRAINT {con}'.format(m2m=m2m, con=target[2]))
        target = None
    if not target:
        _logger.debug("%(m2m)s: add FK %(col2)s -> %(fk2)s", locals())
        cr.execute(
            'ALTER TABLE "{m2m}" ADD FOREIGN KEY ("{col2}") REFERENCES "{fk2}" ON DELETE CASCADE'.format(**locals())
        )

    # create indexes
    fixup_m2m_indexes(cr, m2m, col1, col2)


def fixup_m2m_cleanup(cr, m2m, col1, col2):
    cr.execute(
        """
        DELETE FROM "{m2m}" t
              WHERE "{col1}" IS NULL
                 OR "{col2}" IS NULL
    """.format(**locals())
    )
    deleted = cr.rowcount
    if deleted:
        _logger.debug("%(m2m)s: removed %(deleted)d rows with NULL values", locals())

    # remove duplicated rows
    cr.execute(
        """
        DELETE FROM "{m2m}"
              WHERE ctid IN (SELECT ctid
                               FROM (SELECT ctid,
                                            ROW_NUMBER() OVER (PARTITION BY "{col1}", "{col2}"
                                                                   ORDER BY ctid) as rnum
                                       FROM "{m2m}") t
                              WHERE t.rnum > 1)
    """.format(**locals())
    )
    deleted = cr.rowcount
    if deleted:
        _logger.debug("%(m2m)s: removed %(deleted)d duplicated rows", locals())


def fixup_m2m_indexes(cr, m2m, col1, col2):
    idx1 = get_index_on(cr, m2m, col1, col2)
    idx2 = get_index_on(cr, m2m, col2, col1)

    if not idx1 and not idx2:
        # No index at all
        cr.execute('ALTER TABLE "%s" ADD PRIMARY KEY("%s", "%s")' % (m2m, col1, col2))
        cr.execute('CREATE INDEX ON "%s" ("%s", "%s")' % (m2m, col2, col1))
    elif idx1 and idx2:
        if not idx1.ispk and not idx2.ispk:
            # None is the PK. Create one
            idx1.drop(cr)
            cr.execute('ALTER TABLE "%s" ADD PRIMARY KEY("%s", "%s")' % (m2m, col1, col2))
    else:
        # only 1 index exist, create the second one
        # determine which one is missing
        fmt = (m2m, col2, col1) if idx1 else (m2m, col1, col2)
        existing = idx1 or idx2
        if existing.ispk:
            # the existing index is the PK, create a normal index
            cr.execute('CREATE INDEX ON "%s" ("%s", "%s")' % fmt)
        elif existing.isunique:
            # it's a unique index. Remove it and recreate a PK and a normal index
            existing.drop(cr)
            cr.execute('ALTER TABLE "%s" ADD PRIMARY KEY("%s", "%s")' % (m2m, col1, col2))
            cr.execute('CREATE INDEX ON "%s" ("%s", "%s")' % (m2m, col2, col1))
        else:
            # create a PK (unique index)
            cr.execute('ALTER TABLE "%s" ADD PRIMARY KEY("%s", "%s")' % fmt)

    # remove indexes on 1 column only
    idx = get_index_on(cr, m2m, col1)
    if idx:
        idx.drop(cr)
    idx = get_index_on(cr, m2m, col2)
    if idx:
        idx.drop(cr)


def fix_wrong_m2o(cr, table, column, target, value=None):
    cr.execute(
        """
        WITH wrongs_m2o AS (
            SELECT s.id
              FROM {table} s
         LEFT JOIN {target} t
                ON s.{column} = t.id
             WHERE s.{column} IS NOT NULL
               AND t.id IS NULL
        )
        UPDATE {table} s
           SET {column}=%s
          FROM wrongs_m2o w
         WHERE s.id = w.id
    """.format(table=table, column=column, target=target),
        [value],
    )


def get_m2m_tables(cr, table):
    """
    Return a list of m2m tables associated with `table`.

    We identify as m2m table all tables that have only two columns, both of which are FKs.
    This function will return m2m tables for which one Fk points to `table`

    :meta private: exclude from online docs
    """
    _validate_table(table)
    query = """
        WITH two_cols AS (
                SELECT t.oid
                  FROM pg_class t
                  JOIN pg_attribute a ON a.attrelid=t.oid
                 WHERE t.relkind='r' AND a.attnum>0
                 GROUP BY t.oid
                HAVING count(*)=2
               )
        SELECT DISTINCT t.relname
          FROM pg_class t
          JOIN two_cols tc ON t.oid=tc.oid
          JOIN pg_attribute a1 ON a1.attrelid=t.oid AND a1.attnum>0
          JOIN pg_constraint c1 ON c1.conrelid=t.oid AND c1.contype='f' AND a1.attnum=any(c1.conkey)
               AND array_length(c1.conkey, 1)=1
          JOIN pg_attribute a2 ON a2.attrelid=t.oid AND a2.attnum>0 AND a1.attnum!=a2.attnum
          JOIN pg_constraint c2 ON c2.conrelid=t.oid AND c2.contype='f' AND a2.attnum=any(c2.conkey)
               AND array_length(c1.conkey, 1)=1
          JOIN pg_class the_table ON c1.confrelid=the_table.oid
         WHERE the_table.relkind='r' AND the_table.relname=%s
    """

    cr.execute(query, [table])
    return [row[0] for row in cr.fetchall()]


class named_cursor(object):
    def __init__(self, cr, itersize=None):
        self._ncr = cr._cnx.cursor("upg_nc_" + uuid.uuid4().hex, withhold=True)
        if itersize:
            self._ncr.itersize = itersize

    def __dictrow(self, row):
        return {d.name: v for d, v in zip(self._ncr.description, row)}

    def dictfetchall(self):
        return list(map(self.__dictrow, self._ncr.fetchall()))

    def dictfetchmany(self, size):
        return list(map(self.__dictrow, self._ncr.fetchmany(size)))

    def dictfetchone(self):
        row = self._ncr.fetchone()
        return None if row is None else self.__dictrow(row)

    def iterdict(self):
        return map(self.__dictrow, self._ncr)

    def __iter__(self):
        return self._ncr.__iter__()

    def __enter__(self):
        self._ncr.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._ncr.__exit__(exc_type, exc_value, traceback)

    def __getattr__(self, name):
        return getattr(self._ncr, name)


def create_id_sequence(cr, table, set_as_default=True):
    if not table_exists(cr, table):
        raise MigrationError("The table `%s` doesn't exist, sequence can't be created." % table)

    # inheritance lookup
    cr.execute(
        """
        WITH RECURSIVE recursive_lookup AS (
            SELECT oid AS table_oid,
                   0 AS level
              FROM pg_class
             WHERE relname = %s
             UNION
                -- add parents with id column recursively
            SELECT i.inhparent AS table_oid,
                   recursive_lookup.level + 1 AS level
              FROM pg_inherits i
              JOIN recursive_lookup
                ON i.inhrelid = recursive_lookup.table_oid
              JOIN pg_attribute c
                ON i.inhparent = c.attrelid
             WHERE c.attname = 'id'
        ) SELECT t.relname
            FROM pg_class t
            JOIN recursive_lookup
              ON t.oid = recursive_lookup.table_oid
           ORDER BY level DESC LIMIT 1
        """,
        [table],
    )
    table = cr.fetchone()[0]  # will be different from param if inherited
    sequence = table + "_id_seq"
    sequence_sql, table_sql = sql.Identifier(sequence), sql.Identifier(table)

    if not sequence_exists(cr, sequence):
        cr.execute(
            sql.SQL("CREATE SEQUENCE {sequence} OWNED BY {table}.id").format(
                sequence=sequence_sql,
                table=table_sql,
            )
        )

    cr.execute(
        sql.SQL("SELECT setval('{sequence}', (SELECT COALESCE(max(id), 0) FROM {table}) + 1, false)").format(
            sequence=sequence_sql,
            table=table_sql,
        )
    )
    if set_as_default:
        cr.execute(
            sql.SQL("ALTER TABLE ONLY {table} ALTER COLUMN id SET DEFAULT nextval('{sequence}'::regclass)").format(
                sequence=sequence_sql,
                table=table_sql,
            )
        )
