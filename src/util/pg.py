# -*- coding: utf-8 -*-
import collections
import logging
import os
import re
import time
import uuid
from contextlib import contextmanager
from functools import reduce
from multiprocessing import cpu_count

try:
    from concurrent.futures import ThreadPoolExecutor
except ImportError:
    ThreadPoolExecutor = None

import psycopg2
from psycopg2 import sql

try:
    from odoo.sql_db import db_connect
except ImportError:
    from openerp.sql_db import db_connect

from .exceptions import MigrationError
from .helpers import _validate_table
from .misc import log_progress

_logger = logging.getLogger(__name__)


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


if ThreadPoolExecutor is None:

    def parallel_execute(cr, queries, logger=_logger):
        cnt = 0
        for query in log_progress(queries, logger, qualifier="queries", size=len(queries)):
            cr.execute(query)
            cnt += cr.rowcount
        return cnt

else:

    def parallel_execute(cr, queries, logger=_logger):
        """
        Execute queries in parallel
        Use a maximum of 8 workers (but not more than the number of CPUs)
        Side effect: the given cursor is commited.
        As example, on `**REDACTED**` (using 8 workers), the following gains are:
            +---------------------------------------------+-------------+-------------+
            | File                                        | Sequential  | Parallel    |
            +---------------------------------------------+-------------+-------------+
            | base/saas~12.5.1.3/pre-20-models.py         | ~8 minutes  | ~2 minutes  |
            | mail/saas~12.5.1.0/pre-migrate.py           | ~10 minutes | ~4 minutes  |
            | mass_mailing/saas~12.5.2.0/pre-10-models.py | ~40 minutes | ~18 minutes |
            +---------------------------------------------+-------------+-------------+
        """
        if not queries:
            return None

        if len(queries) == 1:
            # No need to spawn other threads
            cr.execute(queries[0])
            return cr.rowcount

        max_workers = min(get_max_workers(), len(queries))
        cursor = db_connect(cr.dbname).cursor

        def execute(query):
            with cursor() as cr:
                cr.execute(query)
                cnt = cr.rowcount
                cr.commit()
                return cnt

        cr.commit()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return sum(
                log_progress(
                    executor.map(execute, queries),
                    logger,
                    qualifier="queries",
                    size=len(queries),
                    estimate=False,
                    log_hundred_percent=True,
                )
            )


def explode_query(cr, query, alias=None, num_buckets=8, prefix=None):
    """
    Explode a query to multiple queries that can be executed in parallel

    Use modulo stategy to separate queries in buckets
    """
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
    Explode a query to multiple queries that can be executed in parallel

    Use between stategy to separate queries in buckets
    """

    if prefix is not None:
        if alias is not None:
            raise ValueError("Cannot use both `alias` and deprecated `prefix` arguments.")
        _logger.getChild("explode_query_range").warning(
            "The `prefix` argument is deprecated. Use the `alias` argument instead."
        )
        alias = prefix.rstrip(".")

    alias = alias or table

    cr.execute("SELECT min(id), max(id) FROM {}".format(table))
    min_id, max_id = cr.fetchone()
    if min_id is None:
        return []  # empty table

    if "{parallel_filter}" not in query:
        sep_kw = " AND " if re.search(r"\sWHERE\s", query, re.M | re.I) else " WHERE "
        query += sep_kw + "{parallel_filter}"

    if ((max_id - min_id + 1) * 0.9) <= bucket_size:
        # If there is less than `bucket_size` records (with a 10% tolerance), no need to explode the query.
        # Force usage of `prefix` in the query to validate it correctness.
        # If we don't the query may only be valid if there is no split. It avoid scripts to pass the CI but fail in production.
        parallel_filter = "{alias}.id IS NOT NULL".format(alias=alias)
        return [query.format(parallel_filter=parallel_filter)]

    parallel_filter = "{alias}.id BETWEEN %s AND %s".format(alias=alias)
    query = query.replace("%", "%%").format(parallel_filter=parallel_filter)
    return [
        cr.mogrify(query, [index, index + bucket_size - 1]).decode() for index in range(min_id, max_id, bucket_size)
    ]


def pg_array_uniq(a, drop_null=False):
    dn = "WHERE x IS NOT NULL" if drop_null else ""
    return "ARRAY(SELECT x FROM unnest({0}) x {1} GROUP BY x)".format(a, dn)


def pg_replace(s, replacements):
    q = lambda s: psycopg2.extensions.QuotedString(s).getquoted().decode("utf-8")  # noqa: E731
    return reduce(lambda s, r: "replace({}, {}, {})".format(s, q(r[0]), q(r[1])), replacements, s)


def pg_html_escape(s, quote=True):
    """sql version of html.escape"""
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
    return r"""
        CASE WHEN TRIM(COALESCE({src}, '')) ~ '^<.+</\w+>$' THEN {src}
             ELSE CONCAT(
                '{opening_tag}',
                replace(REGEXP_REPLACE({esc},
                                       -- regex from https://blog.codinghorror.com/the-problem-with-urls/
                                       -- double the %% to allow this code chunk to be used in parameterized queries
                                       'https?://[-A-Za-z0-9+&@#/%%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%%=~_()|]',
                                       '<a href="\&" target="_blank" rel="noreferrer noopener">\&</a>',
                                       'g'),
                        E'\n',
                        '<br>'),
                '{closing_tag}')
         END
    """.format(
        opening_tag="<{}>".format(wrap) if wrap else "",
        closing_tag="</{}>".format(wrap) if wrap else "",
        src=s,
        esc=pg_html_escape(s, quote=False),
    )


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
    return _column_info(cr, table, column) is not None


def column_type(cr, table, column):
    nfo = _column_info(cr, table, column)
    return nfo[0] if nfo else None


def column_nullable(cr, table, column):
    nfo = _column_info(cr, table, column)
    return nfo and nfo[1]


def column_updatable(cr, table, column):
    nfo = _column_info(cr, table, column)
    return nfo and nfo[2]


def create_column(cr, table, column, definition, **kwargs):
    # Manual PEP 3102
    no_def = object()
    default = kwargs.pop("default", no_def)
    if kwargs:
        raise TypeError("create_column() got an unexpected keyword argument %r" % kwargs.popitem()[0])
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
        if default is not no_def:
            query = 'UPDATE "{0}" SET "{1}" = %s WHERE "{1}" IS NULL'.format(table, column)
            query = cr.mogrify(query, [default]).decode()
            parallel_execute(cr, explode_query_range(cr, query, table=table))
        return False
    else:
        create_query = """ALTER TABLE "%s" ADD COLUMN "%s" %s""" % (table, column, definition)
        if default is no_def:
            cr.execute(create_query)
        else:
            cr.execute(create_query + " DEFAULT %s", [default])
            cr.execute("""ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT""" % (table, column))
        return True


def remove_column(cr, table, column, cascade=False):
    if column_exists(cr, table, column):
        drop_depending_views(cr, table, column)
        drop_cascade = " CASCADE" if cascade else ""
        cr.execute('ALTER TABLE "{0}" DROP COLUMN "{1}"{2}'.format(table, column, drop_cascade))


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


def remove_constraint(cr, table, name, cascade=False):
    _validate_table(table)
    cascade = "CASCADE" if cascade else ""
    cr.execute('ALTER TABLE "{}" DROP CONSTRAINT IF EXISTS "{}" {}'.format(table, name, cascade))
    # small exception: odoo specific action.
    # needs to be kept here to avoid resurive imports.
    # a solution would be to not do it now but adds an `end-` script that remove the invalid entries
    # from the `ir_model_constraint` table
    cr.execute("DELETE FROM ir_model_constraint WHERE name = %s RETURNING id", [name])
    if cr.rowcount:
        ids = tuple(c for c, in cr.fetchall())
        cr.execute("DELETE FROM ir_model_data WHERE model = 'ir.model.constraint' AND res_id IN %s", [ids])


def get_fk(cr, table, quote_ident=True):
    """return the list of foreign keys pointing to `table`

    returns a 4 tuple: (foreign_table, foreign_column, constraint_name, on_delete_action)

    Foreign key deletion action code:
        a = no action, r = restrict, c = cascade, n = set null, d = set default
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
    """.format(
        funk=funk
    )
    cr.execute(q, (table,))
    return cr.fetchall()


def target_of(cr, table, column):
    """
    Return the target of a foreign key.
    Returns None if there is not foreign key on given column.
    returns a 3-tuple (foreign_table, foreign_column, constraint_name)
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
    def drop(self, cr):
        if self.isconstraint:
            remove_constraint(cr, self.on, self.name)
        else:
            cr.execute('DROP INDEX "%s"' % self.name)


def get_index_on(cr, table, *columns):
    """
    return an optional IndexInfo recors
    NOTE: column order is respected

    Prefer primary keys over other indexes as the caller may want to verify PK existence before create one
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
    """.format(
            position
        ),
        [table, list(columns)],
    )
    return IndexInfo(*cr.fetchone()) if cr.rowcount else None


def _get_unique_indexes_with(cr, table, *columns):
    # (Cursor, str, *str) -> List[Tuple[str, List[str]]
    """
    Returns all unique indexes on at least `columms`
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
    if (
        columns
        and all(column_exists(cr, table_name, c) for c in columns)
        and get_index_on(cr, table_name, *columns) is None
    ):
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


def get_columns(cr, table, ignore=("id",)):
    """return the list of columns in table (minus ignored ones)"""
    _validate_table(table)

    cr.execute(
        """
            SELECT quote_ident(column_name)
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = %s
               AND column_name != ALL(%s)
        """,
        [table, list(ignore)],
    )
    return [c for c, in cr.fetchall()]


def find_new_table_column_name(cr, table, name):
    columns = get_columns(cr, table)
    i = 0
    while name in columns:
        i += 1
        name = "%s_%s" % (name, i)
    return name


def drop_depending_views(cr, table, column):
    """drop views depending on a field to allow the ORM to resize it in-place"""
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
    """.format(
            **locals()
        )
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
    """.format(
            **locals()
        )
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
    """.format(
            **locals()
        )
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
    """.format(
            **locals()
        )
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
            # create a PK (unqiue index)
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
    """.format(
            table=table, column=column, target=target
        ),
        [value],
    )


def get_m2m_tables(cr, table):
    """
    Returns a list of m2m tables associated with `table`.

    We identify as m2m table all tables that have only two columns, both of which are FKs.
    This function will return m2m tables for which one Fk points to `table`
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
        self._ncr = cr._cnx.cursor("upg_nc_" + uuid.uuid4().hex)
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
