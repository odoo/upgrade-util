# -*- coding: utf-8 -*-
from contextlib import contextmanager

from .modules import module_installed
from .orm import env
from .pg import get_columns


@contextmanager
def no_fiscal_lock(cr):
    env(cr)["res.company"].invalidate_cache()
    columns = [col for col in get_columns(cr, "res_company")[0] if col.endswith("_lock_date")]
    assert columns
    set_val = ", ".join("{} = NULL".format(col) for col in columns)
    returns = ", ".join("old.{}".format(col) for col in columns)
    cr.execute(
        """
            UPDATE res_company c
               SET {}
              FROM res_company old
             WHERE old.id = c.id
         RETURNING {}, old.id
        """.format(
            set_val, returns
        )
    )
    data = cr.fetchall()
    yield
    set_val = ", ".join("{} = %s".format(col) for col in columns)
    cr.executemany(
        """
            UPDATE res_company
               SET {}
             WHERE id = %s
        """.format(
            set_val
        ),
        data,
    )


@contextmanager
def skip_failing_python_taxes(env, skipped=None):
    if module_installed(env.cr, "account_tax_python"):
        origin_compute_amount = env.registry["account.tax"]._compute_amount

        def _compute_amount(self, *args, **kwargs):
            if self.amount_type != "code":
                return origin_compute_amount(self, *args, **kwargs)
            try:
                return origin_compute_amount(self, *args, **kwargs)
            except ValueError as e:
                if skipped is not None:
                    skipped[self.id] = (self.name, e.args[0])
                return 0

        env.registry["account.tax"]._compute_amount = _compute_amount
        yield
        env.registry["account.tax"]._compute_amount = origin_compute_amount
    else:
        yield
