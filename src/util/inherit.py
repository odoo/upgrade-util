# -*- coding: utf-8 -*-
import operator
import os

from ._inherit import inheritance_data
from .const import ENVIRON
from .misc import parse_version


def _get_base_version(cr):
    # base_version is normaly computed in `base/0.0.0/pre-base_version.py` (and symlinks)
    # However, if theses scripts are used to upgrade custom modules afterward (like the P.S. do),
    # as the `base` module not being updated, the *base_version* MUST be set as an environment variable.
    bv = ENVIRON.get("__base_version")
    if bv:
        return bv
    # trust env variable if set
    bv = os.getenv("ODOO_BASE_VERSION")
    if bv:
        bv = ENVIRON["__base_version"] = parse_version(bv)
    else:
        cr.execute("SELECT latest_version FROM ir_module_module WHERE name='base' AND state='to upgrade'")
        # Let it fail if called outside update of `base` module.
        bv = ENVIRON["__base_version"] = parse_version(cr.fetchone()[0])
    return bv


def _version_comparator(cr, interval):
    if interval not in {"[]", "()", "[)", "(]"}:
        raise ValueError("Invalid interval: %r" % (interval,))

    op_lower = operator.le if interval[0] == "[" else operator.lt
    op_upper = operator.le if interval[1] == "]" else operator.lt
    base_version = _get_base_version(cr)

    return lambda inh: op_lower(inh.born, base_version) and (inh.dead is None or op_upper(base_version, inh.dead))


def for_each_inherit(cr, model, skip=(), interval="[)"):
    if skip == "*":
        return
    cmp_ = _version_comparator(cr, interval)
    for inh in inheritance_data.get(model, []):
        if inh.model in skip:
            continue
        if cmp_(inh):
            yield inh
