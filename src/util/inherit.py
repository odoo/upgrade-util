# -*- coding: utf-8 -*-
import logging
import operator
import os

from ._inherit import inheritance_data
from .const import ENVIRON
from .misc import parse_version

_logger = logging.getLogger(__name__)


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
        cr.execute("SELECT state, latest_version FROM ir_module_module WHERE name='base'")
        state, version = cr.fetchone()
        if state != "to upgrade":
            major = ".".join(version.split(".")[:2])
            _logger.warning(
                "Assuming upgrading from Odoo %s. If it's not the case, specify the environment variable `ODOO_BASE_VERSION`.",
                major,
            )
        bv = ENVIRON["__base_version"] = parse_version(version)
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


def inherit_parents(cr, model, skip=(), interval="[)"):
    if skip == "*":
        return
    skip = set(skip)
    cmp_ = _version_comparator(cr, interval)
    for parent, inhs in inheritance_data.items():
        if parent in skip:
            continue
        for inh in inhs:
            if inh.model == model and cmp_(inh):
                yield parent
                skip.add(parent)
                for grand_parent in inherit_parents(cr, parent, skip=skip, interval=interval):
                    yield grand_parent
