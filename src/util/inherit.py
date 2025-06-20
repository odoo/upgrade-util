# -*- coding: utf-8 -*-
import logging
import operator
import os

from .const import ENVIRON, NEARLYWARN
from .misc import _cached, parse_version, version_gte

_logger = logging.getLogger(__name__)


if version_gte("saas~17.1"):
    from ._inherit import Inherit, frozendict

    @_cached
    def _get_inheritance_data(cr):
        base_version = _get_base_version(cr)[:2] + ("*final",)
        cr.execute(
            """
            SELECT p.model,
                   array_agg(m.model ORDER BY i.id),
                   array_agg(f.name ORDER BY i.id)
              FROM ir_model_inherit i
              JOIN ir_model m
                ON m.id = i.model_id
              JOIN ir_model p
                ON p.id = i.parent_id
         LEFT JOIN ir_model_fields f
                ON f.id = i.parent_field_id
          GROUP BY p.model
        """
        )
        return frozendict(
            {
                parent: [
                    Inherit(model=model, born=base_version, dead=None, via=via)
                    for model, via in zip(children, vias, strict=True)
                ]
                for parent, children, vias in cr.fetchall()
            }
        )

else:
    from ._inherit import inheritance_data

    def _get_inheritance_data(cr):
        return inheritance_data


def _get_base_version(cr):
    # base_version is normally computed in `base/0.0.0/pre-base_version.py` (and symlinks)
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
            _logger.log(
                NEARLYWARN,
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
    for inh in _get_inheritance_data(cr).get(model, []):
        if inh.model in skip:
            continue
        if cmp_(inh):
            yield inh


def direct_inherit_parents(cr, model, skip=(), interval="[)"):
    """Yield the *direct* inherits parents."""
    if skip == "*":
        return
    skip = set(skip)
    cmp_ = _version_comparator(cr, interval)
    for parent, inhs in _get_inheritance_data(cr).items():
        if parent in skip:
            continue
        for inh in inhs:
            if inh.model == model and cmp_(inh):
                yield parent, inh
                skip.add(parent)


def inherit_parents(cr, model, skip=(), interval="[)"):
    """Recursively yield all inherit parents model names."""
    if skip == "*":
        return
    skip = set(skip)
    for parent, _inh in direct_inherit_parents(cr, model, skip=skip, interval=interval):
        if parent in skip:
            continue
        yield parent
        skip.add(parent)
        for grand_parent in inherit_parents(cr, parent, skip=skip, interval=interval):
            yield grand_parent
            skip.add(grand_parent)
