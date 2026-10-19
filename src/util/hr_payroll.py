import logging

from .fields import remove_field
from .misc import version_between
from .pg import SQLStr, _existing_columns, format_query
from .records import delete_unused, refs

__all__ = ["remove_salary_rule", "remove_salary_rules"]

_logger = logging.getLogger(__name__)


def _neutralize_rules(cr, *xmlids):
    rids = refs(cr, xmlids, strict=True)
    if not rids:
        return

    gone_xids = delete_unused(cr, *xmlids, keep_xmlids=False)
    to_neuter = [r for x, r in rids.items() if x not in gone_xids]
    if not to_neuter:
        return

    existing = _existing_columns(cr, [("hr_salary_rule", "active"), ("hr_salary_rule", "struct_id")])
    active = ", active = false" if ("hr_salary_rule", "active") in existing else ""
    query = format_query(
        cr,
        """
        UPDATE hr_salary_rule
           SET condition_select = 'python',
               condition_python = 'result = False',
               amount_select = 'fix',
               amount_fix = 0.0
               {}
         WHERE id = ANY(%s)
        """,
        SQLStr(active),
    )

    cr.execute(query, [to_neuter])
    if ("hr_salary_rule", "struct_id") not in existing:
        # unlink from struct m2m
        cr.execute("DELETE FROM hr_payroll_structure_hr_salary_rule_rel WHERE hr_salary_rule_id = ANY(%s)", [to_neuter])

    for xmlid, rid in rids.items():
        if rid in to_neuter:
            _logger.info("Salary rule %r has been neutralized and deactivated", xmlid)


def _remove_salary_rules(cr, *xmlids):
    rids = {v: k for k, v in refs(cr, xmlids, strict=True).items()}
    cr.execute(
        r"""
        SELECT r.id, f.name
          FROM ir_model_fields f,
               hr_salary_rule r
          JOIN hr_payroll_structure s
            ON r.struct_id = s.id
     LEFT JOIN res_country c
            ON s.country_id = c.id
         WHERE r.id = ANY(%s)
           AND f.model = 'hr.payroll.report'
           AND f.name = regexp_replace(
                            concat_ws(
                                '_',
                                'x_l10n',
                                COALESCE(lower(c.code), 'xx'),
                                lower(r.code)
                            ),
                            '[\.\- ]',
                            '_'
                        )
        """,
        [list(rids)],
    )
    for rid, fname in cr.fetchall():
        _logger.info(
            "Removing field %r from model 'hr.payroll.report' since salary rule %r is being removed",
            fname,
            rids[rid],
        )
        remove_field(cr, "hr.payroll.report", fname)
    _neutralize_rules(cr, *xmlids)


if not version_between("16.0", "saas~18.4"):

    def remove_salary_rules(cr, *xmlid):
        _neutralize_rules(cr, *xmlid)
else:

    def remove_salary_rules(cr, *xmlid):
        _remove_salary_rules(cr, *xmlid)


def remove_salary_rule(cr, xmlid):
    # XXX deprecate function?
    remove_salary_rules(cr, xmlid)
