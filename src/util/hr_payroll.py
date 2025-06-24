import logging

from .fields import remove_field
from .misc import version_between
from .records import delete_unused, ref

_logger = logging.getLogger(__name__)


def _remove_salary_rule(cr, xmlid):
    rid = ref(cr, xmlid)
    cr.execute(
        r"""
        SELECT f.name
          FROM ir_model_fields f,
               hr_salary_rule r
          JOIN hr_payroll_structure s
            ON r.struct_id = s.id
     LEFT JOIN res_country c
            ON s.country_id = c.id
         WHERE r.id = %s
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
        [rid],
    )
    for (fname,) in cr.fetchall():
        _logger.info(
            "Removing field %r from model 'hr.payroll.report' since salary rule %r is being removed",
            fname,
            xmlid,
        )
        remove_field(cr, "hr.payroll.report", fname)
    delete_unused(cr, xmlid)


if not version_between("16.0", "saas~18.4"):

    def remove_salary_rule(cr, xmlid):
        delete_unused(cr, xmlid)
else:
    remove_salary_rule = _remove_salary_rule
