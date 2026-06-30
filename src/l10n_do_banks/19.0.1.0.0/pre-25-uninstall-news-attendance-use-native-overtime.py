"""Move attendance-overtime computation to Odoo's native overtime engine while
keeping the Dominican payroll bridge.

Historically the Dominican localization computed attendance overtime by hand and
pushed it to payroll through a "news" document:

    l10n_do_hr_news_attendance          (compute overtime by hand -> create news)
    l10n_do_hr_payroll_news_attendance  (news amount -> salary attachment)

From Odoo 18/19 the overtime calculation (daytime / nightly / holiday + rates) and
its path to payroll are handled natively by:

    hr_attendance               (Overtime Rulesets)
    hr_work_entry_attendance    (overtime -> work entries)
    hr_payroll_attendance       (work entries -> payslip worked days)

The manual-calculation module ``l10n_do_hr_news_attendance`` is therefore
redundant and gets uninstalled. ``l10n_do_hr_payroll_news_attendance`` is **kept**
(and reworked) so it still carries the overtime into the Dominican payroll as the
overtime input (``HEL``): it now reads the *native* overtime worked-day lines
instead of the retired wizard.

This script:
  1. Makes sure every employee version points to an Overtime Ruleset so the native
     engine keeps generating their overtime once the manual module is gone.
  2. Uninstalls only the now-redundant manual-calculation module.
"""

import logging

from odoo.upgrade import util

_logger = logging.getLogger(__name__)

MODULES_TO_UNINSTALL = [
    "l10n_do_hr_news_attendance",
]

RULESET_MODEL = "hr.attendance.overtime.ruleset"
RULESET_NAME = "República Dominicana"


def _get_or_create_ruleset(env):
    """Return the ruleset to assign to employees, creating a placeholder if none
    exists. Legal rates must be reviewed by HR afterwards."""
    Ruleset = env[RULESET_MODEL]
    ruleset = Ruleset.search(
        ["|", ("name", "ilike", "Dominicana"), ("name", "ilike", "RD")], limit=1
    ) or Ruleset.search([], limit=1, order="id")
    if ruleset:
        return ruleset

    _logger.warning(
        "No Overtime Ruleset found. Creating a placeholder '%s'. HR must review its rules and legal rates.",
        RULESET_NAME,
    )
    ruleset = Ruleset.create({"name": RULESET_NAME})
    Rule = env["hr.attendance.overtime.rule"]
    vals = {
        "name": "Horas extra (revisar tarifa)",
        "ruleset_id": ruleset.id,
        "base_off": "timing",
        "timing_type": "work_days",
        "timing_start": 7.0,
        "timing_stop": 21.0,
        "paid": True,
        "amount_rate": 1.35,
    }
    # work_entry_type_id is required once hr_work_entry_attendance is installed
    if "work_entry_type_id" in Rule._fields:
        wet = env.ref("hr_work_entry.work_entry_type_overtime", raise_if_not_found=False)
        if wet:
            vals["work_entry_type_id"] = wet.id
    Rule.create(vals)
    return ruleset


def _migrate_employees_to_native_overtime(cr):
    """Assign an Overtime Ruleset to every version that lacks one, so the native
    engine generates overtime from attendances after the custom modules go away."""
    env = util.env(cr)
    if RULESET_MODEL not in env:
        _logger.warning(
            "%s not available (hr_attendance overtime engine missing); skipping employee migration.",
            RULESET_MODEL,
        )
        return
    if "ruleset_id" not in env["hr.version"]._fields:
        _logger.warning("hr.version has no ruleset_id; skipping employee migration.")
        return

    ruleset = _get_or_create_ruleset(env)
    versions = env["hr.version"].search([("ruleset_id", "=", False)])
    if not versions:
        _logger.info("All versions already have an Overtime Ruleset; nothing to migrate.")
        return
    versions.write({"ruleset_id": ruleset.id})
    _logger.info(
        "Assigned Overtime Ruleset '%s' to %d employee version(s).",
        ruleset.name,
        len(versions),
    )


PAYROLL_BRIDGE = "l10n_do_hr_payroll_news_attendance"


def _repoint_payroll_bridge_deps(cr):
    """Repoint the kept payroll bridge to the native overtime engine.

    The reworked ``l10n_do_hr_payroll_news_attendance`` no longer depends on the
    manual-calculation module ``l10n_do_hr_news_attendance`` (about to be
    uninstalled) but on ``hr_payroll_attendance``. Update the recorded
    dependencies *before* the uninstall so the bridge is not cascade-uninstalled
    and so the native module is pulled in."""
    if not util.module_installed(cr, PAYROLL_BRIDGE):
        return
    util.module_deps_diff(
        cr,
        PAYROLL_BRIDGE,
        plus=["hr_payroll_attendance"],
        minus=["l10n_do_hr_news_attendance"],
    )
    _logger.info(
        "Repointed %s dependencies: -l10n_do_hr_news_attendance +hr_payroll_attendance",
        PAYROLL_BRIDGE,
    )


def _uninstall_modules(cr):
    for module_name in MODULES_TO_UNINSTALL:
        if util.module_installed(cr, module_name):
            _logger.info("Uninstalling redundant module: %s", module_name)
            util.uninstall_module(cr, module_name)
        else:
            _logger.debug("Module %s not installed; skipping.", module_name)


def migrate(cr, version):
    _migrate_employees_to_native_overtime(cr)
    _repoint_payroll_bridge_deps(cr)
    _uninstall_modules(cr)
