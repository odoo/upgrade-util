# -*- coding: utf-8 -*-
try:
    from odoo import release

    from odoo.addons.base.maintenance.migrations import util
except ImportError:
    # for symlinked versions
    from openerp import release

    from openerp.addons.base.maintenance.migrations import util


def migrate(cr, version):
    check_upgrade_step(version)
    util.inherit._get_base_version(cr)


def check_upgrade_step(version):
    series = release.major_version
    if version.startswith(series + "."):
        return  # same series, e.g. a module version bump
    source = util.parse_version(version)
    source_major = int(source[0])
    if version.startswith("12.0."):
        next_version = "saas~12.3"
    elif version.startswith("saas~12.3."):
        next_version = "13.0"
    else:
        next_version = "{}.0".format(source_major + 1)
    if source_major < 15:
        # no rolling releases before 15.0: the next version is the only possible target
        allowed = series == next_version
    else:
        # either a later rolling release of the same major version, or the next major version
        allowed = source < util.parse_version(series) <= util.parse_version(next_version)
    if not allowed:
        raise util.MigrationError(
            "Cannot upgrade from {} to {}: Cannot jump over major versions (or over saas~12.3).".format(version, series)
        )
