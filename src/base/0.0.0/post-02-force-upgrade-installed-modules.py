# -*- coding: utf-8 -*-
from odoo.addons.base.maintenance.migrations import util


def migrate(cr, version):
    # Short-circuit the state change made by Odoo during the loading process (at STEP 2).
    # In Odoo, it's done by calling the method `button_upgrade` on all modules passed
    # in the command line option `-u`. This method drills down through all downstream
    # dependencies. That's why it works when upgrading the `base` module.
    # This technique works well during updates (keep the same major version) where the
    # modules' dependencies don't change.
    # However, during upgrades (to the next version), it may happen that modules (A) got new
    # dependencies (B) that are not installed yet (being a new module or not).
    # As `button_update` won't update the state of non installed modules, if the modules (A)
    # only dependencies are the new ones (B), their state will remain `installed`. Still, the
    # corresponding packages (in the graph) will have the `update` flag, meaning the modules
    # will still be upgraded.
    # But partially. Due to their initial `installed` state, the `end-` scripts won't be
    # applied, leading to an incomplete upgrade.
    # This is the case for the `account_asset` module in `saas~12.3`.
    # This can be observed at https://upgradeci.odoo.com/upgradeci/run/3665
    # NOTE: This behavior has been fixed by https://github.com/odoo/odoo/pull/85516
    #       but we need to keep this for older versions.
    query = "UPDATE ir_module_module SET state = 'to upgrade' WHERE state = 'installed'"
    if util.column_exists(cr, "ir_module_module", "imported"):
        query += " AND COALESCE(imported, false) = false"

    cr.execute(query)
