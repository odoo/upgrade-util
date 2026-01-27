try:
    from odoo.addons.base.maintenance.migrations import util
    from odoo.addons.base.maintenance.migrations.util.modules import _get_autoinstallable_modules
except ImportError:
    # for symlinked versions
    from openerp.addons.base.maintenance.migrations import util
    from openerp.addons.base.maintenance.migrations.util.modules import _get_autoinstallable_modules


def migrate(cr, version):
    cr.execute(
        """
        INSERT
          INTO ir_config_parameter(key, value)
        VALUES ('upgrade.start.time', now() at time zone 'utc')
            ON CONFLICT (key)
            DO UPDATE
           SET value = (now() at time zone 'utc')
         WHERE EXCLUDED.value::timestamp - ir_config_parameter.value::timestamp > interval '72 hours'
        """
    )
    util.ENVIRON["__modules_to_skip_autoinstall"] = _get_autoinstallable_modules(cr)
