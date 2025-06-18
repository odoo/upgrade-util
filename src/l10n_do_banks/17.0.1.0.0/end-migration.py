from odoo.addons.base.maintenance.migrations import util
from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)


def delete_advanced_web_domain_widget_assets(cr):
    """
    Script to delete advanced_web_domain_widget assets.
    """
    env = api.Environment(cr, SUPERUSER_ID, {})
    assets = env['ir.asset'].search([('name', 'like', 'advanced_web_domain_widget.assets_backend%')])
    for asset in assets:
        asset.unlink()
    _logger.info("Advanced Web Domain Widget assets deleted")

def deactivate_studio_views(cr):
    """
    Script de post-migración para buscar y desactivar vistas de Studio.

    Args:
        cr (cursor): Cursor de la base de datos.
        installed_version (str): Versión instalada del módulo.
    """

    env = api.Environment(cr, SUPERUSER_ID, {})

    # Get all views with xml_id LIKE 'studio_customization'
    query = """
        SELECT id, name, key
        FROM ir_ui_view
        WHERE name LIKE 'studio_customization%' or name LIKE 'Odoo Studio%'
    """
    cr.execute(query)

    studio_views = []
    for record in cr.fetchall():
        view = {
            'id': record[0],
            'name': record[1],
            'xml_id': record[2]
        }
        studio_views.append(view)

    # Deactivate views
    for view in studio_views:
        try:
            view_obj = env['ir.ui.view'].browse(view['id'])
            
            inherited_views = env['ir.ui.view'].search([
                ('inherit_id', '=', view_obj.id),
                ('active', '=', True)
            ])
            
            for inherited in inherited_views:
                inherited.write({'active': False})
                _logger.info(f"Inherited View Deactivated: {inherited.name} (ID: {inherited.id})")
            
            view_obj.write({'active': False})
            _logger.info(f"View Deactivated: {view['name']} (ID: {view['id']})")            
            env.cr.commit()
            
        except Exception as e:
            _logger.warning(f"Error deactivating view {view['name']} (ID: {view['id']}): {e}")
    

def deactivate_automated_actions(cr):
    """
    Script on end-migration to deactivate automated actions.

    Args:
        cr (cursor): Database cursor
    """

    env = api.Environment(cr, SUPERUSER_ID, {})
    
    # Get all active automated actions
    automated_actions = env['base.automation'].search([('active', '=', True)])
    
    # Deactivate each automated action
    for action in automated_actions:
        try:
            action.write({'active': False})
            _logger.info(f"Automated action deactivated: {action.name} (ID: {action.id})")
        except Exception as e:
            _logger.error(f"Error deactivating automated action {action.name} (ID: {action.id}): {e}")
    
    


def migrate(cr, version):
    delete_advanced_web_domain_widget_assets(cr)
    deactivate_studio_views(cr)
    deactivate_automated_actions(cr)
