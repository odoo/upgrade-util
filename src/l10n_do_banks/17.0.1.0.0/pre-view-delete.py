from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)

def migrate(cr, version):
    """
    Pre-migration script to delete specific views before upgrade.
    
    Args:
        cr (cursor): Database cursor
        version (str): Module version
    """
    _logger.info('Starting pre-migration view deletion')
    env = api.Environment(cr, SUPERUSER_ID, {})

    views_to_delete = [
        'product_product_price_widget.product_product_tree_view_inherit_widget',
        'sale_stock_product_price_widget.sale_price_widgets_view', 
        'sale_stock_qty_date_widgets.sale_stock_qty_date_widgets_view',
        'product_stock_qty_date_widget.product_product_tree_view_inherit_widget_qty'
        'warranty_registration_extra_features.warranty_case_claims_form_view1_inherit',
        'studio_customization.odoo_studio_warranty_5e101e7c-e388-4589-b9b8-628b03ca43f1',
        'studio_customization.odoo_studio_warranty_64540003-0f78-4e15-8bba-6d3475418fed',
        'product_product_price_widget.product_product_tree_view_inherit_widget',
    ]

    for xml_id in views_to_delete:
        try:
            view = env.ref(xml_id)
            if view:
                _logger.info(f'Deleting view: {xml_id}')
                # First check and delete any inherited views
                inherited_views = env['ir.ui.view'].search([('inherit_id', '=', view.id)])
                if inherited_views:
                    _logger.info(f'Found {len(inherited_views)} inherited views for {xml_id}')
                    for inherited in inherited_views:
                        try:
                            _logger.info(f'Deleting inherited view: {inherited.xml_id}')
                            inherited.unlink()
                        except Exception as e:
                            _logger.warning(f'Error deleting inherited view: {e}')
                
                # Check if this view inherits from another
                if view.inherit_id:
                    _logger.info(f'View {xml_id} inherits from {view.inherit_id.xml_id}')
                    view.inherit_id = False
                
                # Now delete the view itself
                view.unlink()
                _logger.info(f'Successfully deleted view: {xml_id}')
            else:
                _logger.info(f'View not found: {xml_id}')
        except Exception as e:
            _logger.warning(f'Error deleting view {xml_id}: {e}')

    _logger.info('Finished pre-migration view deletion')
