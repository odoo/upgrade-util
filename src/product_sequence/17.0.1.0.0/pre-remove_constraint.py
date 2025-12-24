# -*- coding: utf-8 -*-

from odoo.addons.base.maintenance.migrations import util
import logging

_logger = logging.getLogger(__name__)

CONSTRAINT_NAME = 'product_product_default_code_uniq'


def migrate(cr, version):
    """Drop the product default_code unique constraint when needed."""
    _logger.info('PRE-MIGRATION: Dropping %s constraint', CONSTRAINT_NAME)

    if not util.module_installed(cr, 'product_code_unique'):
        _logger.info('Module product_code_unique is not installed. Nothing to do.')
        return

    cr.execute(
        """
            SELECT COUNT(*)
            FROM product_product
            WHERE default_code IS NULL OR default_code = ''
        """
    )
    missing_codes = cr.fetchone()[0]

    if not missing_codes:
        _logger.info('No products without default_code found. Skipping constraint drop.')
        return

    _logger.info('Dropping constraint %s to allow temporary duplicates.', CONSTRAINT_NAME)
    cr.execute(
        """
            ALTER TABLE product_product
            DROP CONSTRAINT IF EXISTS product_product_default_code_uniq
        """
    )
    _logger.info('Constraint %s dropped (if it existed).', CONSTRAINT_NAME)
