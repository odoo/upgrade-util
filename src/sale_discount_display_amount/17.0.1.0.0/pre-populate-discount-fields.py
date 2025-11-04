# Copyright 2024
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).

import logging
from odoo.tools.sql import column_exists, create_column

_logger = logging.getLogger(__name__)

# Fields that need to be pre-populated with 0 to avoid automatic computation
DISCOUNT_FIELDS = [
    ("sale_order", "discount_total"),
    ("sale_order", "discount_subtotal"),
    ("sale_order", "price_subtotal_no_discount"),
    ("sale_order", "price_total_no_discount"),
    ("sale_order_line", "discount_total"),
    ("sale_order_line", "discount_subtotal"),
    ("sale_order_line", "price_subtotal_no_discount"),
    ("sale_order_line", "price_total_no_discount"),
]


def migrate(cr, version):
    """
    Pre-populate discount fields with value 0 to prevent Odoo ORM
    from trying to compute them automatically when converting to stored fields.
    """
    _logger.info("Pre-populating discount fields with value 0 to avoid automatic computation...")
    
    for table, column in DISCOUNT_FIELDS:
        if not column_exists(cr, table, column):
            _logger.info(f"Creating column {column} in table {table}")
            create_column(cr, table, column, "numeric")
    
    for table, column in DISCOUNT_FIELDS:
        _logger.info(f"Pre-populating {table}.{column} with value 0...")
        cr.execute(f"""
            UPDATE {table} 
            SET {column} = 0 
            WHERE {column} IS NULL
        """)
        
        # Count updated records
        cr.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = 0")
        count = cr.fetchone()[0]
        _logger.info(f"Updated {count} records in {table}.{column}")
    
    _logger.info("Pre-population completed. Stored fields will have value 0 and won't be computed automatically.")
