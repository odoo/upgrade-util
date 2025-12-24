import logging

_logger = logging.getLogger(__name__)

CONSTRAINT_NAME = 'product_product_default_code_uniq'
TABLE_NAME = 'product_product'
CONSTRAINT_EXISTS_SQL = """
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE c.conname = %s AND t.relname = %s AND n.nspname = 'public'
"""
DUPLICATES_SQL = """
    SELECT default_code, COUNT(*) AS qty
    FROM product_product
    WHERE default_code IS NOT NULL AND default_code != ''
    GROUP BY default_code
    HAVING COUNT(*) > 1
"""


def migrate(cr, version):
    """Recreate the unique constraint on product_product.default_code."""
    _logger.info('POST-MIGRATION: Restoring %s on %s.default_code', CONSTRAINT_NAME, TABLE_NAME)

    cr.execute(CONSTRAINT_EXISTS_SQL, (CONSTRAINT_NAME, TABLE_NAME))
    if cr.fetchone():
        _logger.info('Constraint %s already exists. Nothing to do.', CONSTRAINT_NAME)
        return

    cr.execute(DUPLICATES_SQL)
    duplicates = cr.fetchall()
    if duplicates:
        sample = ', '.join(f'"{code}" ({qty}x)' for code, qty in duplicates[:5])
        if len(duplicates) > 5:
            sample += f', ... (+{len(duplicates) - 5})'
        _logger.error('Cannot recreate %s; duplicate default_code values detected: %s', CONSTRAINT_NAME, sample)
        raise Exception('Resolve duplicate product_product.default_code values before rerunning the migration.')

    _logger.info('Dropping constraint %s (if it exists) and recreating it.', CONSTRAINT_NAME)
    cr.execute(f"ALTER TABLE {TABLE_NAME} DROP CONSTRAINT IF EXISTS {CONSTRAINT_NAME}")
    cr.execute(f"ALTER TABLE {TABLE_NAME} ADD CONSTRAINT {CONSTRAINT_NAME} UNIQUE (default_code)")
    _logger.info('Constraint %s restored successfully.', CONSTRAINT_NAME)

