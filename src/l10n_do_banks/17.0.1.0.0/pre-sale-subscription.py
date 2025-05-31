import logging

_logger = logging.getLogger(__name__)


def fix_sale_orders(cr):
    """
    Find and fix sale_order records with invalid next_invoice_date.

    This function searches for all sale_order records where:
      - start_date IS NOT NULL
      - next_invoice_date IS NOT NULL
      - next_invoice_date < start_date
      - subscription_state = '6_churn'
    For each such record, it sets next_invoice_date to NULL (or could be set to start_date if required).

    Parameters:
        cr: Odoo database cursor

    Side effects:
        Updates the next_invoice_date field in the sale_order table for affected records.
        Logs actions and findings for traceability.
    """
    update_query = """
        UPDATE sale_order
           SET next_invoice_date = NULL
         WHERE start_date IS NOT NULL
           AND next_invoice_date IS NOT NULL
           AND next_invoice_date < start_date
           AND subscription_state = '6_churn'
    """
    cr.execute(update_query)
    affected_rows = cr.rowcount
    if affected_rows == 0:
        _logger.info(
            "No sale orders found with next_invoice_date before start_date and state '6_churn'."
        )
        return
    _logger.info(
        "Fixing %d sale orders: Setting next_invoice_date to NULL.",
        affected_rows
    )


def migrate(cr, version):
    """
    Migration entrypoint for fixing invalid next_invoice_date in sale_order records.

    Parameters:
        cr: Odoo database cursor
        version: Target version for the migration (unused)

    Side effects:
        Calls fix_sale_orders to perform the corrective update.
    """
    fix_sale_orders(cr)
