from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    _logger.info("Pre-migration test alert 15.0.1.0.0")