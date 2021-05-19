# -*- coding: utf-8 -*-
import collections

try:
    from odoo import netsvc
except ImportError:
    from openerp import netsvc


# migration environ, used to share data between scripts
ENVIRON = {
    "__renamed_fields": collections.defaultdict(set),
}

NEARLYWARN = 25  # between info and warning; appear on runbot build page
netsvc.LEVEL_COLOR_MAPPING[NEARLYWARN] = (netsvc.YELLOW, netsvc.DEFAULT)
