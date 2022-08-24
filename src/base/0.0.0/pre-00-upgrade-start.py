# -*- coding: utf-8 -*-


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
