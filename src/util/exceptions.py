# -*- coding: utf-8 -*-


class UpgradeError(Exception):
    pass


class SleepyDeveloperError(ValueError):
    pass


# Compat
MigrationError = UpgradeError
