# -*- coding: utf-8 -*-


class UpgradeError(Exception):
    pass


class SleepyDeveloperError(ValueError):
    pass


class UnknownModuleError(AssertionError):
    pass


# Compat
MigrationError = UpgradeError
