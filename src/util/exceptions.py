# -*- coding: utf-8 -*-
__all__ = ["MigrationError", "SleepyDeveloperError", "UnknownModuleError", "UpgradeError", "UpgradeWarning"]


class UpgradeError(Exception):
    pass


class SleepyDeveloperError(ValueError):
    pass


class UnknownModuleError(AssertionError):
    pass


class UpgradeWarning(Warning):
    pass


# Compat
MigrationError = UpgradeError
"""
:meta private: exclude from online docs
"""
