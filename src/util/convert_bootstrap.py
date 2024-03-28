import warnings

from .views.bootstrap import *  # noqa: F403

warnings.warn(
    "`util.convert_bootstrap` module has been deprecated in favor of `util.views.bootstrap`. "
    "Consider adjusting your imports.",
    category=DeprecationWarning,
    stacklevel=1,
)
