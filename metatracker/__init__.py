from pathlib import Path
from typing import Any, Optional

from swxsoc import log as swxsoc_log  # type: ignore

from metatracker.config import load_config
from metatracker.config.config import MetaTrackerConfiguration

# Set up Logging based on SWxSOC Logging
log = swxsoc_log

CONFIGURATION = load_config()

_package_directory = Path(__file__).parent
_test_files_directory = _package_directory / "tests" / "test_files"


def get_config() -> MetaTrackerConfiguration:
    """
    Retrieve the current global MetaTracker configuration.

    Returns the module-level ``CONFIGURATION`` singleton that holds
    database connection details, mission name, instrument definitions,
    file levels, and file types used throughout the package.

    Returns
    -------
    MetaTrackerConfiguration
        The active configuration instance for the MetaTracker package.

    See Also
    --------
    set_config : Update or reload the global configuration.

    Examples
    --------
    >>> from metatracker import get_config
    >>> cfg = get_config()
    >>> cfg.mission_name  # doctest: +SKIP
    'padre'
    """
    return CONFIGURATION


def set_config(config: Optional[dict[str, Any]] = None) -> None:
    """
    Set or reload the global MetaTracker configuration.

    Replaces the module-level ``CONFIGURATION`` singleton with a new
    `MetaTrackerConfiguration` built from *config*. When called with no
    arguments (or ``None``), the configuration is reloaded from the
    current ``swxsoc`` config, which is driven by the ``SWXSOC_MISSION``
    environment variable.

    Parameters
    ----------
    config : dict, optional
        A dictionary of configuration values to apply. Keys may include
        ``"mission_name"``, ``"instruments"``, ``"file_levels"``,
        ``"file_types"``, ``"db_host"``, among others.  If ``None``
        (the default), the configuration is reloaded from ``swxsoc``.

    See Also
    --------
    get_config : Retrieve the current global configuration.

    Examples
    --------
    Reload configuration from the ``swxsoc`` defaults:

    >>> from metatracker import set_config
    >>> set_config()  # doctest: +SKIP

    Apply a custom configuration dictionary:

    >>> set_config({"mission_name": "hermes", "db_host": "sqlite:///"})  # doctest: +SKIP
    """
    global CONFIGURATION

    CONFIGURATION = load_config(config)
