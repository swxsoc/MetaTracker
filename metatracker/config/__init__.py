from typing import Any, Optional

from . import config


def load_config(new_config: Optional[dict[str, Any]] = None) -> config.MetaTrackerConfiguration:
    """
    Load configuration

    Args:
        config (Dict[str, Any], optional): Configuration. Defaults to None.

    Returns:
        config.MetaTrackerConfiguration: Configuration
    """

    return config.MetaTrackerConfiguration(new_config)
