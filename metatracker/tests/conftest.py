"""
Shared pytest fixtures for all metatracker tests.

These fixtures are automatically available to all test modules in the package.
"""

import os
from typing import Any


def pytest_configure(config: Any) -> None:
    """
    Pytest hook that runs before test collection.

    Sets SWXSOC_MISSION so that when metatracker is first imported during
    collection, the CONFIGURATION singleton (and table class definitions that
    bind to it at class-definition time) use the correct mission.
    """
    os.environ.setdefault("SWXSOC_MISSION", "padre")
    import swxsoc  # type: ignore

    swxsoc._reconfigure()
