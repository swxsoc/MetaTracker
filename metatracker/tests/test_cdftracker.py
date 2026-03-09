import logging

from metatracker import get_config, log, set_config


def test_log() -> None:
    """
    Test log
    """

    # Check if log is created
    assert log is not None

    # Check if log is a logger
    assert isinstance(log, logging.Logger)

    # Check if log has a handler
    assert len(log.handlers) > 0


# Test set_config
def test_set_config() -> None:
    """
    Test set_config
    """

    config = get_config()

    # Check if config is set
    assert config is not None

    test_db_host = "test"

    # Set config
    set_config({"db_host": test_db_host})

    config = get_config()

    # Check if config is set
    assert config is not None

    # Check if config is set correctly
    assert config.db_host == test_db_host

    # Test String Representation
    print(config)
