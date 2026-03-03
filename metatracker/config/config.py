"""
Module for configuration of the application.
"""

from itertools import combinations
from typing import Any, Dict, List, Optional

from swxsoc import config as swxsoc_config

# Default Database Host
DEFAULT_DB_HOST = "sqlite:///"
DEFAULT_FILE_LEVELS = [
    {"description": "RAW File", "full_name": "RAW", "short_name": "raw"},
    {"description": "Level 0 File", "full_name": "Level 0", "short_name": "l0"},
    {"description": "Level 1 File", "full_name": "Level 1", "short_name": "l1"},
    {"description": "Quick Look File", "full_name": "Quick Look", "short_name": "ql"},
    {"description": "Level 2 File", "full_name": "Level 2", "short_name": "l2"},
    {"description": "Level 3 File", "full_name": "Level 3", "short_name": "l3"},
    {"description": "Level 4 File", "full_name": "Level 4", "short_name": "l4"},
]

# fmt: off
DEFAULT_FILE_TYPES = [
    {
        "description": "Raw Binary File",
        "full_name": "Raw BINARY",
        "short_name": "bin",
        "extension": ".bin"
    },
    {
        "description": "Raw Dat File",
        "full_name": "Raw DAT",
        "short_name": "dat",
        "extension": ".dat"
    },
    {
        "description": "Raw IDX File",
        "full_name": "Raw IDX",
        "short_name": "idx",
        "extension": ".idx"
    },
    {
        "description": "Common Data Format File",
        "full_name": "Common Data Format",
        "short_name": "cdf",
        "extension": ".cdf",
    },
    {
        "description": "Flexible Image Transport System File",
        "full_name": "Flexible Image Transport System",
        "short_name": "fits",
        "extension": ".fits",
    },
    {
        "description": "CSV File",
        "full_name": "Comma Separated Values",
        "short_name": "csv",
        "extension": ".csv",
    },
    {
        "description": "JSON File",
        "full_name": "JavaScript Object Notation",
        "short_name": "json",
        "extension": ".json",
    }
]
# fmt: on


class MetaTrackerConfiguration:
    """
    Class definition to wrap dictionary configuration the MetaTracker database. This class is used to load configuration from a dictionary and provide default values for missing keys. It also provides a string representation of the configuration for easy debugging and logging.
    """

    db_host: str
    mission_name: str
    instruments: List[Dict[str, Any]]
    instrument_configurations: List[Dict[str, Any]]
    file_levels: List[Dict[str, Any]]
    file_types: List[Dict[str, Any]]

    def __init__(self, config: Optional[Dict[str, Any]], use_swxsoc: bool = True) -> None:
        """
        Initialize a MetaTrackerConfiguration instance.

        Parameters
        ----------
        config : dict[str, Any] or None
            Configuration dictionary. Required keys are ``mission_name``, ``instruments``, and
            ``instrument_configurations``. Optional keys include ``db_host``, ``file_levels``, and
            ``file_types``, which fall back to package defaults if not provided. Pass ``None`` or an
            empty dict to rely entirely on ``use_swxsoc`` population.
        use_swxsoc : bool, optional
            If ``True`` (default), update ``config`` with values derived from the active SWxSOC
            mission configuration before validation.

        Raises
        ------
        ValueError
            If ``mission_name``, ``instruments``, or ``instrument_configurations`` are absent from
            ``config`` after any SWxSOC update.
        """
        # Instantiate a Config if Not Provided
        if not config:
            config = {}

        # Update with SWxSOC Configuration if directed
        if use_swxsoc:
            config.update(self.from_swxsoc())

        # Check that required keys are present in the config
        if "mission_name" not in config:
            raise ValueError("Missing required key 'mission_name' in configuration")
        if "instruments" not in config:
            raise ValueError("Missing required key 'instruments' in configuration")
        if "instrument_configurations" not in config:
            raise ValueError("Missing required key 'instrument_configurations' in configuration")

        # Set Default values for keys that are not present in the config
        if "db_host" not in config:
            config["db_host"] = DEFAULT_DB_HOST
        if "file_levels" not in config:
            config["file_levels"] = DEFAULT_FILE_LEVELS
        if "file_types" not in config:
            config["file_types"] = DEFAULT_FILE_TYPES

        self.db_host = config["db_host"]
        self.mission_name = config["mission_name"]
        self.instruments = config["instruments"]
        self.instrument_configurations = config["instrument_configurations"]
        self.file_levels = config["file_levels"]
        self.file_types = config["file_types"]

    def __repr__(self) -> str:
        return (
            f"MetaTrackerConfiguration(db_host={self.db_host}, mission_name={self.mission_name},"
            f" instruments={self.instruments}, instrument_configurations={self.instrument_configurations},"
            f" file_levels={self.file_levels}, file_types={self.file_types})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def from_swxsoc() -> Dict[str, Any]:
        """
        Load Configuration from SWxSOC Config as default configuration options for MetaTrackerConfiguration. This method is used to load configuration from SWxSOC Config and provide default values for missing keys.

        Parameters
        ----------
        None

        Returns
        -------
        Dict[str, Any]
            Configuration dictionary loaded from SWxSOC Config
        """
        # Get the Mission Configuration for the current SWxSOC Mission
        mission_config = swxsoc_config["mission"]
        # Get the list of instruments from the Mission Configuration
        instruments = mission_config["inst_names"]

        instruments_list = [
            {
                "instrument_id": idx + 1,
                "description": f"{mission_config['inst_fullnames'][idx]} ({mission_config['inst_targetnames'][idx]})",
                "full_name": mission_config["inst_fullnames"][idx],
                "short_name": mission_config["inst_shortnames"][idx],
            }
            for idx in range(len(instruments))
        ]

        # Generate all possible configurations of the instruments
        instrument_configurations = []
        config_id = 1
        for r in range(1, len(instruments) + 1):
            for combo in combinations(range(1, len(instruments) + 1), r):
                config = {"instrument_configuration_id": config_id}
                config.update(
                    {f"instrument_{i + 1}_id": combo[i] if i < len(combo) else None for i in range(len(instruments))}
                )
                instrument_configurations.append(config)
                config_id += 1

        metatracker_config = {
            "mission_name": mission_config["mission_name"],
            "instruments": instruments_list,
            "instrument_configurations": instrument_configurations,
        }

        return metatracker_config
