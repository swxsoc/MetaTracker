# Instrument Configuration Table
# Schema:
#   instrument_configuration_id: int (primary key)
#   instrument_{i+1}_id: int (foreign key)

from typing import Any

from sqlalchemy import Column, ForeignKey, Integer

from metatracker import CONFIGURATION

from . import base_table as Base

table_dict = {
    "__tablename__": f"{CONFIGURATION.mission_name}_instrument_configuration",
    "instrument_configuration_id": Column(Integer, primary_key=True),
}

for i in range(len(CONFIGURATION.instruments)):
    table_dict[f"instrument_{i + 1}_id"] = Column(
        Integer, ForeignKey(f"{CONFIGURATION.mission_name}_instrument.instrument_id")
    )

InstrumentConfigurationTable = type("InstrumentConfigurationTable", (Base.Base,), table_dict)


def return_class() -> Any:
    """
    Return Class
    """
    return InstrumentConfigurationTable
