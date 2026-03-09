# Instrument Table
# Schema:
#   instrument_id: int (primary key)
#   instrument_configuration_id: int (foreign key)
#   mode: str
#   reference_timestamp: datetime

from typing import Any

from sqlalchemy import Column, Integer, String

from metatracker import CONFIGURATION

from . import base_table as Base


class InstrumentTable(Base.Base):  # type: ignore
    # Name Of Table
    __tablename__ = f"{CONFIGURATION.mission_name}_instrument"

    # ID Of Instrument (Primary Key)
    instrument_id = Column(Integer, primary_key=True)

    # Full Name Of Instrument
    full_name = Column(String)

    # Short Name Of Instrument
    short_name = Column(String)

    # Description Of Instrument
    description = Column(String)

    def __init__(self, instrument_id: int, full_name: str, short_name: str, description: str) -> None:
        """
        Constructor for Instrument Table
        """
        self.instrument_id = instrument_id  # type: ignore[assignment]
        self.full_name = full_name  # type: ignore[assignment]
        self.short_name = short_name  # type: ignore[assignment]
        self.description = description  # type: ignore[assignment]

    def __repr__(self) -> str:
        return super().__repr__()  # type: ignore[no-any-return]


def return_class() -> Any:
    """
    Return Class
    """
    return InstrumentTable
