# Science Product Table
# Schema:
#   science_product_id: int (primary key)
#   instrument_configuration_id: int (foreign key)
#   mode: str
#   reference_timestamp: datetime

from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from metatracker import CONFIGURATION

from . import base_table as Base


class ScienceProductTable(Base.Base):  # type: ignore
    __tablename__ = f"{CONFIGURATION.mission_name}_science_product"

    # ID Of Science Product (Primary Key)
    science_product_id = Column(Integer, primary_key=True, autoincrement=True)

    # ID Of Instrument Configuration (Foreign Key)
    instrument_configuration_id = Column(
        Integer, ForeignKey(f"{CONFIGURATION.mission_name}_instrument_configuration.instrument_configuration_id")
    )

    # Mode Of Science Product
    mode = Column(String)

    # Reference Timestamp Of Science Product
    reference_timestamp = Column(DateTime)

    children = relationship("ScienceFileTable", back_populates="parent", cascade="all, delete")

    def __init__(self, instrument_configuration_id: int, mode: str, reference_timestamp: datetime) -> None:
        """
        Constructor for Science Product Table
        """
        self.instrument_configuration_id = instrument_configuration_id  # type: ignore[assignment]
        self.mode = mode  # type: ignore[assignment]
        self.reference_timestamp = reference_timestamp  # type: ignore[assignment]

    def __repr__(self) -> str:
        return super().__repr__()  # type: ignore[no-any-return]


def return_class() -> Any:
    """
    Return Class
    """
    return ScienceProductTable
