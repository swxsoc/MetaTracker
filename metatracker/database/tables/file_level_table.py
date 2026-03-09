# File Level Table
# Schema:
#   short_name: str (primary key)
#   full_name: str
#   description: str

from typing import Any

from sqlalchemy import Column, String

from metatracker import CONFIGURATION

from . import base_table as Base


class FileLevelTable(Base.Base):  # type: ignore
    # Name Of Table
    __tablename__ = f"{CONFIGURATION.mission_name}_file_level"

    # Short Name Of File Level
    short_name = Column(String, primary_key=True)

    # Full Name Of File Level
    full_name = Column(String)

    # Description Of File Level
    description = Column(String)

    def __init__(self, full_name: str, short_name: str, description: str) -> None:
        """
        Constructor for File Level Table
        """

        self.full_name = full_name  # type: ignore[assignment]
        self.short_name = short_name  # type: ignore[assignment]
        self.description = description  # type: ignore[assignment]

    def __repr__(self) -> str:
        return super().__repr__()  # type: ignore[no-any-return]


def return_class() -> Any:
    """
    Return Class
    """
    return FileLevelTable
