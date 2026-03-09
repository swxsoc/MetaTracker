# File Type Table
# Schema:
#   short_name: str (primary key)
#   full_name: str
#   description: str

from typing import Any

from sqlalchemy import Column, String

from metatracker import CONFIGURATION

from . import base_table as Base


class FileTypeTable(Base.Base):  # type: ignore
    # Name Of Table
    __tablename__ = f"{CONFIGURATION.mission_name}_file_type"

    # Short Name Of File Type
    short_name = Column(String, primary_key=True)

    # Full Name Of File Type
    full_name = Column(String)

    # Description Of File Type
    description = Column(String)

    # Extension Of File Type
    extension = Column(String)

    def __init__(self, short_name: str, full_name: str, description: str, extension: str) -> None:
        """
        Constructor for File Type Table
        """
        self.short_name = short_name  # type: ignore[assignment]
        self.full_name = full_name  # type: ignore[assignment]
        self.description = description  # type: ignore[assignment]
        self.extension = extension  # type: ignore[assignment]

    def __repr__(self) -> str:
        return super().__repr__()  # type: ignore[no-any-return]


def return_class() -> Any:
    """
    Return Class
    """
    return FileTypeTable
