# Status Table
# Schema:
#   status_id: int (primary key)
#   science_file_id: int (foreign key)
#   processing_status: str
#   processing_status_message: str
#   original_processing_timestamp: datetime
#   last_processing_timestamp: datetime
#   reprocessed_count: int
#   processing_time_length: int
#   origin_file_id: int (foreign key) (optional)


from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from metatracker import CONFIGURATION

from . import base_table as Base

status_origin_association = Table(
    f"{CONFIGURATION.mission_name}_status_origin_association",
    Base.Base.metadata,
    Column("status_id", Integer, ForeignKey(f"{CONFIGURATION.mission_name}_status.status_id"), primary_key=True),
    Column(
        "origin_file_id",
        Integer,
        ForeignKey(f"{CONFIGURATION.mission_name}_science_file.science_file_id"),
        primary_key=True,
    ),
)


class StatusTable(Base.Base):  # type: ignore
    __tablename__ = f"{CONFIGURATION.mission_name}_status"

    # Primary Key
    status_id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    science_file_id = Column(
        Integer, ForeignKey(f"{CONFIGURATION.mission_name}_science_file.science_file_id"), nullable=False
    )

    # Many-to-many relationship to origin files
    origin_files = relationship(
        "ScienceFileTable",  # replace with actual class if it's named differently
        secondary=status_origin_association,
        backref="status_origins",
        lazy="joined",
    )

    # Processing Information
    processing_status = Column(String, nullable=False)
    processing_status_message = Column(String, nullable=True)
    original_processing_timestamp = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    last_processing_timestamp = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    reprocessed_count = Column(Integer, default=0)
    processing_time_length = Column(Integer, nullable=True)  # seconds

    def __init__(
        self,
        science_file_id: int,
        processing_status: str,
        processing_status_message: Optional[str] = None,
        original_processing_timestamp: Optional[datetime] = None,
        last_processing_timestamp: Optional[datetime] = None,
        reprocessed_count: int = 0,
        processing_time_length: Optional[int] = None,
        origin_files: Optional[list[Any]] = None,
    ) -> None:
        self.science_file_id = science_file_id  # type: ignore[assignment]
        self.processing_status = processing_status  # type: ignore[assignment]
        self.processing_status_message = processing_status_message  # type: ignore[assignment]
        self.original_processing_timestamp = original_processing_timestamp or datetime.now(timezone.utc)  # type: ignore[assignment]
        self.last_processing_timestamp = last_processing_timestamp or datetime.now(timezone.utc)  # type: ignore[assignment]
        self.reprocessed_count = reprocessed_count  # type: ignore[assignment]
        self.processing_time_length = processing_time_length  # type: ignore[assignment]
        self.origin_files = origin_files

    def __repr__(self) -> str:
        return super().__repr__()  # type: ignore[no-any-return]


def return_class() -> Any:
    return StatusTable
