from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from metatracker import log
from metatracker.database import check_connection, create_session
from metatracker.database.tables.file_level_table import FileLevelTable
from metatracker.database.tables.file_type_table import FileTypeTable
from metatracker.database.tables.instrument_configuration_table import InstrumentConfigurationTable
from metatracker.database.tables.instrument_table import InstrumentTable
from metatracker.database.tables.science_file_table import ScienceFileTable
from metatracker.database.tables.science_product_table import ScienceProductTable
from metatracker.database.tables.status_table import StatusTable

db_retry = retry(
    reraise=True,
    stop=stop_after_attempt(5),  # Try up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 2s, 4s, 8s, 10s, 10s
    retry=(retry_if_exception_type(OperationalError) | retry_if_exception_type(IntegrityError)),
)


class MetaTracker:
    """Main API entry point for tracking science files in the MetaTracker database.

    Provides methods to parse, validate, and store science file metadata including
    file information, science products, and processing status. All database write
    operations are wrapped with retry logic via the ``@db_retry`` decorator.
    """

    def __init__(self, engine: Engine, science_file_parser: Callable[[Path], dict[str, Any]]) -> None:
        """
        Initialize the MetaTracker instance.

        Parameters
        ----------
        engine : Engine
            SQLAlchemy database engine used for all database operations.
        science_file_parser : Callable[[Path], dict[str, Any]]
            Callable that accepts a file path and returns a dictionary of parsed
            science file metadata (e.g., instrument, level, version, time).

        Raises
        ------
        ConnectionError
            If the database connection cannot be established.
        """
        self.engine = engine

        try:
            check_connection(self.engine)
        except Exception:
            raise ConnectionError("Database connection is not valid") from None

        self.science_file_parser = science_file_parser

    def track(
        self,
        file: Path,
        s3_key: str,
        s3_bucket: str,
        science_product_id: Optional[int] = None,
        status: Optional[dict[str, Any]] = None,
    ) -> tuple[int, int]:
        """Track a science file by parsing its metadata and storing it in the database.

        Parses the file to extract metadata, creates or retrieves an associated science
        product record, inserts the science file record, and optionally records processing
        status information.

        Parameters
        ----------
        file : Path
            Path to the science file on disk.
        s3_key : str
            S3 object key where the file is stored.
        s3_bucket : str
            S3 bucket name where the file is stored.
        science_product_id : Optional[int]
            Existing science product ID to associate with. If ``None``, a new
            science product record is created.
        status : Optional[dict[str, Any]]
            Optional dictionary containing processing status information.
            Expected keys: ``"processing_status"``, ``"processing_status_message"``,
            ``"processing_time_length"``, ``"origin_file_ids"``.

        Returns
        -------
        tuple[int, int]
            A tuple of ``(science_file_id, science_product_id)``.

        Raises
        ------
        FileNotFoundError
            If the file does not exist on disk.
        """
        if not self.is_file_real(file):
            log.debug("File does not exist")
            raise FileNotFoundError("File does not exist")
        session = create_session(self.engine)

        parsed_file = self.parse_file(session, file, s3_key, s3_bucket)
        parsed_science_product = self.parse_science_product(session, file)

        # Check if science_product_id is provided
        if science_product_id is None:
            science_product_id = self.add_to_science_product_table(
                session=session, parsed_science_product=parsed_science_product
            )
            log.debug("Added to Science Product Table")
        else:
            log.debug(f"Using existing science_product_id: {science_product_id}")

        # Add to science file table
        log.debug("Added to Science Product Table")
        science_file_id = self.add_to_science_file_table(
            session=session, parsed_file=parsed_file, science_product_id=science_product_id
        )
        log.debug("Added to Science File Table")

        if status:
            # Add to status table if status is provided
            log.debug("Added to Status Table")
            self.add_to_status_table(
                session=session,
                science_file_id=science_file_id,
                processing_status=status.get("processing_status"),  # type: ignore[arg-type]
                processing_status_message=status.get("processing_status_message"),
                processing_time_length=status.get("processing_time_length"),
                origin_file_ids=status.get("origin_file_ids"),
            )

        return science_file_id, science_product_id

    @db_retry
    def add_to_science_file_table(
        self, session: sessionmaker[Session], parsed_file: dict[str, Any], science_product_id: int
    ) -> int:
        """Add a science file record to the science file table, or return the existing ID.

        If a file with the same filename already exists, returns its ID without
        inserting a duplicate. Otherwise, inserts a new record and returns the
        newly created ID.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        parsed_file : dict[str, Any]
            Dictionary of parsed file metadata. Expected keys include
            ``"filename"``, ``"file_type"``, ``"file_level"``, ``"file_version"``,
            ``"file_size"``, ``"s3_key"``, ``"s3_bucket"``, ``"file_extension"``,
            ``"file_path"``, ``"file_modified_timestamp"``, and ``"is_public"``.
        science_product_id : int
            ID of the associated science product record.

        Returns
        -------
        int
            The ``science_file_id`` of the inserted or existing record,
            or ``0`` if ``parsed_file`` is empty.
        """

        with session.begin() as sql_session:
            if not parsed_file:
                log.debug("File is not valid")
                return 0

            # 1. Check for existing file by UNIQUE constraint (filename)
            file = (
                sql_session.query(ScienceFileTable).filter(ScienceFileTable.filename == parsed_file["filename"]).first()
            )

            if file:
                # Optionally update fields if needed (for now just return the id)
                log.debug(f"File already exists in Science File Table with id: {file.science_file_id}")
                return file.science_file_id  # type: ignore[return-value]

            # 2. If not found, insert new
            file = ScienceFileTable(
                science_product_id=science_product_id,
                file_type=parsed_file["file_type"],
                file_level=parsed_file["file_level"],
                filename=parsed_file["filename"],
                file_version=parsed_file["file_version"],
                file_size=parsed_file["file_size"],
                s3_key=parsed_file["s3_key"],
                s3_bucket=parsed_file["s3_bucket"],
                file_extension=parsed_file["file_extension"],
                file_path=parsed_file["file_path"],
                file_modified_timestamp=parsed_file["file_modified_timestamp"],
                is_public=parsed_file["is_public"],
            )
            sql_session.add(file)
            sql_session.flush()
            science_file_id = file.science_file_id
            log.debug(f"Added file to Science File Table with id: {science_file_id}")
            return science_file_id  # type: ignore[return-value]

    @db_retry
    def add_to_science_product_table(
        self, session: sessionmaker[Session], parsed_science_product: dict[str, Any]
    ) -> int:
        """Add a science product record to the science product table, or return the existing ID.

        Checks for an existing science product matching the same instrument configuration,
        mode, and reference timestamp. If found, returns its ID. Otherwise, inserts a new
        record and returns the newly created ID.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        parsed_science_product : dict[str, Any]
            Dictionary of parsed science product metadata. Expected keys:
            ``"instrument_configuration_id"``, ``"mode"``,
            ``"reference_timestamp"``.

        Returns
        -------
        int
            The ``science_product_id`` of the inserted or existing record.
        """

        with session.begin() as sql_session:
            # Check if science product exists with same instrument configuration id, mode, and reference timestamp
            science_product = (
                sql_session.query(ScienceProductTable)
                .filter(
                    ScienceProductTable.instrument_configuration_id
                    == parsed_science_product["instrument_configuration_id"],
                    ScienceProductTable.mode == parsed_science_product["mode"],
                    ScienceProductTable.reference_timestamp == parsed_science_product["reference_timestamp"],
                )
                .first()
            )

            # If science product exists, return science product id
            if science_product:
                return science_product.science_product_id  # type: ignore[return-value]

            # If science product doesn't exist, add it to the database
            science_product = ScienceProductTable(
                instrument_configuration_id=parsed_science_product["instrument_configuration_id"],
                mode=parsed_science_product["mode"],
                reference_timestamp=parsed_science_product["reference_timestamp"],
            )
            sql_session.add(science_product)
            sql_session.flush()

            # return science product id that was just added
            return science_product.science_product_id  # type: ignore[return-value]

    @db_retry
    def add_to_status_table(
        self,
        session: sessionmaker[Session],
        science_file_id: int,
        processing_status: str,
        processing_status_message: Optional[str] = None,
        processing_time_length: Optional[int] = None,
        origin_file_ids: Optional[list[int]] = None,
    ) -> int:
        """Add or update a status entry for a science file in the status table.

        If a status record already exists for the given ``science_file_id``, updates
        the processing fields and increments the reprocessed count. Otherwise, creates
        a new status record. Origin files are appended without duplicates.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        science_file_id : int
            ID of the science file to associate the status with.
        processing_status : str
            Processing status string (e.g., ``"SUCCESS"``, ``"FAILED"``).
        processing_status_message : Optional[str]
            Optional human-readable status message.
        processing_time_length : Optional[int]
            Optional processing duration in seconds.
        origin_file_ids : Optional[list[int]]
            Optional list of science file IDs that were inputs to the processing
            that produced this file.

        Returns
        -------
        int
            The ``status_id`` of the inserted or updated record.

        Raises
        ------
        ValueError
            If ``origin_file_ids`` is not a list of integers.
        """

        with session.begin() as sql_session:
            # Validate and fetch origin files if provided
            origin_files = []
            if origin_file_ids is not None:
                if not isinstance(origin_file_ids, list) or not all(isinstance(i, int) for i in origin_file_ids):
                    raise ValueError("origin_file_ids must be a list of integers or None")
                origin_files = (
                    sql_session.query(ScienceFileTable)
                    .filter(ScienceFileTable.science_file_id.in_(origin_file_ids))
                    .all()
                )

            # Check if status already exists
            status = sql_session.query(StatusTable).filter(StatusTable.science_file_id == science_file_id).first()

            if status:
                # Update fields
                status.processing_status = processing_status  # type: ignore[assignment]
                status.processing_status_message = processing_status_message  # type: ignore[assignment]
                status.last_processing_timestamp = datetime.now(timezone.utc)  # type: ignore[assignment]
                status.reprocessed_count += 1  # type: ignore[assignment]
                status.processing_time_length = processing_time_length  # type: ignore[assignment]

                # Extend existing origin_files without duplicates
                if origin_files:
                    existing_ids = {f.science_file_id for f in status.origin_files}
                    new_files = [f for f in origin_files if f.science_file_id not in existing_ids]
                    status.origin_files.extend(new_files)

            else:
                # Create new entry
                status = StatusTable(
                    science_file_id=science_file_id,
                    processing_status=processing_status,
                    processing_status_message=processing_status_message,
                    processing_time_length=processing_time_length,
                    origin_files=origin_files,
                )
                sql_session.add(status)

            sql_session.flush()
            return status.status_id  # type: ignore[return-value]

    @staticmethod
    def get_file_size(file: Path) -> int:
        """Get the size of a file in bytes.

        Parameters
        ----------
        file : Path
            Path to the file.

        Returns
        -------
        int
            File size in bytes.
        """
        return file.stat().st_size

    @staticmethod
    def get_file_modified_timestamp(file: Path) -> datetime:
        """Get the last-modified timestamp of a file.

        Parameters
        ----------
        file : Path
            Path to the file.

        Returns
        -------
        datetime
            Datetime of the file's last modification.
        """
        return datetime.fromtimestamp(file.stat().st_mtime)

    @staticmethod
    def is_file_real(file: Path) -> bool:
        """Check if the given path points to an existing file.

        Parameters
        ----------
        file : Path
            Path to check.

        Returns
        -------
        bool
            ``True`` if the path is an existing file, ``False`` otherwise.
        """
        return file.is_file()

    def parse_science_file_data(self, file: Path) -> dict[str, Any]:
        """Parse a science file using the configured parser.

        Delegates to the ``science_file_parser`` callable provided at initialization.

        Parameters
        ----------
        file : Path
            Path to the science file to parse.

        Returns
        -------
        dict[str, Any]
            Dictionary of parsed file metadata (e.g., instrument, level,
            version, time, mode).
        """
        return self.science_file_parser(file)

    def parse_file(self, session: sessionmaker[Session], file: Path, s3_key: str, s3_bucket: str) -> dict[str, Any]:
        """Parse a file and build a metadata dictionary for the science file table.

        Validates the file extension and level against the database before constructing
        the metadata dictionary. Returns an empty dictionary if the file does not exist
        or fails validation.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        file : Path
            Path to the science file to parse.
        s3_key : str
            S3 object key where the file is stored.
        s3_bucket : str
            S3 bucket name where the file is stored.

        Returns
        -------
        dict[str, Any]
            Dictionary of file metadata ready for insertion, or an empty
            dictionary if the file is invalid.
        """
        if self.is_file_real(file):
            extension = self.parse_extension(file)
            if not self.is_valid_file_type(session=session, extension=extension):
                log.debug("File type is not valid")
                return {}

            science_file_data = self.parse_science_file_data(file)

            if not self.is_valid_file_level(session=session, file_level=science_file_data["level"]):
                log.debug("File level is not valid")
                return {}

            return {
                "file_path": self.parse_absolute_path(file),
                "s3_key": s3_key,
                "s3_bucket": s3_bucket,
                "filename": self.parse_filename(file),
                "file_extension": self.parse_extension(file),
                "file_size": self.get_file_size(file),
                "file_modified_timestamp": self.get_file_modified_timestamp(file),
                "file_level": science_file_data["level"],
                "file_type": self.get_file_type(session=session, extension=extension),
                "file_version": science_file_data["version"],
                "is_public": True,
            }

        return {}

    def parse_science_product(self, session: sessionmaker[Session], file: Path) -> dict[str, Any]:
        """Parse a science file and build a metadata dictionary for the science product table.

        Extracts the instrument configuration, reference timestamp, and mode from the
        parsed file data. Validates the timestamp, instrument, and instrument configuration
        against the database. Returns an empty dictionary if the file does not exist
        or fails any validation step.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        file : Path
            Path to the science file to parse.

        Returns
        -------
        dict[str, Any]
            Dictionary with keys ``"instrument_configuration_id"``,
            ``"reference_timestamp"``, and ``"mode"``, or an empty dictionary
            if validation fails.
        """
        if self.is_file_real(file):
            science_product_data = self.parse_science_file_data(file)

            if not self.is_valid_timestamp(science_product_data["time"]):
                log.debug("Timestamp is not valid")
                return {}

            # Check if value is already a datetime object
            if isinstance(science_product_data["time"].value, datetime):
                reference_timestamp = science_product_data["time"].value
            else:
                reference_timestamp = datetime.strptime(science_product_data["time"].value, "%Y-%m-%dT%H:%M:%S.%f")

            if not self.is_valid_instrument(session=session, instrument_short_name=science_product_data["instrument"]):
                log.debug("Instrument is not valid")
                return {}

            config = self.get_instrument_configurations(session=session)

            if [science_product_data["instrument"]] not in config.values():
                log.debug("Instrument configuration is not valid")
                return {}

            # return Key with matching list values
            instrument_config_id = [k for k, v in config.items() if science_product_data["instrument"] in v][0]
            if not instrument_config_id:
                raise ValueError(f"Instrument configuration id not found {science_product_data}")

            return {
                "instrument_configuration_id": instrument_config_id,
                "reference_timestamp": reference_timestamp,
                "mode": science_product_data["mode"],
            }

        return {}

    @staticmethod
    def is_valid_timestamp(timestamp: Any) -> bool:
        """Check if a timestamp value is not ``None``.

        Parameters
        ----------
        timestamp : Any
            Timestamp value to validate. Typically an astropy-like time object
            from the science file parser.

        Returns
        -------
        bool
            ``True`` if the timestamp is not ``None``.
        """
        return timestamp is not None

    @staticmethod
    def is_valid_instrument(session: sessionmaker[Session], instrument_short_name: str) -> bool:
        """Check if an instrument short name exists in the instrument table.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        instrument_short_name : str
            Short name of the instrument to validate.

        Returns
        -------
        bool
            ``True`` if the instrument short name is found in the database.
        """
        with session.begin() as sql_session:
            instruments = sql_session.query(InstrumentTable).all()
            valid_instrument_short_names = [instrument.short_name for instrument in instruments]

            return instrument_short_name in valid_instrument_short_names

    @staticmethod
    def get_file_type(session: sessionmaker[Session], extension: str) -> str:
        """Get the file type short name for a given file extension.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        extension : str
            File extension to look up (e.g., ``".cdf"``).

        Returns
        -------
        str
            Short name of the matching file type.
        """
        with session.begin() as sql_session:
            file_type = sql_session.query(FileTypeTable).filter(FileTypeTable.extension == extension).first()

            return file_type.short_name  # type: ignore[union-attr, return-value]

    @staticmethod
    def is_valid_file_type(session: sessionmaker[Session], extension: str) -> bool:
        """Check if a file extension corresponds to a valid file type in the database.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        extension : str
            File extension to validate (e.g., ``".cdf"``).

        Returns
        -------
        bool
            ``True`` if the extension matches a known file type.
        """
        with session.begin() as sql_session:
            file_types = sql_session.query(FileTypeTable).all()
            valid_extensions = [file_type.extension for file_type in file_types]

            return extension in valid_extensions

    @staticmethod
    def is_valid_file_level(session: sessionmaker[Session], file_level: str) -> bool:
        """Check if a file level string is a valid level in the database.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        file_level : str
            File level short name to validate (e.g., ``"l1"``).

        Returns
        -------
        bool
            ``True`` if the file level is found in the database.
        """
        with session.begin() as sql_session:
            file_levels = sql_session.query(FileLevelTable).all()
            valid_file_levels = [file_level.short_name for file_level in file_levels]

            return file_level in valid_file_levels

    @staticmethod
    def parse_extension(file: Path) -> str:
        """Parse and return the lowercase file extension.

        Parameters
        ----------
        file : Path
            Path to the file.

        Returns
        -------
        str
            Lowercase file extension including the leading dot (e.g., ``".cdf"``).
        """
        return file.suffix.lower()

    @staticmethod
    def parse_filename(file: Path) -> str:
        """Parse and return the filename without its extension.

        Parameters
        ----------
        file : Path
            Path to the file.

        Returns
        -------
        str
            Filename stem (e.g., ``"data_file"`` from ``"data_file.cdf"``).
        """
        return file.stem

    @staticmethod
    def parse_absolute_path(file: Path) -> str:
        """Return the absolute path of a file as a string.

        Parameters
        ----------
        file : Path
            Path to the file.

        Returns
        -------
        str
            Absolute file path as a string.
        """
        return str(file.absolute())

    @staticmethod
    def get_instruments(session: sessionmaker[Session]) -> dict[int, str]:
        """Get all instruments from the database.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.

        Returns
        -------
        dict[int, str]
            Mapping of instrument IDs to their short names.
            Example: ``{1: "meddea", 2: "sharp"}``.
        """
        with session.begin() as sql_session:
            instruments = sql_session.query(InstrumentTable).all()
            result: dict[int, str] = {
                instrument.instrument_id: instrument.short_name for instrument in instruments  # type: ignore[misc]
            }

            return result

    def get_instrument_configurations(self, session: sessionmaker[Session]) -> dict[int, list[str]]:
        """Get all instrument configurations from the database.

        Each configuration maps to a sorted list of instrument short names that
        comprise that configuration.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.

        Returns
        -------
        dict[int, list[str]]
            Mapping of configuration IDs to sorted lists of instrument short names.
            Example: ``{1: ["meddea"], 2: ["sharp"]}``.
        """
        with session.begin() as sql_session:
            # Get amount of instruments from InstrumentTable
            instruments = self.get_instruments(session)
            amount_of_instruments = len(instruments)
            configurations: list[Any] = sql_session.query(InstrumentConfigurationTable).all()

            instrument_configurations: dict[int, list[str]] = {}
            for config in configurations:
                # For the amount of instruments
                instrument_names: list[str] = []
                for i in range(amount_of_instruments):
                    attribute = getattr(config, "instrument_" + str(i + 1) + "_id")
                    if attribute is not None:
                        attribute = self.get_instrument_by_id(session, int(attribute))
                        instrument_names.append(attribute)

                instrument_names.sort()
                instrument_configurations[config.instrument_configuration_id] = instrument_names

            return instrument_configurations

    def get_instrument_by_id(self, session: sessionmaker[Session], instrument_id: int) -> str:
        """Get the short name of an instrument by its ID.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        instrument_id : int
            Primary key ID of the instrument.

        Returns
        -------
        str
            Short name of the instrument.

        Raises
        ------
        KeyError
            If no instrument with the given ID exists.
        """
        with session.begin():
            instruments = self.get_instruments(session)
            return instruments[instrument_id]

    def map_instrument_list(self, session: sessionmaker[Session], instrument_list: list[int]) -> list[str]:
        """Map a list of instrument IDs to their corresponding short names.

        Parameters
        ----------
        session : sessionmaker[Session]
            SQLAlchemy session factory bound to the database engine.
        instrument_list : list[int]
            List of instrument IDs to resolve.

        Returns
        -------
        list[str]
            List of instrument short names in the same order as the input.
        """
        with session.begin():
            return [self.get_instrument_by_id(session, instrument_id) for instrument_id in instrument_list]
