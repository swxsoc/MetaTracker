"""
Setup Tables
"""

import re
from types import ModuleType
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.schema import Table

from metatracker import CONFIGURATION, log
from metatracker.database import create_session

from . import file_level_table as FileLevelTable
from . import file_type_table as FileTypeTable
from . import instrument_configuration_table as InstrumentConfigurationTable
from . import instrument_table as InstrumentTable
from . import science_file_table as ScienceFileTable
from . import science_product_table as ScienceProductTable
from . import status_table as StatusTable


def get_class_name(class_object: type) -> str:
    """
    Get the name of a class.

    Parameters
    ----------
    class_object : type
        The class to get the name of.

    Returns
    -------
    str
        The name of the class.
    """

    class_name = class_object.__name__

    return class_name


def get_table_modules() -> list[ModuleType]:
    """
    Get all table modules.

    Returns
    -------
    list[ModuleType]
        List of table modules.
    """

    modules = [
        FileLevelTable,
        FileTypeTable,
        InstrumentTable,
        InstrumentConfigurationTable,
        ScienceProductTable,
        ScienceFileTable,
        StatusTable,
    ]

    return modules


def get_table_classes(table_modules: list[ModuleType]) -> list[Any]:
    """
    Get ORM table classes from their containing modules.

    Parameters
    ----------
    table_modules : list[ModuleType]
        List of table modules, each expected to have a ``return_class()`` function.

    Returns
    -------
    list[Any]
        List of ORM table classes.
    """

    table_classes = [module.return_class() for module in table_modules]

    return table_classes


def get_table_from_class(table_class: Any) -> Table:
    """
    Get the SQLAlchemy ``Table`` object from an ORM table class.

    Parameters
    ----------
    table_class : type
        An ORM table class with a ``__table__`` attribute.

    Returns
    -------
    Table
        The underlying SQLAlchemy ``Table`` object.
    """

    table = table_class.__table__

    return table  # type: ignore[no-any-return]


def get_tables_from_classes(table_classes: list[Any]) -> list[Table]:
    """
    Get SQLAlchemy ``Table`` objects from a list of ORM table classes.

    Parameters
    ----------
    table_classes : list[type]
        List of ORM table classes.

    Returns
    -------
    list[Table]
        List of SQLAlchemy ``Table`` objects.
    """

    tables = [get_table_from_class(table_class) for table_class in table_classes]

    return tables


def get_tables(engine: Engine) -> list[str]:
    """
    Get all table names in the database.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.

    Returns
    -------
    list[str]
        List of table names in the database.
    """
    inspector = inspect(engine)

    return inspector.get_table_names()


def table_exists(engine: Engine, table_name: str) -> bool:
    """
    Check if a table exists in the database.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    table_name : str
        Name of the table to check for.

    Returns
    -------
    bool
        ``True`` if the table exists, ``False`` otherwise.
    """
    inspector = inspect(engine)

    tables = inspector.get_table_names()

    return table_name in tables


def get_columns(engine: Engine, table_name: str) -> list[dict[str, Any]]:
    """
    Get all columns in a table.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    table_name : str
        Name of the table to get columns for.

    Returns
    -------
    list[dict[str, Any]]
        List of column metadata dictionaries.
    """

    inspector = inspect(engine)

    return inspector.get_columns(table_name)  # type: ignore[return-value]


def populate_file_level_table(
    sql_session: sessionmaker[Session], file_levels: list[dict[str, Any]], file_level_table: Any
) -> None:
    """
    Upsert the file level table with the configured file levels.

    Inserts new file levels and updates metadata (``full_name``, ``description``)
    on existing rows. Rows present in the database but absent from *file_levels*
    are left in place for foreign-key integrity.

    Parameters
    ----------
    sql_session : sessionmaker[Session]
        SQLAlchemy session factory.
    file_levels : list[dict[str, Any]]
        List of file level dictionaries, each containing ``short_name``,
        ``full_name``, and ``description`` keys.
    file_level_table : type
        The ORM class for the file level table.
    """
    log.debug("Upserting File Level Table")
    with sql_session.begin() as session:
        for file_level in file_levels:
            existing = session.query(file_level_table).filter_by(short_name=file_level["short_name"]).first()
            if existing is None:
                log.debug(f"Inserting new file level '{file_level['short_name']}' into File Level Table")
                session.add(
                    file_level_table(
                        full_name=file_level["full_name"],
                        short_name=file_level["short_name"],
                        description=file_level["description"],
                    )
                )
            else:
                updated_fields: list[str] = []
                if existing.full_name != file_level["full_name"]:
                    existing.full_name = file_level["full_name"]
                    updated_fields.append("full_name")
                if existing.description != file_level["description"]:
                    existing.description = file_level["description"]
                    updated_fields.append("description")
                if updated_fields:
                    log.debug(f"Updated file level '{file_level['short_name']}' fields: {', '.join(updated_fields)}")
                else:
                    log.debug(f"File level '{file_level['short_name']}' is up to date, no changes needed")


def populate_file_type_table(
    sql_session: sessionmaker[Session], file_types: list[dict[str, Any]], file_type_table: Any
) -> None:
    """
    Upsert the file type table with the configured file types.

    Inserts new file types and updates metadata (``full_name``, ``description``,
    ``extension``) on existing rows. Rows present in the database but absent
    from *file_types* are left in place for foreign-key integrity.

    Parameters
    ----------
    sql_session : sessionmaker[Session]
        SQLAlchemy session factory.
    file_types : list[dict[str, Any]]
        List of file type dictionaries, each containing ``short_name``,
        ``full_name``, ``description``, and ``extension`` keys.
    file_type_table : type
        The ORM class for the file type table.
    """
    log.debug("Upserting File Type Table")
    with sql_session.begin() as session:
        for file_type in file_types:
            existing = session.query(file_type_table).filter_by(short_name=file_type["short_name"]).first()
            if existing is None:
                log.debug(f"Inserting new file type '{file_type['short_name']}' into File Type Table")
                session.add(
                    file_type_table(
                        short_name=file_type["short_name"],
                        full_name=file_type["full_name"],
                        description=file_type["description"],
                        extension=file_type["extension"],
                    )
                )
            else:
                updated_fields: list[str] = []
                if existing.full_name != file_type["full_name"]:
                    existing.full_name = file_type["full_name"]
                    updated_fields.append("full_name")
                if existing.description != file_type["description"]:
                    existing.description = file_type["description"]
                    updated_fields.append("description")
                if existing.extension != file_type["extension"]:
                    existing.extension = file_type["extension"]
                    updated_fields.append("extension")
                if updated_fields:
                    log.debug(f"Updated file type '{file_type['short_name']}' fields: {', '.join(updated_fields)}")
                else:
                    log.debug(f"File type '{file_type['short_name']}' is up to date, no changes needed")


def populate_instrument_table(
    sql_session: sessionmaker[Session], instruments: list[dict[str, Any]], instrument_table: Any
) -> None:
    """
    Upsert the instrument table with the configured instruments.

    Inserts new instruments and updates metadata (``short_name``, ``full_name``,
    ``description``) on existing rows. Rows present in the database but absent
    from *instruments* are left in place for foreign-key integrity.

    Parameters
    ----------
    sql_session : sessionmaker[Session]
        SQLAlchemy session factory.
    instruments : list[dict[str, Any]]
        List of instrument dictionaries, each containing ``instrument_id``,
        ``short_name``, ``full_name``, and ``description`` keys.
    instrument_table : type
        The ORM class for the instrument table.
    """
    log.debug("Upserting Instrument Table")
    with sql_session.begin() as session:
        for instrument in instruments:
            existing = session.query(instrument_table).filter_by(instrument_id=instrument["instrument_id"]).first()
            if existing is None:
                log.debug(
                    f"Inserting new instrument '{instrument['short_name']}'"
                    f" (id={instrument['instrument_id']}) into Instrument Table"
                )
                session.add(
                    instrument_table(
                        instrument_id=instrument["instrument_id"],
                        short_name=instrument["short_name"],
                        full_name=instrument["full_name"],
                        description=instrument["description"],
                    )
                )
            else:
                updated_fields: list[str] = []
                if existing.short_name != instrument["short_name"]:
                    existing.short_name = instrument["short_name"]
                    updated_fields.append("short_name")
                if existing.full_name != instrument["full_name"]:
                    existing.full_name = instrument["full_name"]
                    updated_fields.append("full_name")
                if existing.description != instrument["description"]:
                    existing.description = instrument["description"]
                    updated_fields.append("description")
                if updated_fields:
                    log.debug(
                        f"Updated instrument '{instrument['short_name']}'"
                        f" (id={instrument['instrument_id']}) fields: {', '.join(updated_fields)}"
                    )
                else:
                    log.debug(
                        f"Instrument '{instrument['short_name']}'"
                        f" (id={instrument['instrument_id']}) is up to date, no changes needed"
                    )


def sync_instrument_configuration_schema(engine: Engine) -> None:
    """
    Synchronise the physical instrument-configuration table schema with the ORM.

    When new instruments are added to the configuration, the ORM class (built
    dynamically at import time) will have ``instrument_N_id`` columns that do
    not yet exist in the physical database table.  This function detects those
    missing columns and issues ``ALTER TABLE … ADD COLUMN`` DDL to bring the
    schema in line with the ORM definition.

    All identifiers are quoted via the engine dialect's identifier preparer,
    and every missing column name is validated against the expected
    ``instrument_\\d+_id`` pattern before any DDL is executed.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.

    Raises
    ------
    ValueError
        If a missing column name does not match the ``instrument_N_id``
        naming convention.  No DDL is executed in this case.
    """
    ic_table_class = InstrumentConfigurationTable.return_class()
    table_name = ic_table_class.__table__.name

    if not table_exists(engine, table_name):
        log.debug(f"Table '{table_name}' does not exist yet; schema sync skipped (will be created by create_all)")
        return

    # Determine which columns the ORM expects vs. what the DB has
    db_columns = {col["name"] for col in get_columns(engine, table_name)}
    orm_columns = {col.name for col in ic_table_class.__table__.columns}
    missing_columns = orm_columns - db_columns

    if not missing_columns:
        log.debug(f"Instrument configuration table '{table_name}' schema is up to date")
        return

    log.debug(
        f"Instrument configuration table '{table_name}' is missing columns: {sorted(missing_columns)}."
        " Issuing ALTER TABLE statements."
    )

    # Validate that every missing column matches the expected naming pattern
    _INSTRUMENT_COL_RE = re.compile(r"^instrument_\d+_id$")
    for col_name in missing_columns:
        if not _INSTRUMENT_COL_RE.match(col_name):
            raise ValueError(
                f"Unexpected column name '{col_name}' does not match the expected "
                f"'instrument_N_id' pattern. Refusing to issue DDL."
            )

    # Use the dialect's identifier preparer to safely quote all identifiers
    preparer = engine.dialect.identifier_preparer
    instrument_table_name = InstrumentTable.return_class().__table__.name
    quoted_table = preparer.quote_identifier(table_name)
    quoted_instrument_table = preparer.quote_identifier(instrument_table_name)

    with engine.connect() as connection:
        for col_name in sorted(missing_columns):
            quoted_col = preparer.quote_identifier(col_name)
            # All instrument_N_id columns are nullable Integer FKs
            ddl = (
                f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_col} INTEGER"
                f" REFERENCES {quoted_instrument_table}({preparer.quote_identifier('instrument_id')})"
            )
            log.debug(f"Executing DDL: {ddl}")
            connection.execute(text(ddl))
        connection.commit()
    log.debug(f"Schema sync complete for '{table_name}'")


def populate_instrument_configuration_table(
    sql_session: sessionmaker[Session],
    instrument_configurations: list[dict[str, Any]],
    instrument_configuration_table: Any,
) -> None:
    """
    Upsert the instrument configuration table with all configured instrument combinations.

    Inserts new configuration rows and updates the ``instrument_N_id`` foreign-key
    values on existing rows.  Rows present in the database but absent from
    *instrument_configurations* are left in place for foreign-key integrity.

    Parameters
    ----------
    sql_session : sessionmaker[Session]
        SQLAlchemy session factory.
    instrument_configurations : list[dict[str, Any]]
        List of instrument configuration dictionaries, each containing
        ``instrument_configuration_id`` and ``instrument_N_id`` keys.
    instrument_configuration_table : type
        The ORM class for the instrument configuration table.
    """
    log.debug("Upserting Instrument Configuration Table")
    with sql_session.begin() as session:
        for _instrument_configuration in instrument_configurations:
            config_id = _instrument_configuration["instrument_configuration_id"]
            existing = (
                session.query(instrument_configuration_table).filter_by(instrument_configuration_id=config_id).first()
            )
            if existing is None:
                log.debug(f"Inserting new instrument configuration (id={config_id})")
                session.add(instrument_configuration_table(**_instrument_configuration))
            else:
                # Update any instrument_N_id fields that differ
                updated_fields: list[str] = []
                for key, value in _instrument_configuration.items():
                    if key == "instrument_configuration_id":
                        continue
                    if getattr(existing, key, None) != value:
                        setattr(existing, key, value)
                        updated_fields.append(key)
                if updated_fields:
                    log.debug(f"Updated instrument configuration (id={config_id}) fields: {', '.join(updated_fields)}")
                else:
                    log.debug(f"Instrument configuration (id={config_id}) is up to date, no changes needed")


def create_table(engine: Engine, table_class: Any) -> None:
    """
    Create a table if it doesn't already exist.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    table_class : type
        The ORM class for the table to create.
    """
    table_name = table_class.__table__.name
    if not table_exists(engine, table_name):
        log.debug(f"Creating {get_class_name(table_class)} table: {table_name}")
        table_class.__table__.create(bind=engine, checkfirst=True)
    else:
        log.debug(f"Table {table_name} already exists, skipping creation.")


def create_tables(engine: Engine) -> None:
    """
    Create and upsert all database tables.

    This function is **idempotent** — it is safe to call repeatedly.  On the
    first invocation it creates every table via ``Base.metadata.create_all``.
    On subsequent calls it:

    * Adds any new columns required by schema changes (e.g. new instruments
      adding ``instrument_N_id`` columns to the instrument-configuration table).
    * Inserts new rows for file levels, file types, instruments, and instrument
      configurations that are present in ``CONFIGURATION`` but missing from the
      database.
    * Updates metadata on existing rows whose values have drifted from the
      current configuration.
    * Leaves rows that are no longer in the configuration untouched so that
      existing foreign-key references remain valid.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    """
    log.debug("create_tables: starting")

    # --- 1. Create all tables at once (no-op if they already exist) ---
    from metatracker.database.tables.base_table import Base

    Base.metadata.create_all(engine)
    log.debug("create_tables: Base.metadata.create_all complete")

    # --- 2. Sync instrument configuration schema (add missing columns) ---
    sync_instrument_configuration_schema(engine)

    # --- 3. Upsert lookup / configuration tables ---
    session = create_session(engine)

    file_level_class = FileLevelTable.return_class()
    file_type_class = FileTypeTable.return_class()
    instrument_class = InstrumentTable.return_class()
    instrument_config_class = InstrumentConfigurationTable.return_class()

    log.debug("create_tables: upserting file level table")
    populate_file_level_table(session, CONFIGURATION.file_levels, file_level_class)

    log.debug("create_tables: upserting file type table")
    populate_file_type_table(session, CONFIGURATION.file_types, file_type_class)

    log.debug("create_tables: upserting instrument table")
    populate_instrument_table(session, CONFIGURATION.instruments, instrument_class)

    log.debug("create_tables: upserting instrument configuration table")
    populate_instrument_configuration_table(session, CONFIGURATION.instrument_configurations, instrument_config_class)

    log.debug("create_tables: complete")


def remove_tables(engine: Engine) -> None:
    """
    Remove all tables from the database.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    """
    # Get Table Modules
    table_modules = get_table_modules()

    # Get Table Classes
    table_classes = get_table_classes(table_modules)

    # Reverse Table Classes
    table_classes.reverse()

    # Remove Tables
    for table_class in table_classes:
        log.debug(f"Removing {get_class_name(table_class)} Table")
        table_class.__table__.drop(bind=engine, checkfirst=True)
