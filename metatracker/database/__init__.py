"""
Module to handle database operations
"""

from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


# Function to check if you can connect to the database with SQLAlchemy
def check_connection(engine: Engine) -> bool:
    """
    Check Connection

    :param engine: SQLAlchemy Engine
    :type engine: Engine
    :return: Connection Status
    :rtype: bool
    """

    with engine.connect():
        return True


def create_engine(db_host: str) -> Engine:
    """
    Create Engine

    :param db_host: Database Host
    :type db_host: str
    :return: SQLAlchemy Engine
    :rtype: Engine
    """

    engine = sqlalchemy_create_engine(db_host)
    return engine


# Function to create a database session
def create_session(engine: Engine) -> sessionmaker[Session]:
    """
    Create Session

    :param engine: SQLAlchemy Engine
    :type engine: Engine
    :return: SQLAlchemy Session
    :rtype: sessionmaker[Session]
    """

    session = sessionmaker(bind=engine)
    return session
