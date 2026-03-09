from metatracker.database import check_connection, create_engine, create_session


# Test create database engine
def test_create_engine() -> None:
    # Create engine
    engine = create_engine("sqlite://")

    # Check if engine is created
    assert engine is not None


# Test create database session
def test_create_session() -> None:
    # Create engine
    engine = create_engine("sqlite://")

    # Create session
    session = create_session(engine)

    # Check if session is created
    assert session is not None


# Test check connection
def test_check_connection() -> None:
    # Create engine
    engine = create_engine("sqlite://")

    # Check connection
    connection = check_connection(engine)

    # Check if connection is valid
    assert connection is True
