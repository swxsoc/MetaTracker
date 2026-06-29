# MetaTracker


![GitHub issues](https://img.shields.io/github/issues/swxsoc/MetaTracker)
[![Build status](https://img.shields.io/github/actions/workflow/status/swxsoc/MetaTracker/main.yml?branch=main)](https://github.com/swxsoc/MetaTracker/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/swxsoc/MetaTracker/branch/main/graph/badge.svg)](https://codecov.io/gh/swxsoc/MetaTracker)
[![Commit activity](https://img.shields.io/github/commit-activity/m/swxsoc/MetaTracker)](https://img.shields.io/github/commit-activity/m/swxsoc/MetaTracker)
[![License](https://img.shields.io/github/license/swxsoc/MetaTracker)](https://img.shields.io/github/license/swxsoc/MetaTracker)

This is a python package that helps keep track of both Raw Binary and CDF Files in a Relational Database.

- **Github repository**: <https://github.com/swxsoc/MetaTracker/>

## Features
- Track both Raw Binary and CDF Files
- Supports multiple instrument configurations
- Support for multiple RDBMS via SQLAlchemy connection strings

## Requirements
- Python 3.8 or higher
- SQL Database (via connection string)
- [Make](https://www.gnu.org/software/make/manual/make.html) 
- [Poetry](https://github.com/python-poetry/poetry) (optional)

## Installation and Usage
To install the library and its dependencies, follow these steps:

1. Install with dependency manager of your choice:
    ```bash
    # Install with pip
    pip install git+https://github.com/swxsoc/MetaTracker.git

    # Install with poetry
    poetry add git+https://github.com/swxsoc/MetaTracker.git
    ```


2. Create both the engine and the session, with your RDBMS of choice connection string. For example, to create a session with a SQLite database, you can do the following:
    ```python
    from metatracker.database import create_engine, create_session

    # Create a database engine and session
    engine = create_engine("sqlite:///test.db")
    session = create_session(engine)
    ```

3. If this is your first time using the library, you will need to create the database tables. To do so, run the following command:
    ```python
    from metatracker.database.tables import set_up_tables

    # Set up tables
    set_up_tables(engine, session)
    ```

4. Define a science file name parser function which parses the file Path object and returns the following information in a dictionary. This is the formart the dictionary outputted by the function should have:
    ```python
    # def science_file_name_parser():
    #    return {
      #      "instrument": str,
      #      "mode": str,
      #      "test": bool,
      #      "time": str,
      #      "level": str,
      #      "version": str,
      #      "descriptor": str,
    #    }
    
    # For this example we will be using a predefined science file parser defined in the 
    # swxsoc package (https://github.com/swxsoc/swxsoc.git).
    from swxsoc.util import util

    parser = util.parse_science_filename
    ```
5. Now you can instantiate a `MetaTracker` object with the engine and science file parser function you defined:
    ```python
    from metatracker.tracker import tracker
    from pathlib import Path

    # Instantiate the tracker
    tracker = tracker.MetaTracker(engine, parser)

    # Path of the science file to be tracked (for example)
    file = Path("./hermes_MAG_l0_2022259-030002_v01.bin")
    ```
6. You can also `track` the file which adds the appropriate entries to the database. To do so, run the following command:
    ```python
    # Track the file
    tracker.track(file)
    ```

## Database Schema
This is the database schema for the MetaTracker database. The database schema is defined in the `metatracker.database.tables` module. 

## Contributing
### How to set-up Development Environment
This project makes use of [Poetry](https://python-poetry.org/) to manage dependencies and virtual environments. Also included is a Make file to set-up your development environment. To set-up your development environment, follow these steps:

1. Clone the repository

    ```bash
    git clone https://github.com/swxsoc/MetaTracker.git

    cd MetaTracker
    ```

2. Set-up your development environment

    ```bash
    make install
    ```


### How to run tests
This project uses [pytest](https://docs.pytest.org/en/stable/) to run tests and exports an HTML report of the code coverage. To run tests, follow these steps:

1. Inside the project directory, run the following command:

    ```bash
    make test
    ```


### How to run linter
This project uses black and ruff to lint the code. To run the linter, follow these steps:

1. Inside the project directory, run the following command:

    ```bash
    make check
    ```

## License
This project is licensed under the terms of the MIT license.


