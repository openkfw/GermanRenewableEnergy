import logging
import os
import warnings
from contextlib import contextmanager
from datetime import datetime
from typing import List

import psycopg2
import yaml
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import sessionmaker

from kfw_mastr import config_path
from kfw_mastr.utils.orm import Base

# Suppress specific SAWarning
warnings.filterwarnings(
    "ignore", r"Did not recognize type 'geometry' of column 'geometry'", SAWarning
)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

_engine = None
_metadata = None


def setup_logger():
    """Configure logging in console and log file.

    Returns
    -------
    logging.Logger
        Logger with two handlers: console and file.
    """

    log_dir = os.path.join(os.path.dirname(__file__), "../..", "logs")
    run_id =  os.environ["RUN_ID"]
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Check if handlers already exist to prevent adding them multiple times
    if not logger.handlers:
        # Define a common formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Create and configure file handler
        file_handler = logging.FileHandler(os.path.join(log_dir, f"kfw-mastr_{run_id}.log"))
        file_handler.setFormatter(formatter)

        debug_handler = logging.FileHandler(
            os.path.join(log_dir, f"kfw-mastr_{run_id}_debug.log")
        )
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(formatter)

        # Create and configure console handler

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.addHandler(debug_handler)

    return logger


def set_env_var_from_config(config, key):
    """
    Set an environment variable from the configuration dictionary.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing the key-value pairs.
    key : str
        The key to look for in the configuration dictionary.

    Raises
    ------
    ValueError
        If the key is not found in the configuration dictionary.
    """
    value = config.get(key)
    if value:
        os.environ[key] = value
    else:
        raise ValueError(f"{key} not found in the configuration file. It is mandatory!")


def setup_configs(config, config_path):
    """
    Set up configuration by reading values from a YAML file and setting environment variables.

    Parameters
    ----------
    config_filename : str, optional
        The name of the configuration file, by default "config.yaml".

    Raises
    ------
    ValueError
        If 'REPO_ROOT' is not found in the configuration file.

    Notes
    -----
    The function reads the specified YAML configuration file and sets environment variables
    based on its content. If certain keys are not found, default values are used. All relevant
    environment variables are logged.
    """

    os.environ["CONFIG_PATH"] = config_path
    

    # mandatory variables
    set_env_var_from_config(config, "REPO_ROOT")
    set_env_var_from_config(config, "INPUT_PATH")
    set_env_var_from_config(config, "OUTPUT_PATH")
    os.environ["RUN_ID"] = config.get("RUN_ID", "")
    os.environ["NUMBER_THREADS"] = config.get("NUMBER_THREADS", "4")

    os.environ["POSTGRESQL_HOST"] = config.get("POSTGRESQL_HOST", "None")
    os.environ["POSTGRESQL_DB_NAME"] = config.get("POSTGRESQL_DB_NAME", "kfw-mastr")
    os.environ["POSTGRESQL_USER"] = config.get("POSTGRESQL_USER", "postgres")
    os.environ["POSTGRESQL_PASSWORD"] = config.get("POSTGRESQL_PASSWORD", "postgres")
    os.environ["POSTGRESQL_PORT"] = config.get("POSTGRESQL_PORT", "5512")

    os.environ["SOFTWARE_VERSION"] = config.get("SOFTWARE_VERSION", "1_0_0")
    os.environ["OUTFILE_POSTFIX"] = config.get("OUTFILE_POSTFIX", timestamp)
    os.environ["CONFLICT_ACTION"] = config.get("CONFLICT_ACTION", "update")

    os.environ["YEARS"] = config.get("YEARS", "2023")
    os.environ["BATCH_SIZE"] = config.get("BATCH_SIZE", "200000")
    os.environ["LIMIT_MASTR_UNITS"] = config.get("LIMIT_MASTR_UNITS", "None")

    os.environ["YEAR_SOLAR_ANGLE"] = config.get("YEAR_SOLAR_ANGLE", "2000")
    os.environ["SAVE_HOURLY_DATA"] = config.get("SAVE_HOURLY_DATA", "False")

    os.environ["CALC_SOLAR"] = config.get("CALC_SOLAR", "False")
    os.environ["SPECIFIC_SOLAR_UNITS"] = config.get("SPECIFIC_SOLAR_UNITS", "None")
    os.environ["CURTAILMENT_SOLAR"] = config.get("CURTAILMENT_SOLAR", "None")

    os.environ["CALC_WIND"] = config.get("CALC_WIND", "False")
    os.environ["SPECIFIC_WIND_UNITS"] = config.get("SPECIFIC_WIND_UNITS", "None")
    os.environ["CURTAILMENT_WIND"] = config.get("CURTAILMENT_WIND", "None")

    os.environ["EXPORT_WIND"] = config.get("EXPORT_WIND", "False")
    os.environ["EXPORT_SOLAR"] = config.get("EXPORT_SOLAR", "False")
    os.environ["EXPORT_BATCH_SIZE"] = config.get("EXPORT_BATCH_SIZE", "100000")
    os.environ["EXPORT_UNITS"] = config.get("EXPORT_UNITS", "all")
    os.environ["EXPORT_YEARS"] = config.get("EXPORT_YEARS", "None")

    os.environ["AGGREGATE_WIND"] = config.get("AGGREGATE_WIND", "False")
    os.environ["AGGREGATE_SOLAR"] = config.get("AGGREGATE_SOLAR", "False")


def output_configs():
    # Log all relevant environment variables
    logger.info(
        f"Environment variables set: "
        f"REPO_ROOT={os.getenv('REPO_ROOT')}, "
        f"NUMBER_THREADS={os.getenv('NUMBER_THREADS')}, "
        f"INPUT_PATH={os.getenv('INPUT_PATH')}, "
        f"OUTPUT_PATH={os.getenv('OUTPUT_PATH')}, "
        f"SOFTWARE_VERSION={os.getenv('SOFTWARE_VERSION')}, "
        f"OUTFILE_POSTFIX={os.getenv('OUTFILE_POSTFIX')}, "
        f"CONFLICT_ACTION={os.getenv('CONFLICT_ACTION')}, "
        f"YEARS={os.getenv('YEARS')}, "
        f"BATCH_SIZE={os.getenv('BATCH_SIZE')}, "
        f"LIMIT_MASTR_UNITS={os.getenv('LIMIT_MASTR_UNITS')}, "
        f"YEAR_SOLAR_ANGLE={os.getenv('YEAR_SOLAR_ANGLE')}, "
        f"SAVE_HOURLY_DATA={os.getenv('SAVE_HOURLY_DATA')}, "
        f"CALC_SOLAR={os.getenv('CALC_SOLAR')}, "
        f"SPECIFIC_SOLAR_UNITS={os.getenv('SPECIFIC_SOLAR_UNITS')}, "
        f"CURTAILMENT_SOLAR={os.getenv('CURTAILMENT_SOLAR')}, "
        f"CALC_WIND={os.getenv('CALC_WIND')}, "
        f"SPECIFIC_WIND_UNITS={os.getenv('SPECIFIC_WIND_UNITS')}, "
        f"CURTAILMENT_WIND={os.getenv('CURTAILMENT_WIND')}, "
        f"EXPORT_WIND={os.getenv('EXPORT_WIND')}, "
        f"EXPORT_SOLAR={os.getenv('EXPORT_SOLAR')}, "
        f"EXPORT_BATCH_SIZE={os.getenv('EXPORT_BATCH_SIZE')}, "
        f"EXPORT_UNITS={os.getenv('EXPORT_UNITS')}, "
        f"EXPORT_YEARS={os.getenv('EXPORT_YEARS')}"
    )  # don't include postgresql settings in log due to security concerns


def load_config(config_path):
    """
    Load configuration from a YAML file.

    This function reads the specified YAML configuration file and sets up the
    configurations.

    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file.

    Returns
    -------
    None
    """

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        setup_configs(config, config_path)


load_config(config_path)
logger = setup_logger()
output_configs()

def create_directories(directory_list: List[str]) -> None:
    """
    Create directories from a list if they do not already exist.

    Parameters
    ----------
    directory_list : List[str]
        List of directory paths to be created.
    """
    logger.info("Creating input and output directories if not existent")
    for directory in directory_list:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)


create_directories(
    [
        f"{os.path.join(os.getenv('INPUT_PATH'), 'era5', 'hourly')}",
        f"{os.path.join(os.getenv('INPUT_PATH'), 'ags')}",
        f"{os.path.join(os.getenv('OUTPUT_PATH'), 'wind')}",
        f"{os.path.join(os.getenv('OUTPUT_PATH'), 'solar')}",
    ]
)


class SingletonEngine:
    _instance = None

    def __new__(cls):
        """
        Create and return a singleton instance of the class.

        If the instance does not exist, it creates the instance and initializes
        the engine and metadata attributes.

        Returns
        -------
        SingletonEngine
            The singleton instance of the SingletonEngine class.
        """

        if cls._instance is None:
            cls._instance = super(SingletonEngine, cls).__new__(cls)
            cls._instance.engine, cls._instance.metadata = (
                cls._instance._create_engine()
            )
        return cls._instance

    def _create_engine(self) -> tuple:
        """
        Create and configure the database engine and metadata.

        Depending on the environment, the method sets up the engine for either
        an Azure-hosted PostgreSQL database or a local PostgreSQL database.

        Returns
        -------
        tuple
            A tuple containing the created engine and the reflected metadata.
        """

        POSTGRESQL_DB_NAME = os.getenv("POSTGRESQL_DB_NAME")
        POSTGRES_USER = os.getenv("POSTGRESQL_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
        PORT = os.getenv("POSTGRESQL_PORT")

        if POSTGRES_PASSWORD == "AZURE":
            logger.info("AZURE database is selected")
            POSTGRESQL_HOST = os.getenv("POSTGRESQL_HOST")
            os.system(
                'export PGPASSWORD=$(az account get-access-token --resource-type oss-rdbms --query "[accessToken]" -o tsv);'
            )
            password = os.environ["PGPASSWORD"].rstrip("\n")
            sslmode = "require"

            # Construct connection string
            def connect():
                conn_string = (
                    "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
                        POSTGRESQL_HOST,
                        POSTGRES_USER,
                        POSTGRESQL_DB_NAME,
                        password,
                        sslmode,
                    )
                )
                return psycopg2.connect(conn_string)

            engine = create_engine("postgresql://", creator=connect)

        else:
            engine = create_engine(
                f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{POSTGRESQL_DB_NAME}"
            )
        Base.metadata.bind = engine
        metadata = MetaData()
        metadata.reflect(engine)
        logger.info("Local database is selected")
        return engine, metadata


def get_engine() -> tuple:
    """
    Retrieve the database engine and metadata.

    Initializes the engine and metadata if they are not already set.

    Returns
    -------
    tuple
        A tuple containing the engine and metadata.
    """

    global _engine, _metadata
    if _engine is None or _metadata is None:
        singleton_engine = SingletonEngine()
        _engine = singleton_engine.engine
        _metadata = singleton_engine.metadata
    return _engine, _metadata


@contextmanager
def session_scope(engine):
    """
    Provide a transactional scope around a series of operations.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine to bind the session.

    Yields
    ------
    sqlalchemy.orm.Session
        The database session.
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
