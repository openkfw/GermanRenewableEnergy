import os
import shutil
from typing import Generator

import numpy as np
import pandas as pd
import xarray as xr
from sqlalchemy import Table, select, asc

from kfw_mastr.utils.config import setup_logger, get_engine, session_scope
from kfw_mastr.utils.constants import VARIABLES_SOLAR, VARIABLES_WIND

engine, metadata = get_engine()
logger = setup_logger()


def log_downloaded_mastr_version():
    """
    Logs the download dates of MaStR units from the 'wind_extended' table.

    Retrieves the unique download dates from the 'DatumDownload' column of the 'wind_extended' table
    and formats them as strings. Logs an info message if there is only one unique date, or a warning
    message if there are multiple unique dates.

    Notes
    -----
    Ensure the database is checked for unintentional mix of different MaStR unit download dates if
    multiple dates are found.
    """

    data_version = (
        get_table_columns_as_dataframe(
            table_name="wind_extended", columns=["DatumDownload"]
        )["DatumDownload"]
        .unique()
        .tolist()
    )
    data_version_str = [date_obj.strftime("%Y-%m-%d") for date_obj in data_version]
    if len(data_version_str) <= 1:
        logger.info(
            f"Data version: The MaStR units were downloaded on: {data_version_str}"
        )
    else:
        logger.warning(
            f"Multiple MaStR unit download dates! The MaStR units were downloaded on: {data_version_str}. Please, check your db and make sure a mix of different MaStR unit download dates is intentional"
        )


def get_table_names() -> pd.DataFrame:
    """
    Retrieve the names of all tables in the database.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the table names with a single column "tn".
    """

    table_names = metadata.tables.keys()
    logger.debug(f"{table_names}")
    return pd.DataFrame(table_names, columns=["tn"])


def get_table_columns(
    tables: list, schema: str = "public"
) -> Generator[pd.DataFrame, None, None]:
    """
    Retrieve columns for specified tables from a given schema in the database.

    Parameters
    ----------
    tables : list
        The list of table names for which to retrieve the columns.
    schema : str, optional
        The schema in which the tables reside, by default "public".

    Yields
    ------
    Generator[pd.DataFrame, None, None]
        A generator yielding DataFrames, each containing the columns for one table.
    """

    columns_table = Table(
        "columns", metadata, autoload_with=engine, schema="information_schema"
    )
    for table_name in tables:
        with session_scope(engine=engine) as session:
            # Query to get columns for the specific table
            query = (
                select(columns_table.c.column_name)
                .where(columns_table.c.table_schema == schema)
                .where(columns_table.c.table_name == table_name)
                .order_by(asc(columns_table.c.ordinal_position))
            )

            result = session.execute(query).fetchall()

            # Process the result to get a list of columns
            data = {f"{table_name}": [column_name for (column_name,) in result]}
            df = pd.DataFrame(data=data)
            # Print the table columns
            logger.debug(f"Table: {df}")
            yield df


def get_table_columns_as_dataframe(table_name, columns: list, limit: int = None):
    """
    Retrieve specified columns from a database table as a pandas DataFrame.

    Parameters
    ----------
    table_name : str
        The name of the table from which to retrieve the columns.
    columns : list
        The list of column names to retrieve.
    limit : int, optional
        The maximum number of rows to retrieve, by default None.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the retrieved columns. Returns an empty DataFrame
        if the table is not found in metadata or if no valid columns are specified.
    """

    with session_scope(engine=engine) as session:

        if table_name not in metadata.tables:
            logger.error(f"Table {table_name} not found in metadata.")
            return pd.DataFrame()

        table = metadata.tables[table_name]
        selected_columns = [table.c[column] for column in columns if column in table.c]
        if not selected_columns:
            logger.error(f"No valid columns found in table {table_name}")
            return pd.DataFrame()

        # Query to get the specified columns from the table
        query = select(*selected_columns)
        if limit is not None:
            query = query.limit(limit)
        result = session.execute(query).fetchall()

        df = pd.DataFrame(result, columns=columns)

        df.drop_duplicates(inplace=True)
        return df


def load_era5_weather_wind(year):
    """
    Load ERA5 weather and wind datasets for a given year.

    Parameters
    ----------
    year : int
        The year for which to load the datasets.

    Returns
    -------
    tuple
        A tuple containing:
        - 100m_u_component_of_wind : xarray.Dataset
        - 100m_v_component_of_wind : xarray.Dataset
        - forecast_surface_roughness : xarray.Dataset
        - surface_pressure : xarray.Dataset
        - 2m_temperature : xarray.Dataset
    """
    variables = VARIABLES_WIND

    datasets = {}
    for var in variables:
        file_path = os.path.join(os.environ['INPUT_PATH'], "era5", "hourly", f"{year}_{var}.nc")
        try:
            datasets[var] = xr.open_dataset(file_path).load()
        except Exception as e:
            raise OSError (f"Error loading {file_path}. Caught exception: {e}")

    return (
        datasets["100m_u_component_of_wind"],
        datasets["100m_v_component_of_wind"],
        datasets["forecast_surface_roughness"],
        datasets["surface_pressure"],
        datasets["2m_temperature"],
    )


def load_era5_weather_solar(year):
    """
    Load ERA5 weather and solar datasets for a given year.

    Parameters
    ----------
    year : int
        The year for which to load the datasets.

    Returns
    -------
    tuple
        A tuple containing:
        - xr_time : xarray.DataArray
            The time coordinates.
        - 10m_u_component_of_wind : xarray.Dataset
        - 10m_v_component_of_wind : xarray.Dataset
        - surface_solar_radiation_downwards : xarray.Dataset
        - total_sky_direct_solar_radiation_at_surface : xarray.Dataset
        - surface_pressure : xarray.Dataset
        - 2m_temperature : xarray.Dataset
        - near_ir_albedo_for_diffuse_radiation : xarray.Dataset
    """
    variables = VARIABLES_SOLAR

    datasets = {}

    for var in variables:
        file_path = os.path.join(os.environ['INPUT_PATH'], "era5", "hourly", f"{year}_{var}.nc")
        try:
            datasets[var] = xr.open_dataset(file_path).load()
        except Exception as e:
            raise OSError(f"Error loading {file_path}. Caught exception: {e}")

    xr_time = datasets["10m_u_component_of_wind"]["time"]

    return (
        xr_time,
        datasets["10m_u_component_of_wind"],
        datasets["10m_v_component_of_wind"],
        datasets["surface_solar_radiation_downwards"],
        datasets["total_sky_direct_solar_radiation_at_surface"],
        datasets["surface_pressure"],
        datasets["2m_temperature"],
        datasets["near_ir_albedo_for_diffuse_radiation"],
    )


def slice_xr_data(
    xr: xr.Dataset,
    start_time: str,
    end_time: str,
    latitude: float,
    longitude: float,
    method: str = None,
) -> xr.Dataset:
    """
    Slice xarray data based on time and location.

    Parameters
    ----------
    xr : xr.Dataset
        The xarray dataset to be sliced.
    start_time : str
        The start time for slicing.
    end_time : str
        The end time for slicing.
    latitude : float
        The latitude for slicing.
    longitude : float
        The longitude for slicing.
    method : str, optional
        The method for slicing, by default None.

    Returns
    -------
    xr.Dataset
        The sliced xarray dataset.
    """
    return xr.sel(
        time=slice(start_time, end_time),
        latitude=latitude,
        longitude=longitude,
        method=method,
    )


def slice_weather_wind(
    xr_u: xr.Dataset,
    xr_v: xr.Dataset,
    xr_fsr: xr.Dataset,
    xr_sp: xr.Dataset,
    xr_t2m: xr.Dataset,
    start_time: str,
    end_time: str,
    latitude: float,
    longitude: float,
) -> tuple:
    """
    Slice wind-related weather data from xarray datasets.

    Parameters
    ----------
    xr_u : xr.Dataset
        U-component of wind dataset.
    xr_v : xr.Dataset
        V-component of wind dataset.
    xr_fsr : xr.Dataset
        Forecast surface roughness dataset.
    xr_sp : xr.Dataset
        Surface pressure dataset.
    xr_t2m : xr.Dataset
        2m temperature dataset.
    start_time : str
        The start time for slicing.
    end_time : str
        The end time for slicing.
    latitude : float
        The latitude for slicing.
    longitude : float
        The longitude for slicing.

    Returns
    -------
    tuple
        A tuple of sliced xarray datasets.
    """
    variables = [xr_u, xr_v, xr_fsr, xr_sp, xr_t2m]
    sliced_data = [
        slice_xr_data(
            var,
            start_time=start_time,
            end_time=end_time,
            latitude=latitude,
            longitude=longitude,
        )
        for var in variables
    ]
    return tuple(sliced_data)


def slice_weather_solar(
    xr_u: xr.Dataset,
    xr_v: xr.Dataset,
    xr_ssrd: xr.Dataset,
    xr_fdir: xr.Dataset,
    xr_sp: xr.Dataset,
    xr_t2m: xr.Dataset,
    xr_alnid: xr.Dataset,
    start_time: str,
    end_time: str,
    latitude: float,
    longitude: float,
) -> tuple:
    """
    Slice solar-related weather data from xarray datasets.

    Parameters
    ----------
    xr_u : xr.Dataset
        U-component of wind dataset.
    xr_v : xr.Dataset
        V-component of wind dataset.
    xr_ssrd : xr.Dataset
        Surface solar radiation downwards dataset.
    xr_fdir : xr.Dataset
        Total sky direct solar radiation at surface dataset.
    xr_sp : xr.Dataset
        Surface pressure dataset.
    xr_t2m : xr.Dataset
        2m temperature dataset.
    xr_alnid : xr.Dataset
        Near IR albedo for diffuse radiation dataset.
    start_time : str
        The start time for slicing.
    end_time : str
        The end time for slicing.
    latitude : float
        The latitude for slicing.
    longitude : float
        The longitude for slicing.

    Returns
    -------
    tuple
        A tuple of sliced xarray datasets.
    """
    variables = [xr_u, xr_v, xr_ssrd, xr_fdir, xr_sp, xr_t2m, xr_alnid]
    sliced_data = [
        slice_xr_data(
            var,
            start_time=start_time,
            end_time=end_time,
            latitude=latitude,
            longitude=longitude,
        )
        for var in variables
    ]
    return tuple(sliced_data)


def compute_monthly_statistics(data: np.ndarray, operation: str = None) -> np.ndarray:
    """
    Compute the monthly sums or averages of an array representing hourly data over a year.

    Parameters
    ----------
    data : np.ndarray
        Numpy array of shape (8760,) or (8784,) representing hourly data.
    operation : str
        The operation to perform, either 'sum' or 'mean'. Default is 'sum'.

    Returns
    -------
    np.ndarray
        Numpy array of shape (12,) containing the monthly sums or averages.
    """
    hours_in_months = np.array(
        [
            31 * 24,
            28 * 24,
            31 * 24,
            30 * 24,
            31 * 24,
            30 * 24,
            31 * 24,
            31 * 24,
            30 * 24,
            31 * 24,
            30 * 24,
            31 * 24,
        ]
    )

    # Adjust for leap year if necessary
    if data.size == 8784:
        hours_in_months[1] = 29 * 24

    start_idx = 0
    monthly_statistics = []

    for hours in hours_in_months:
        end_idx = start_idx + hours
        if operation == "sum":
            monthly_statistics.append(np.sum(data[start_idx:end_idx]))
        elif operation == "mean":
            monthly_statistics.append(np.mean(data[start_idx:end_idx]))
        else:
            raise ValueError("Invalid operation. Use 'sum' or 'mean'.")
        start_idx = end_idx

    return np.array(monthly_statistics)


def get_curtailment(curtailment_env_var: str):
    """
    Retrieve and calculate the curtailment value from an environment variable.

    Parameters
    ----------
    curtailment_env_var : str
        The name of the environment variable that stores the curtailment value.

    Returns
    -------
    float or None
        The calculated curtailment value (1 - curtailment) if valid and within range [0, 1].
        Returns None if the value is "None", out of range, or cannot be converted to a float.

    Logs
    ----
    This function logs informational and warning messages based on the status of the curtailment value.
    """

    curtailment_env = os.getenv(curtailment_env_var)

    if curtailment_env == "None":
        logger.info(
            f"{curtailment_env_var} is set to 'None'. No curtailment applied in run"
        )
        return None

    try:
        curtailment = 1 - float(curtailment_env)
        if not (0 <= curtailment <= 1):
            logger.warning(
                f"{curtailment_env_var} value {curtailment_env} is out of range [0, 1]. No curtailment applied in run"
            )
            return None
        logger.info(f"{curtailment_env_var} is set to {curtailment_env}")
        return curtailment

    except ValueError:
        logger.warning(
            f"{curtailment_env_var} could not be converted to float: {curtailment}. No curtailment applied in run"
        )
        return None


def fetch_batch(session, table, query, batch_size, current_offset):
    """
    Fetch a batch of rows from the table based on the query.

    Parameters
    ----------
    session : sqlalchemy.orm.session.Session
        The database session to use for the query.
    table : sqlalchemy.Table
        The table object from which to fetch rows.
    query : sqlalchemy.sql.Select
        The SQL query to execute.
    batch_size : int
        The number of rows to fetch in each batch.
    current_offset : int
        The offset to start fetching rows from.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the fetched batch of rows.
    """

    batch_query = query.limit(batch_size).offset(current_offset)
    result = session.execute(batch_query)
    return pd.DataFrame(result.fetchall(), columns=result.keys())


def export_to_csv(table_name, output_file, export_batch_size=None, mastr_ids=os.getenv('EXPORT_UNITS'), year=None):
    """
    Export data from a database table to a CSV file in batches.

    Parameters
    ----------
    table_name : str
        Name of the database table to query.
    output_file : str
        Name of the output CSV file.
    batch_size : int, optional
        Number of rows to include in each batch. Default is None.
    mastr_ids : list of str, optional
        List of mastr_ids to filter the query by. Default is None.
    years : list of int or str, optional
        List of years to filter the query by. Default is None.

    Returns
    -------
    None
    """
    if mastr_ids.strip().lower() == 'all':
        mastr_ids = None
    else:
        mastr_ids = [unit.strip() for unit in mastr_ids.split(",")]

    with session_scope(engine) as session:
        table = Table(table_name, metadata, autoload_with=engine)
        query = build_query(table, mastr_ids, year)



        current_offset = 0
        while True:
            batch_df = fetch_batch(session, table, query, export_batch_size, current_offset)
            if batch_df.empty and current_offset == 0:
                logger.warning(f"'{table_name}' for {year} - no csv exported. First exported batch was empty. This might be due to: 1. EXPORT_YEAR: {year}, has no calculated results in db. 2. Specified EXPORT_UNITS: {mastr_ids}, are no wind or solar units (depending on intended export).")
                break
            if batch_df.empty:
                logger.info(f"'{table_name}' for {year} exported to csv")
                break
            write_to_csv(batch_df, output_file, current_offset)
            current_offset += export_batch_size


def build_query(table, mastr_ids, years):
    """
    Build the SQL query with optional filtering by mastr_ids and years.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table object from which to select columns.
    mastr_ids : list of str or None
        List of mastr_ids to filter the query. If None, no filtering is applied.
    years : list of int or str or None
        List of years to filter the query. If a single year is provided as a string, it is converted to a list.
        If None, no filtering is applied.

    Returns
    -------
    sqlalchemy.sql.Select
        The constructed SQL query with the applied filters.
    """

    query = select(table.columns)
    if mastr_ids:
        query = query.where(table.c.EinheitMastrNummer.in_(mastr_ids))
    if years:
        if isinstance(years, str):
            years = [years]
        try:
            years = [int(year) for year in years]
        except ValueError as e:
            logger.error(f"Year values must be integers! Cannot query table")
        query = query.where(table.c.year.in_(years))
    return query


def write_to_csv(df, output_file, current_offset):
    """
    Write the DataFrame to a CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to be written to the CSV file.
    output_file : str
        The path of the output CSV file.
    current_offset : int
        The current offset indicating the mode of writing. If 0, write in 'w' mode with header;
        otherwise, append to the file without writing the header.

    Returns
    -------
    None
    """

    """Write the DataFrame to a CSV file."""
    mode = "w" if current_offset == 0 else "a"
    header = current_offset == 0
    df.to_csv(output_file, index=False, mode=mode, header=header)


def create_results_dict(mastrid, year, power, cf, key_prefix: str, no_calc_reason:str = None):
    """
    Create a dictionary containing results data for a specified year and power metrics.

    Parameters
    ----------
    mastrid : str
        The unique identifier for the unit.
    year : int
        The year for which the results are being recorded.
    power : float
        The power value for the specified year and unit.
    cf : float
        The capacity factor for the specified year and unit.
    key_prefix : str
        Prefix for the keys related to energy and capacity factor.

    Returns
    -------
    dict
        Dictionary containing the results data with appropriate keys and values.
    """

    return {
        "EinheitMastrNummer": mastrid,
        "year": year,
        f"energy_{key_prefix}": power,
        f"cf_{key_prefix}": cf,
        "software_version": os.getenv("SOFTWARE_VERSION"),
        "outfile_postfix": os.getenv("OUTFILE_POSTFIX"),
        "no_calc_reason": no_calc_reason,
    }


def export_and_copy_files(years, export_batch_size, tech):
    """
    Export CSV files for specified years and periods, and copy configuration and log files.

    Parameters
    ----------
    years : list of int
        List of years for which data will be exported.
    export_batch_size : int
        The size of each batch of data to be exported.
    tech : str
        Technology type ('wind' or 'solar') for which data will be exported and files copied.

    Returns
    -------
    None
    """

    outpath_dict = {
        "wind": os.path.join(os.environ['OUTPUT_PATH'], "wind"),
        "solar": os.path.join(os.environ['OUTPUT_PATH'], "solar"),
    }

    # Ensure each directory exists
    for key, path in outpath_dict.items():
        os.makedirs(path, exist_ok=True)

    table_prefix = f"results_{tech}"

    # Export to CSV
    for year in years:
        for period in ["monthly", "yearly"]:
            export_to_csv(
                table_name=f"{table_prefix}_{period}",
                output_file=f"{outpath_dict[tech]}/{tech}_{period}_{year}_{os.getenv('SOFTWARE_VERSION')}_{os.getenv('OUTFILE_POSTFIX')}.csv",
                export_batch_size=export_batch_size,
                year=[year],
            )

    logger.info(f"Save config.yaml and log to: {outpath_dict[tech]}")
    # Copy config.yaml and log file to the tech directory
    shutil.copy(os.path.join(os.environ['CONFIG_PATH']), os.path.join(outpath_dict[tech], f"config_{os.getenv('SOFTWARE_VERSION')}_{os.getenv('OUTFILE_POSTFIX')}.yaml"))
    shutil.copy(
        os.path.join(
            os.environ['REPO_ROOT'],
            "logs",
            f"kfw-mastr_{os.getenv('RUN_ID')}.log",
        ),
        os.path.join(outpath_dict[tech], f"kfw-mastr_{os.getenv('SOFTWARE_VERSION')}_{os.getenv('OUTFILE_POSTFIX')}.log"),
    )

    logger.info(f"Saved csv's, config.yaml and log file to: {outpath_dict[tech]}")
