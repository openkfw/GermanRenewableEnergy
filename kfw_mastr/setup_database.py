import os
import urllib.request as request

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from open_mastr import Mastr
from scipy.spatial import cKDTree
from shapely.geometry import Point
from sqlalchemy import func, select, text
from tqdm import tqdm

from pvlib.solarposition import get_solarposition
from pvlib.irradiance import aoi

from kfw_mastr.utils.config import setup_logger, get_engine, session_scope, create_directories
from kfw_mastr.utils.constants import (
    AVERAGE_HUB_HEIGHT_MASTR,
    UPDATE_LOG,
    MASTR_TURBINE_MAP,
    AZIMUTH_ANGLE_MAP,
    TILT_ANGLE_MAP, LEAP_YEARS,
)
from kfw_mastr.utils.helpers import (
    get_table_columns_as_dataframe,
)
from kfw_mastr.utils.orm import *
from kfw_mastr.utils.session_funcs import batch_update, query_table

# Setup logger and engine
logger = setup_logger()
engine, metadata = get_engine()


def setup_db_and_download_mastr_and_regions():
    """
    Download the Marktstammdatenregister and data about German administrative areas.
    Then, save it to the database created by docker-compose.

    This function performs the following steps:
    1. Downloads the Marktstammdatenregister data and saves it to the database.
    2. Downloads and saves the geospatial boundaries for districts to the database.
    3. Downloads and saves the geospatial boundaries for municipalities to the database.

    Returns
    -------
    None
    """

    download_mastr(engine)
    download_districts_geoboundaries(engine=engine)
    download_municipalities_geoboundaries(engine=engine)


def download_mastr(engine):
    """
    Download the Marktstammdatenregister data and save it to the database.

    This function initializes a Mastr database object and downloads the
    Marktstammdatenregister data for the current date, saving it to the
    specified database.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for the database connection.

    Returns
    -------
    None
    """

    db = Mastr(engine=engine)
    db.download(date="today")


def download_districts_geoboundaries(engine) -> gpd.GeoDataFrame:
    """
    Download and load geospatial boundaries data for districts into the database.

    This function downloads a ZIP file containing district boundaries data from
    a specified URL, extracts the relevant file, reads it into a GeoDataFrame,
    reprojects it to EPSG:4326, and uploads it to a PostgreSQL/PostGIS database.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for the database connection.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the district geospatial boundaries data.
    """

    constants = {
        "url": "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip",
        "table_name": "districts_geoboundaries",
        "download_path": os.path.join(os.environ["REPO_ROOT"], os.environ["INPUT_PATH"], "ags"),
        "zipfile_path": "vg5000_12-31.utm32s.shape.ebenen/vg5000_ebenen_1231/VG5000_KRS.shp",
        "filename": "vg5000_1231.zip",
    }
    load_geoboundaries(constants, engine)


def download_municipalities_geoboundaries(engine) -> gpd.GeoDataFrame:
    """
    Download and load geospatial boundaries data for municipalities into the database.

    This function downloads a ZIP file containing municipality boundaries data
    from a specified URL, extracts the relevant file, reads it into a GeoDataFrame,
    reprojects it to EPSG:4326, and uploads it to a PostgreSQL/PostGIS database.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for the database connection.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the municipality geospatial boundaries data.
    """

    constants = {
        "url": "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip",
        "table_name": "municipalities_geoboundaries",
        "download_path": os.path.join(os.environ["REPO_ROOT"], os.environ["INPUT_PATH"], "ags"),
        "zipfile_path": "vg5000_12-31.utm32s.shape.ebenen/vg5000_ebenen_1231/VG5000_GEM.shp",
        "filename": "vg5000_1231.zip",
    }
    load_geoboundaries(constants, engine)


def download_from_url(
    url: str, save_directory: str, filename: str, overwrite: bool = False
) -> None:
    """
    Download a file from a given URL and save it to the specified path.

    Parameters
    ----------
    url : str
        URL to download from.
    save_directory : str
        Folder path where the file will be saved.
    filename : str
        Name of the file.
    overwrite : bool, optional
        Whether to overwrite the file if it already exists (default is False).

    Returns
    -------
    None
    """

    save_path = os.path.join(save_directory, filename)

    if not overwrite and os.path.isfile(save_path):
        logger.info(f"File {filename} already downloaded")
        return None
    create_directories([save_directory])
    logger.info(url, save_path)
    request.urlretrieve(url, save_path)


def load_geoboundaries(constants, engine) -> gpd.GeoDataFrame:
    """
    Download and load geospatial boundaries data into a database.

    This function downloads a ZIP file containing geospatial boundaries data
    from a specified URL, extracts the relevant file, reads it into a GeoDataFrame,
    reprojects it to EPSG:4326, and uploads it to a PostgreSQL/PostGIS database.

    Parameters
    ----------
    constants : dict
        Dictionary containing configuration constants:
        - "download_path" : str
            Directory where the file will be downloaded.
        - "filename" : str
            Name of the downloaded ZIP file.
        - "zipfile_path" : str
            Path within the ZIP file to the desired file.
        - "url" : str
            URL from which to download the ZIP file.
        - "table_name" : str
            Name of the table to create in the database.
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for the database connection.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the geospatial boundaries data.
    """

    download_path = constants["download_path"]
    filename = constants["filename"]
    zipfile_path = constants["zipfile_path"]
    download_from_url(
        url=constants["url"],
        save_directory=download_path,
        filename=filename,
    )
    zipfile = f"{download_path}/{filename}!{zipfile_path}".replace("\\", "/").replace(
        "C:/", "zip:///"
    )
    gdf = gpd.read_file(zipfile)
    gdf.to_crs(crs="EPSG:4326", inplace=True)
    gdf["geometry"] = gdf["geometry"]
    gdf.to_postgis(name=constants["table_name"], con=engine, if_exists="replace")
    logger.info("Geo-boundaries loaded to database")


def create_tables():
    """
    Create tables for calculations.

    This function creates the necessary tables for calculations in the database
    using the defined SQLAlchemy models.

    Returns
    -------
    None
    """

    with session_scope(engine=engine) as session:
        # Create tables
        Base.metadata.create_all(engine, checkfirst=True)

    metadata.reflect(engine)
    logger.info(f"Created tables: {list(Base.metadata.tables.keys())}")


def load_unique_era5_coordinates_db(engine, path):
    """
    Load unique ERA5 coordinates into the database.

    This function reads unique ERA5 coordinates from a NetCDF file and
    loads them into a specified database table.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        SQLAlchemy engine object for the database connection.
    path : str
        Path to the NetCDF file containing ERA5 coordinates.

    Returns
    -------
    None
    """
    logger.info("Load unique era5 coordinates to db")
    df = xr.open_dataset(path).to_dataframe().reset_index()
    df = df[["latitude", "longitude"]].drop_duplicates().reset_index(drop=True)
    df.index.name = "coordinate_id"
    df.to_sql(
        "unique_era5_coordinates",
        engine,
        if_exists="replace",
        index=True,
        method="multi",
        chunksize=2000,
    )


def insert_into_existing_table(
    table_name_to_update,
    existing_table,
    columns_to_update,
    schema="public",
    chunksize: int = 10000,
    limit: int = None,
):
    """
    Insert data into an existing table in the database.

    This function retrieves data from an existing table, and inserts specified
    columns into another table, updating the database in chunks.

    Parameters
    ----------
    table_name_to_update : str
        Name of the table to be updated.
    existing_table : str
        Name of the existing table from which data will be retrieved.
    columns_to_update : list of str
        List of columns to be inserted into the target table.
    schema : str, optional
        Schema of the database tables (default is 'public').
    chunksize : int, optional
        Number of rows to process in each batch (default is 10000).
    limit : int, optional
        Limit the number of rows to retrieve from the existing table (default is None).

    Returns
    -------
    None
    """

    df_cols = get_table_columns_as_dataframe(
        existing_table, columns_to_update, limit=limit
    )

    # Insert the DataFrame into the target table in chunks
    num_rows = len(df_cols)
    logger.info(
        f"Table: {table_name_to_update} is updated. Columns: {columns_to_update} are inserted from {existing_table} into {table_name_to_update}"
    )
    with engine.begin() as connection:
        with tqdm(
            total=num_rows, desc="Inserting data to existing db", unit="rows"
        ) as pbar:
            for start in range(0, num_rows, chunksize):
                end = start + chunksize
                chunk = df_cols.iloc[start:end]
                try:
                    chunk.to_sql(
                        table_name_to_update,
                        con=connection,
                        schema=schema,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                    pbar.update(len(chunk))
                except:
                    print(f"Mastr unit already in table")
                    continue


def get_nearest_era5_coordinate(lat, lon):
    """
    Find the nearest coordinate from unique_era5_coordinates based on latitude and longitude using PostGIS.

    Parameters
    ----------
    lat : float
        Latitude of the point.
    lon : float
        Longitude of the point.

    Returns
    -------
    tuple
        Latitude and longitude of the nearest ERA5 coordinate.
    """
    with session_scope(engine=engine) as session:
        # Create a geographic point from the input latitude and longitude
        point = func.ST_SetSRID(func.ST_MakePoint(lat, lon), 4326)

        query = (
            session.query(
                unique_era5_coordinates.latitude, unique_era5_coordinates.longitude
            )
            .order_by(
                func.ST_Distance(
                    point,
                    func.ST_SetSRID(
                        func.ST_MakePoint(
                            unique_era5_coordinates.latitude,
                            unique_era5_coordinates.longitude,
                        ),
                        4326,
                    ),
                )
            )
            .limit(1)
        )  # Add limit to ensure only the nearest coordinate is returned

        result = query.first()
        if result:
            return result.latitude, result.longitude


def ckdnearest(gdA, gdB):
    """
    Find the nearest point in gdB for each point in gdA.

    This function finds the point in gdB with the closest distance to each point in gdA.

    Parameters
    ----------
    gdA : GeoDataFrame
        GeoDataFrame with points to find nearest neighbors for.
    gdB : GeoDataFrame
        GeoDataFrame with potential nearest neighbor points.

    Returns
    -------
    GeoDataFrame
        GeoDataFrame with nearest points and distances.
    """
    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB_nearest, pd.Series(dist, name="dist")], axis=1
    )
    return gdf[["AGS", "centroid_lon", "centroid_lat", "latitude", "longitude"]]


def update_municipalities_centroids():
    """
    Update the centroids of municipalities in the 'municipalities_geoboundaries' table.

    This function ensures that the 'centroid_lat' and 'centroid_lon' columns exist in the
    'municipalities_geoboundaries' table. If they do not exist, they are added. It then
    calculates and updates the centroid latitude and longitude for each municipality
    based on the 'geometry' column.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine to use for database connection.

    Returns
    -------
    None
    """
    metadata.reflect(engine)
    municipalities_table = metadata.tables["municipalities_geoboundaries"]

    with session_scope(engine) as session:
        # Ensure the centroid columns exist (you might need to add them if not already present)
        alter_table_query = text(
            f"""
            ALTER TABLE {municipalities_table.name}
            ADD COLUMN IF NOT EXISTS centroid_lat DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS centroid_lon DOUBLE PRECISION;
        """
        )
        session.execute(alter_table_query)

        # Use raw SQL for updating centroids
        update_query = text(
            f"""
        UPDATE {municipalities_table.name}
        SET 
            centroid_lat = ST_Y(ST_Centroid(geometry)),
            centroid_lon = ST_X(ST_Centroid(geometry));
        """
        )

        session.execute(update_query)
        session.commit()
        logger.info("Muncipality centroids calculated")


def update_coordinates(tables: list, chunk_size=1000000):
    """
    Update the coordinates in the specified tables with the nearest ERA5 location points.

    Parameters
    ----------
    tables : list
        List of tables to be updated.
    chunk_size : int, optional
        Number of rows to process in each batch (default is 1000000).

    Returns
    -------
    None
    """
    # Clear and reflect metadata to ensure previously created columns can be accessed
    metadata.clear()
    metadata.reflect(engine)

    municipalities_table = metadata.tables["municipalities_geoboundaries"]
    era5_coordinates_table = metadata.tables["unique_era5_coordinates"]

    with session_scope(engine) as session:
        ags_data = session.execute(
            select(
                municipalities_table.c.AGS,
                municipalities_table.c.centroid_lon,
                municipalities_table.c.centroid_lat,
            )
        ).fetchall()

        df_municipalities = pd.DataFrame(
            ags_data, columns=["AGS", "centroid_lon", "centroid_lat"]
        )
        df_municipalities["geometry"] = df_municipalities.apply(
            lambda row: Point(row["centroid_lon"], row["centroid_lat"]), axis=1
        )
        gdA = gpd.GeoDataFrame(df_municipalities, geometry="geometry")

        era5_result = session.execute(
            select(
                era5_coordinates_table.c.latitude,
                era5_coordinates_table.c.longitude,
            )
        ).fetchall()
        unique_era5_coords = pd.DataFrame(
            era5_result, columns=["latitude", "longitude"]
        )
        unique_era5_coords["geometry"] = unique_era5_coords.apply(
            lambda row: Point(row["longitude"], row["latitude"]), axis=1
        )
        gdB = gpd.GeoDataFrame(unique_era5_coords, geometry="geometry")

        nearest_df = ckdnearest(gdA, gdB)

        # Rename columns
        nearest_df.rename(
            columns={
                "AGS": "Gemeindeschluessel",
                "centroid_lon": "ags_lon",
                "centroid_lat": "ags_lat",
                "latitude": "era5_ags_lat",
                "longitude": "era5_ags_lon",
            },
            inplace=True,
        )

        total_rows = len(nearest_df)

        for table in tables:
            for i in tqdm(
                range(0, total_rows, chunk_size),  # Process in chunks of 1,000,000 rows
                desc=f"Update {table.__tablename__} table with the "
                f"nearest coordinates of ERA5 location points",
            ):
                chunk = nearest_df.iloc[i : i + chunk_size]

                update_data = chunk.to_dict(orient="records")

                batch_update(
                    session,
                    table=table,
                    updates=update_data,
                    id_cols=["Gemeindeschluessel"],
                    conflict_action="join",
                )


def map_turbine(Typenbezeichnung, Nettonennleistung):
    """
    Map turbine type based on Typenbezeichnung and Nettonennleistung.

    This function maps the type designation of a turbine to a predefined turbine type
    based on the closest matching net capacity.

    Parameters
    ----------
    Typenbezeichnung : str
        Type designation of the turbine.
    Nettonennleistung : float
        Net capacity of the turbine.

    Returns
    -------
    tuple
        Mapped turbine type (str) and error log (str or None).
    """
    turbine_mapped = "default100_6/3360"
    turbine_error = None

    found = False
    closest_power_diff = float("inf")

    if Typenbezeichnung:
        for manufacturer, turbines in MASTR_TURBINE_MAP.items():
            for turbine_type, power_map in turbines.items():
                for power, mastr_type in power_map.items():
                    if Typenbezeichnung in mastr_type:
                        found = True
                        power_diff = abs(int(power) - Nettonennleistung)
                        if power_diff < closest_power_diff:
                            closest_power_diff = power_diff
                            turbine_mapped = turbine_type

    else:
        turbine_error = UPDATE_LOG["tte"]

    if not found:
        turbine_error = UPDATE_LOG["ttnme"]

    return turbine_mapped, turbine_error


def map_hub_height(Nabenhoehe: float) -> tuple[float, str | None]:
    """
    Map the hub height of a turbine.

    This function maps the hub height to a default value if it is not provided.

    Parameters
    ----------
    Nabenhoehe : float
        Hub height of the turbine.

    Returns
    -------
    tuple
        Mapped hub height (float) and error log (str or None).
    """

    if Nabenhoehe is None:
        hub_height_mapped = AVERAGE_HUB_HEIGHT_MASTR
        log = UPDATE_LOG["hhe"]
    else:
        hub_height_mapped = Nabenhoehe
        log = None
    return hub_height_mapped, log


def update_turbine_type_and_hub_height(batch_size: int = 2000, query_limit: int = None):
    """
    Batch-map turbine types and hub heights into the Calculation_wind table.

    This function retrieves turbine data, maps the turbine type and hub height, and
    updates the Calculation_wind table in batches.

    Parameters
    ----------
    batch_size : int, optional
        Number of rows to process in each batch (default is 2000).
    query_limit : int, optional
        Limit the number of rows to retrieve from the table (default is None).

    Returns
    -------
    None
    """
    logger.info(
        f"Batch-Mapping turbine types and hub heights into table: {Calculation_wind.__tablename__}. Batch size: {batch_size}"
    )
    with session_scope(engine=engine) as session:
        wc_query = query_table(
            session,
            table=Calculation_wind,
            column_names=[
                "Nabenhoehe",
                "Nettonennleistung",
                "Typenbezeichnung",
                "EinheitMastrNummer",
            ],
            limit=query_limit,
        )

        updates = []
        for row in tqdm(
            wc_query,
            desc="Updating turbine types and hub heights. Committing do db in chunks",
        ):
            tt_mapped, tt_log = map_turbine(row.Typenbezeichnung, row.Nettonennleistung)
            hh_mapped, hh_log = map_hub_height(row.Nabenhoehe)

            mapping_log = (tt_log or "") + (hh_log or "")
            updates.append(
                {
                    "EinheitMastrNummer": row.EinheitMastrNummer,
                    "turbine_mapped": tt_mapped,
                    "hub_height_mapped": hh_mapped,
                    "mapping_log": mapping_log,
                }
            )

            if len(updates) >= batch_size:
                batch_update(
                    session,
                    table=Calculation_wind,
                    updates=updates,
                    id_cols=["EinheitMastrNummer"],
                    conflict_action="update",
                )
                updates.clear()

        if updates:
            batch_update(
                session,
                table=Calculation_wind,
                updates=updates,
                id_cols=["EinheitMastrNummer"],
                conflict_action="update",
            )

        logger.info("Mapped turbine types and hub heights")


def update_azimuth_and_tilt_angle(batch_size: int = 2000, query_limit: int = None):
    """
    Batch-map azimuth and tilt angles into the Calculation_solar table.

    This function retrieves solar data, maps the azimuth and tilt angles, and
    updates the Calculation_solar table in batches.

    Parameters
    ----------
    batch_size : int, optional
        Number of rows to process in each batch (default is 2000).
    query_limit : int, optional
        Limit the number of rows to retrieve from the table (default is None).

    Returns
    -------
    None
    """
    logger.info(
        f"Batch-Mapping azimuth and tilt angles into table: {Calculation_solar.__tablename__}. Batch size:"
        f" {batch_size}"
    )
    with session_scope(engine=engine) as session:

        sc_query = query_table(
            session,
            table=Calculation_solar,
            column_names=[
                "Hauptausrichtung",
                "HauptausrichtungNeigungswinkel",
                "EinheitMastrNummer",
            ],
            limit=query_limit,
        )

        updates = []
        for row in tqdm(
            sc_query,
            desc="Updating azimuth and tilt angles. Committing do db in chunks",
        ):
            aa_mapped, aa_log = map_azimuth_angle(row.Hauptausrichtung)
            ta_mapped, ta_log = map_tilt_angle(row.HauptausrichtungNeigungswinkel)

            # Concatenate and log errors
            mapping_log = (aa_log or "") + (ta_log or "")
            updates.append(
                {
                    "EinheitMastrNummer": row.EinheitMastrNummer,
                    "azimuth_angle_mapped": aa_mapped,
                    "tilt_angle_mapped": ta_mapped,
                    "mapping_log": mapping_log,
                }
            )

            if len(updates) >= batch_size:
                batch_update(
                    session,
                    table=Calculation_solar,
                    updates=updates,
                    id_cols=["EinheitMastrNummer"],
                    conflict_action="update",
                )
                updates.clear()

        if updates:
            batch_update(
                session,
                table=Calculation_solar,
                updates=updates,
                id_cols=["EinheitMastrNummer"],
                conflict_action="update",
            )

        logger.info("Mapped azimuth and tilt angles")


def map_azimuth_angle(hauptausrichtung):
    """
    Map the azimuth angle based on the Hauptausrichtung value.

    This function maps the Hauptausrichtung value to a predefined azimuth angle.
    If the Hauptausrichtung value is not found in the map, a default value is used.

    Parameters
    ----------
    hauptausrichtung : str
        Hauptausrichtung value to be mapped.

    Returns
    -------
    tuple
        Mapped azimuth angle (float) and error log (str or None).
    """
    mapped_angle = AZIMUTH_ANGLE_MAP.get(hauptausrichtung)
    if mapped_angle is not None:
        return mapped_angle, None
    else:
        mapped_angle = AZIMUTH_ANGLE_MAP["default"]
        log = UPDATE_LOG["aa"]
        return mapped_angle, log


def map_tilt_angle(hauptausrichtung_neigungswinkel):
    """
    Map the tilt angle based on the HauptausrichtungNeigungswinkel value.

    This function maps the HauptausrichtungNeigungswinkel value to a predefined tilt angle.
    If the HauptausrichtungNeigungswinkel value is not found in the map, a default value is used.

    Parameters
    ----------
    hauptausrichtung_neigungswinkel : float
        HauptausrichtungNeigungswinkel value to be mapped.

    Returns
    -------
    tuple
        Mapped tilt angle (float) and error log (str or None).
    """
    mapped_angle = TILT_ANGLE_MAP.get(hauptausrichtung_neigungswinkel)
    if mapped_angle is not None:
        return mapped_angle, None
    else:
        mapped_angle = TILT_ANGLE_MAP["default"]
        log = UPDATE_LOG["ta"]
        return mapped_angle, log


def calculate_solar_angles():
    """
    Calculate solar angles for a set of ERA5 coordinates and update the database.

    This function retrieves a set of coordinates from ERA5 data, generates a time index for a specified year,
    and calculates solar zenith and azimuth angles for each coordinate at each time step. The results are then
    updated in the `Calculation_solar_angles` table in the database.

    Raises
    ------
    ValueError
        If the dimensions of latitude and longitude from the ERA5 data are unexpected.

    Notes
    -----
    The following environment variables are expected:
    - `YEAR_SOLAR_ANGLE`: The year for which to calculate the solar angles.
    - `INPUT_PATH`: The base input path to the ERA5 data to get coordinates.
    - `NUMBER_THREADS`: The number of threads to use for solar position calculations.

    The ERA5 data file should be located at `{INPUT_PATH}/era5/hourly/2000_2m_temperature.nc`.

    This function uses the `get_solarposition` function from the `pvlib` library with the `nrel_numba` method.

    Uses a context manager `session_scope` for database transactions and a `batch_update` function to update
    the `Calculation_solar_angles` table.

    Progress of the solar position calculations is displayed using `tqdm`.
    """

    # get timeindex
    year = os.getenv('YEAR_SOLAR_ANGLE')
    logger.info(f"Computing solar positions for year: {year}")

    start_date = f'{year}-01-01 00:00:00'
    end_date = f'{year}-12-31 23:00:00'
    # Generate the hourly time index for the leap year
    time_index = pd.date_range(start=start_date, end=end_date, freq='h')
    
    # get set of era5 coordinates
    file_path = os.path.join(os.environ['INPUT_PATH'], "era5", "hourly", f"2000_2m_temperature.nc")
    coordinates = xr.open_dataset(file_path)
    latitude = coordinates['latitude']
    longitude = coordinates['longitude']

    # Build mesh 
    if latitude.ndim == 1 and longitude.ndim == 1:
        # If 1D, use meshgrid to create a grid of coordinates
        lon_grid, lat_grid = xr.broadcast(longitude, latitude)
        coordinates = xr.Dataset({'era5_lat': lat_grid, 'era5_lon': lon_grid})
    else:
        raise ValueError("Unexpected dimensions for lat and lon")

    # Flatten and combine the lat and lon into pairs, find unique pairs, 
    unique_coordinates = coordinates.to_dataframe().reset_index()[['era5_lat', 'era5_lon']].drop_duplicates()
    unique_coordinates['lat_lon'] = list(zip(unique_coordinates['era5_lat'], unique_coordinates['era5_lon']))

    def compute_solar_positions(row):
        solar_position = get_solarposition(
            time_index,
            row['era5_lat'],
            row['era5_lon'],
            method="nrel_numba",
            numthreads=int(os.getenv('NUMBER_THREADS'))
        )

        row['solar_zenith'] = solar_position['zenith'].values.tolist()
        row['solar_azimuth'] = solar_position['azimuth'].values.tolist()

        return row

    # Apply the function to each row
    tqdm.pandas(desc="Computing solar positions for each ERA5 coordinate")
    unique_coordinates = unique_coordinates.progress_apply(compute_solar_positions, axis=1)

    # if selected solar angles year is not a leap year, an artificial 29th February will be created
    # e.g so that with a solar angles year 2001 (non-leap year) capacity factors for leap year 2024 can be calculated
    def duplicate_feb_28(row):
        # Convert lists to Series for easier manipulation
        solar_zenith_series = pd.Series(row['solar_zenith'], index=time_index)
        solar_azimuth_series = pd.Series(row['solar_azimuth'], index=time_index)

        # Identify the 28th of February
        feb_28th_mask = (solar_zenith_series.index.month == 2) & (solar_zenith_series.index.day == 28)
        feb_28th_zenith = solar_zenith_series[feb_28th_mask]
        feb_28th_azimuth = solar_azimuth_series[feb_28th_mask]

        # Create the 29th February dates by copying the 28th February dates
        feb_29th_index = feb_28th_zenith.index + pd.DateOffset(days=1)
        feb_29th_zenith = pd.Series(feb_28th_zenith.values, index=feb_29th_index)
        feb_29th_azimuth = pd.Series(feb_28th_azimuth.values, index=feb_29th_index)

        # Combine the original Series with the artificial 29th February Series directly
        solar_zenith_updated = pd.concat([solar_zenith_series.loc[:feb_28th_zenith.index[-1]], feb_29th_zenith,
                                          solar_zenith_series.loc[
                                          feb_28th_zenith.index[-1] + pd.Timedelta(hours=1):]])
        solar_azimuth_updated = pd.concat([solar_azimuth_series.loc[:feb_28th_azimuth.index[-1]], feb_29th_azimuth,
                                           solar_azimuth_series.loc[
                                           feb_28th_azimuth.index[-1] + pd.Timedelta(hours=1):]])

        # Update the row with the new values
        row['solar_zenith'] = solar_zenith_updated.values.tolist()
        row['solar_azimuth'] = solar_azimuth_updated.values.tolist()

        return row

    if int(year) not in LEAP_YEARS:
        tqdm.pandas(desc=f"Year: {year} is not a leap year. Duplicating February 28th as February 29th for each row")
        unique_coordinates = unique_coordinates.progress_apply(duplicate_feb_28, axis=1)


    # add year column to enable multiple solar angles years in Calculation_solar_angles table
    unique_coordinates["year"] = year
    updates = unique_coordinates.to_dict(orient='records')

    logger.info("Writing solar angles to database")
    with session_scope(engine=engine) as session:
        batch_update(
            session,
            table=Calculation_solar_angles,
            updates=updates,
            id_cols=["era5_lat", "era5_lon", "year"],
            conflict_action="update",
        )
        updates.clear()

def main():
    """
    Main function to set up the database, download necessary data, and prepare tables.

    This function performs the following steps:
    1. Sets up the database and downloads Marktstammdatenregister and regional data.
    2. Creates the necessary tables for calculations.
    3. Loads unique ERA5 coordinates into the database.
    4. Prepares the Calculation_wind table by inserting data from the wind_extended table.
    5. Prepares the Calculation_solar table by inserting data from the solar_extended table.
    6. Updates the centroids of municipalities.
    7. Updates the coordinates in the specified tables with the nearest ERA5 location points.
    8. Maps turbine types and hub heights into the Calculation_wind table.
    9. Maps azimuth and tilt angles into the Calculation_solar table.
    10. Pre-calculate solar angles to speed up calculation process

    Returns
    -------
    None
    """
    setup_db_and_download_mastr_and_regions()
    create_tables()
    load_unique_era5_coordinates_db(
        engine, os.path.join(os.environ["REPO_ROOT"], "input/era5/monthly/2000_all.nc")
    )
    # Prepare Calculation_wind table
    insert_into_existing_table(
        "Calculation_wind",
        "wind_extended",
        [
            "EinheitMastrNummer",
            "EinheitBetriebsstatus",
            "Nettonennleistung",
            "Nabenhoehe",
            "Typenbezeichnung",
            "Laengengrad",
            "Breitengrad",
            "Postleitzahl",
            "Gemeindeschluessel",
            "Inbetriebnahmedatum",
        ],
    )

    # Prepare Calculation_solar table
    insert_into_existing_table(
        "Calculation_solar",
        "solar_extended",
        [
            "EinheitMastrNummer",
            "EinheitBetriebsstatus",
            "Nettonennleistung",
            "Hauptausrichtung",
            "HauptausrichtungNeigungswinkel",
            "Laengengrad",
            "Breitengrad",
            "Postleitzahl",
            "Gemeindeschluessel",
            "Inbetriebnahmedatum",
        ],
    )
    update_municipalities_centroids()
    update_coordinates(
        tables=[Calculation_wind, Calculation_solar]
    )  # Calculation_solar
    update_turbine_type_and_hub_height()
    update_azimuth_and_tilt_angle()
    calculate_solar_angles()

if __name__ == "__main__":
    # prepare database for calculations
    main()
