import os
import time
from typing import Dict

import numpy as np
from tqdm import tqdm

from kfw_mastr.solar import (
    load_calculation_solar_data,
    solar_calculations,
    calc_capacity_factor,
    load_calculation_solar_data_angles,
)
from kfw_mastr.utils.config import get_engine, setup_logger
from kfw_mastr.utils.constants import LEAP_YEARS
from kfw_mastr.utils.helpers import load_era5_weather_solar, get_curtailment, create_results_dict
from kfw_mastr.utils.session_funcs import save_and_commit
from kfw_mastr.setup_database import get_nearest_era5_coordinate

# configs
logger = setup_logger()
engine, metadata = get_engine()


# ERA5 data docs
# https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation


def calculate_cf_solar(
    years: list[str],
    batch_size: int,
    conflict_action: str = "update",
    limit_mastr_units: int = None,
    incremental: bool = False,
):
    """
    Calculate capacity factors for solar energy units over specified years.

    This function loads weather data, solar unit data, and performs capacity factor
    calculations for solar energy units, updating the results in batches.

    Parameters
    ----------
    years : list of str
        List of years for which to calculate capacity factors.
    batch_size : int
        Size of the data batches for committing results to the database.
    conflict_action : str, optional
        Action to take in case of database conflicts (default is 'update').
    limit_mastr_units : int, optional
        Limit on the number of units to process (default is None).

    Returns
    -------
    None
    """

    # Load all solar units and their "Stammdaten"
    logger.info(f"Loading {limit_mastr_units} solar MaStR units")
    mastr_unit_list_of_tuples, session = load_calculation_solar_data(
            query_limit = limit_mastr_units,
            incremental = incremental,
            years = years
        )

    logger.info(f"Loading pre-calculated solar angles from solar angles year: {os.getenv('YEAR_SOLAR_ANGLE')}")
    # load solar angles
    solar_angles_dict = load_calculation_solar_data_angles()

    # Get optional curtailment
    curtailment = get_curtailment("CURTAILMENT_SOLAR")

    # Load era5 data for each year
    for year in years:
        start_c_time = time.time()
        logger.info(f"Load weather year: {year}. Batch-size: {batch_size}")
        start_time = f"{year}"
        end_time = f"{year}"

        # Static load era5 data for the specified year
        xr_time, xr_u, xr_v, xr_ssrd, xr_fdir, xr_sp, xr_t2m, xr_alnid = (
            load_era5_weather_solar(year=year)
        )

        # if year is not leap year, drop 29 February of solar angles (0-based index). Solar angles are read from db table "Calculation_solar_angles", calculated for leap year 2000 with 8784 timesteps
        if int(year) not in LEAP_YEARS:
            logger.info(
                f"Calculating non-leap year: {year}. Dropping 29th February of pre-calculated solar angles year: {os.getenv('YEAR_SOLAR_ANGLE')}"
            )
            feb_29_positions = range(24 * 59, 24 * 60)
            mask = np.ones(8784, dtype=bool)
            mask[list(feb_29_positions)] = False
            for _, series in solar_angles_dict.items():
                series["solar_zenith"] = series["solar_zenith"][mask]
                series["solar_azimuth"] = series["solar_azimuth"][mask]

        updates_hourly = []
        updates_monthly = []
        updates_yearly = []
        
        def update_db(batch_size):
          # update the result database tables 
          for name, arr in [ ("updates_hourly", updates_hourly),
                             ("updates_monthly", updates_monthly),
                             ("updates_yearly", updates_yearly) ]:
             if arr:
               save_and_commit(
                  session,
                  arr,
                  conflict_action,
                  name,
                  batch_size=batch_size,
                  table_id="solar",
               )

        for unit in tqdm(
            mastr_unit_list_of_tuples,
            desc=f"Calculations for mastr solar unit. Committing to db in batches of {batch_size}",
        ):

            mastrid = unit[0]
            lat = unit[6]
            lon = unit[7]
            era5_ags_lat = unit[4]
            era5_ags_lon = unit[5]

            # Handle if AGS coordinates are empty
            if era5_ags_lat is None or era5_ags_lon is None:
                era5_ags_lat, era5_ags_lon = get_nearest_era5_coordinate(lat, lon)
                if lat is None or lon is None:
                    no_calc_reason = "Missing AGS or other coordinates"
                    if os.getenv('SAVE_HOURLY_DATA') == 'True':
                        updates_hourly.append(create_results_dict(mastrid, year, [0], [0], "h", no_calc_reason))
                    updates_monthly.append(
                        create_results_dict(mastrid, year, [0], [0], "m", no_calc_reason))
                    updates_yearly.append(
                        create_results_dict(mastrid, year, 0, 0, "y", no_calc_reason))
                    continue

            # get solar angles
            lat_lon_to_match = f"({era5_ags_lat},{era5_ags_lon})"
            solar_angles_unit: Dict = solar_angles_dict.get(lat_lon_to_match)

            # Power for one PV module
            power = solar_calculations(
                unit,
                xr_time,
                xr_u,
                xr_v,
                xr_ssrd,
                xr_fdir,
                xr_sp,
                xr_t2m,
                xr_alnid,
                start_time=start_time,
                end_time=end_time,
                era5_ags_lat=era5_ags_lat,
                era5_ags_lon=era5_ags_lon,
                solar_zenith=solar_angles_unit["solar_zenith"],
                solar_azimuth=solar_angles_unit["solar_azimuth"],
            )

            # Scales power for one PV module with Nettonennleistung of mastr solar unit and calculates capacity factor
            updates_hourly, updates_monthly, updates_yearly = calc_capacity_factor(
                power,
                updates_monthly,
                updates_yearly,
                updates_hourly,
                year,
                unit[0],
                unit[3],
                curtailment,
            )

            update_db(batch_size)

        # Last batch updates
        update_db(None)

        logger.info(
            f"Processed {len(mastr_unit_list_of_tuples)} solar units for {year}"
        )

        duration = round(time.time() - start_c_time, 2)

        logger.info(f"Processing time for year {year} took: {duration} seconds")
