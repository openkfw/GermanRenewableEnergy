import os
import time

from tqdm import tqdm

from kfw_mastr.utils.config import setup_logger, get_engine
from kfw_mastr.utils.helpers import (
    load_era5_weather_wind,
    slice_weather_wind,
    get_curtailment, create_results_dict,
)
from kfw_mastr.utils.session_funcs import save_and_commit
from kfw_mastr.setup_database import get_nearest_era5_coordinate
from kfw_mastr.wind import WindCalc

logger = setup_logger()
engine, metadata = get_engine()

wind = WindCalc()


def calculate_cf_wind(
    years: list[str],
    batch_size: int,
    conflict_action: str = "update",
    limit_mastr_units: int = None,
):
    """
    Calculate capacity factors for wind energy units over specified years.

    This function loads weather data, turbine data, and performs capacity factor
    calculations for wind energy units, updating the results in batches.

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

    # load all wind units and their "Stammdaten"
    mastr_unit_list_of_tuples, session = wind.load_turbine_data(
        query_limit=limit_mastr_units
    )

    # get optional curtailment
    curtailment = get_curtailment("CURTAILMENT_WIND")

    # load era5 data for year
    for year in years:
        start_c_time = time.time()
        logger.info(
            f"Loading weather year {year} and list of mastr units. Batch-size: {batch_size}"
        )
        start_time = f"{year}"
        end_time = f"{year}"

        # load relevant wind weather data for each year static load era5 data
        xr_u, xr_v, xr_fsr, xr_sp, xr_t2m = load_era5_weather_wind(year=year)

        updates_hourly = []
        updates_monthly = []
        updates_yearly = []

        for unit in tqdm(
            mastr_unit_list_of_tuples,
            desc=f"Calculations for mastr wind unit. Committing do db in batches of {batch_size}",
        ):
            # logger.info(f"Process unit {unit[0]}")

            # assign
            mastrid = unit[0]
            turbine_type = unit[1]
            hh = unit[2]
            era5_ags_lat = unit[4]
            era5_ags_lon = unit[5]
            lat = unit[6]
            lon = unit[7]

            # handle if ags coordinates are empty
            if era5_ags_lat is None or era5_ags_lon is None:
                era5_ags_lat, era5_ags_lon = get_nearest_era5_coordinate(lat, lon)
                # Correct check for None
                if lat is None or lon is None:
                    no_calc_reason = "Missing AGS or other coordinates"
                    if os.getenv('SAVE_HOURLY_DATA') == 'True':
                        updates_hourly.append(create_results_dict(mastrid, year, [0], [0], "h", no_calc_reason))
                    updates_monthly.append(
                        create_results_dict(mastrid, year, [0], [0], "m", no_calc_reason))
                    updates_yearly.append(
                        create_results_dict(mastrid, year, 0, 0, "y", no_calc_reason))

                    continue

            xr_u_sliced, xr_v_sliced, xr_fsr_sliced, xr_sp_sliced, xr_t2m_sliced = (
                slice_weather_wind(
                    xr_u,
                    xr_v,
                    xr_fsr,
                    xr_sp,
                    xr_t2m,
                    start_time=start_time,
                    end_time=end_time,
                    latitude=era5_ags_lat,
                    longitude=era5_ags_lon,
                )
            )

            u = xr_u_sliced["u100"].values
            v = xr_v_sliced["v100"].values
            fsr = xr_fsr_sliced["fsr"].values
            sp = xr_sp_sliced["sp"].values
            t2m = xr_t2m_sliced["t2m"].values
            h0 = 100

            power, max_power_power_curve = wind.calculate_power(
                u, v, hh, h0, fsr, sp, t2m, turbine_type
            )

            updates_hourly, updates_monthly, updates_yearly = (
                wind.calc_capacity_factor_wind(
                    power,
                    max_power_power_curve,
                    updates_monthly,
                    updates_yearly,
                    updates_hourly,
                    year,
                    unit[0],
                    unit[3],
                    curtailment,
                )
            )

            # Save batches
            save_and_commit(
                session,
                updates_hourly,
                conflict_action,
                "updates_hourly",
                batch_size,
                "wind",
            )
            save_and_commit(
                session,
                updates_monthly,
                conflict_action,
                "updates_monthly",
                batch_size,
                "wind",
            )
            save_and_commit(
                session,
                updates_yearly,
                conflict_action,
                "updates_yearly",
                batch_size,
                "wind",
            )

            # Last batch updates
        if updates_hourly:
            save_and_commit(
                session,
                updates_hourly,
                conflict_action,
                "updates_hourly",
                batch_size=None,
                table_id="wind",
            )
        if updates_monthly:
            save_and_commit(
                session,
                updates_monthly,
                conflict_action,
                "updates_monthly",
                batch_size=None,
                table_id="wind",
            )
        if updates_yearly:
            save_and_commit(
                session,
                updates_yearly,
                conflict_action,
                "updates_yearly",
                batch_size=None,
                table_id="wind",
            )

        logger.info(
            f"Processed {len(mastr_unit_list_of_tuples)} wind unit for year {year}"
        )

        duration = round(time.time() - start_c_time, 2)

        logger.info(f"Processing time for year {year} took: {duration} seconds")
