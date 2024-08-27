import os
import time
import numpy as np
import pandas as pd
import xarray as xr
from typing import List
from pvlib.irradiance import get_sky_diffuse, poa_components, aoi_projection, dni
from pvlib.pvsystem import retrieve_sam, sapm, sapm_effective_irradiance
from pvlib.solarposition import get_solarposition

from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS, sapm_cell

from kfw_mastr.utils.config import setup_logger, get_engine, session_scope
from kfw_mastr.utils.helpers import (
    slice_weather_solar,
    compute_monthly_statistics,
    create_results_dict,
)
from kfw_mastr.utils.orm import *
from kfw_mastr.utils.session_funcs import query_table

# configs
logger = setup_logger()
engine, metadata = get_engine()

# load pv module specifics & chose pv_module
"""
Notes
Glass/Glass Modules
Construction: These modules have glass on both the front and back sides, which enhances their durability and longevity. They are particularly resistant to environmental stress and can withstand harsh weather conditions better than other types.
Performance: Glass/glass modules tend to have lower degradation rates and higher durability. The open rack mounting option shows a higher temperature coefficient of power (a = -3.47) compared to close roof mounting (a = -2.98), indicating a greater loss of power with temperature increase. The temperature difference (ΔT[C]) is also higher for open rack (3) than for close roof (1), showing that open rack modules experience greater temperature fluctuations.

Glass/Polymer Modules
Construction: These modules have glass on the front and a polymer material on the back. They are generally lighter and less expensive than glass/glass modules but may have a shorter lifespan and less resistance to physical stress.
Performance: Glass/polymer modules mounted on an open rack show the highest temperature coefficient of power (a = -3.56) among the four configurations, indicating they are most affected by temperature increases. When mounted with an insulated back, these modules perform better with a lower temperature coefficient of power (a = -2.81) and no temperature difference (ΔT[C] = 0), suggesting that insulation helps maintain a consistent temperature, improving performance.

Summary
Temperature Coefficient of Power (a): This value indicates how much the power output of the solar panel decreases with an increase in temperature. Lower values are better, as they indicate less loss of power.
Temperature Coefficient of Voltage (b): This value shows how the voltage output changes with temperature. Lower values are preferable as they indicate less impact from temperature changes.
ΔT[C]: This represents the temperature difference experienced by the module. Higher values indicate greater temperature fluctuations, which can impact the performance and longevity of the modules.
In general, glass/glass modules offer better durability and performance stability, especially under close roof mounting, while glass/polymer modules are lighter and less costly, with insulated back mounting providing the best thermal performance.

Choose from keys:
{
    'open_rack_glass_glass': {'a': -3.47, 'b': -.0594, 'deltaT': 3},
    'close_mount_glass_glass': {'a': -2.98, 'b': -.0471, 'deltaT': 1},
    'open_rack_glass_polymer': {'a': -3.56, 'b': -.0750, 'deltaT': 3},
    'insulated_back_glass_polymer': {'a': -2.81, 'b': -.0455, 'deltaT': 0},
}
"""
params = TEMPERATURE_MODEL_PARAMETERS["sapm"]["open_rack_glass_glass"]
module = retrieve_sam(name="SandiaMod")
# chose from SandiaMod catalog in: kfw-mastr\input\SandiaMod_catalog_pv_modules.csv
# to change module replace "Advent_Solar_AS160___2006_" with column header from csv in "module.Advent_Solar_AS160___2006_" below
pv_module = module.Advent_Solar_AS160___2006_
pvm_max_power = sapm(1000, 25, pv_module)["p_mp"]  # unit: Watt | yields same result as multiplying amperage * voltage in max.power point --> pv_module["Impo"] * pv_module["Vmpo"]


def solar_calculations(
    unit: tuple,
    xr_time: xr.DataArray,
    xr_u: xr.Dataset,
    xr_v: xr.Dataset,
    xr_ssrd: xr.Dataset,
    xr_fdir: xr.Dataset,
    xr_sp: xr.Dataset,
    xr_t2m: xr.Dataset,
    xr_alnid: xr.Dataset,
    start_time: str,
    end_time: str,
    era5_ags_lat,
    era5_ags_lon,
    solar_zenith,
    solar_azimuth
) -> tuple:
    """
    Perform solar power calculations for a given unit and time range.

    Parameters
    ----------
    unit : tuple
        A tuple containing unit-specific parameters including azimuth angle, tilt angle,
        power, latitude, and longitude.
    xr_time : xr.DataArray
        The xarray DataArray containing time coordinates.
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

    Returns
    -------
    tuple
        A tuple containing the calculated power and effective irradiance.
    """

    # assign
    azimuth_angle = unit[1]
    tilt_angle = unit[2]
    power = unit[3]

    (
        xr_u_sliced,
        xr_v_sliced,
        xr_ssrd_sliced,
        xr_fdir_sliced,
        xr_sp_sliced,
        xr_t2m_sliced,
        xr_alnid_sliced,
    ) = slice_weather_solar(
        xr_u,
        xr_v,
        xr_ssrd,
        xr_fdir,
        xr_sp,
        xr_t2m,
        xr_alnid,
        start_time=start_time,
        end_time=end_time,
        latitude=era5_ags_lat,
        longitude=era5_ags_lon,
    )

    # weather sliced for unit coordinates, as numpy arrays
    u = xr_u_sliced["u10"].values
    v = xr_v_sliced["v10"].values
    ssrd = xr_ssrd_sliced["ssrd"].values / 3600  # W/m^2 # ghi
    fdir = (
        xr_fdir_sliced["fdir"].values / 3600
    )  # W/m^2 # dhi (direct horizontal irradiance)
    sp = xr_sp_sliced["sp"].values
    t2m = xr_t2m_sliced["t2m"].values - 273.15  # celsius
    alnid = xr_alnid_sliced["alnid"].values

    # wind speed at 10m
    v_h0 = np.sqrt(u**2 + v**2)

    # # get timeindex for solar calculations
    pd_timeindex = xr_time.coords["time"].to_index()


    ########################
    ## solar calculations ##
    ########################

    # diffuse horizontal radiation = global horizontal radiation - direct horizontal radiation
    # diffuse_hor_irr = surface_solar_radiation_downwards (ssrd) - total_sky_direct_solar_radiation_at_surface (fdir)
    diffuse_hor_irr = ssrd - fdir

    # #get_solarposition, requires pandas.DatetimeIndex | most expensive in solar calculations
    # solar_angles = get_solarposition(
    #     pd_timeindex,
    #     era5_ags_lat,
    #     era5_ags_lon,
    #     method="nrel_numba",
    #     numthreads=int(os.getenv('NUMBER_THREADS')),
    # )
    # solar_zenith = solar_angles["zenith"]
    # solar_azimuth = solar_angles["azimuth"]

    # angle of incidence
    min_projection_angle = 88
    threshold = np.cos(np.deg2rad(min_projection_angle))
    aoi_ndarray_projection = aoi_projection(tilt_angle, azimuth_angle, solar_zenith, solar_azimuth)
    if (aoi_ndarray_projection < threshold).any():
        # set values to smaller than 88 degree to zero, assuming panel cannot produce electricity when sun is behind panel
        aoi_ndarray_projection[aoi_ndarray_projection < threshold] = 0
    aoi_ndarray = np.rad2deg(np.arccos(aoi_ndarray_projection))

    # direct normal irradiation, no corrections
    # dni_ = fdir / aoi_ndarray_projection

    # direct normal irradiation with corrections
    dni_ = dni(ssrd, diffuse_hor_irr, aoi_ndarray, clearsky_dni=None, clearsky_tolerance=1.1,
        zenith_threshold_for_zero_dni=min_projection_angle,
        zenith_threshold_for_clearsky_limit=80.0)

    poa_ground_diffuse = ssrd * alnid * (1 - np.cos(np.radians(tilt_angle))) * 0.5

    # determine in-plane sky diffuse irradiance
    poa_sky_diffuse = get_sky_diffuse(
        tilt_angle,
        azimuth_angle,
        solar_zenith,
        solar_azimuth,
        dni_,
        ssrd,
        diffuse_hor_irr,
        dni_extra=None,
        airmass=None,
        model="isotropic",
    )

    irrads = poa_components(aoi_ndarray, dni_, poa_sky_diffuse, poa_ground_diffuse)

    poa_global = irrads["poa_global"]
    poa_direct = irrads["poa_direct"]
    poa_diffuse = irrads["poa_diffuse"]

    # cell temperature
    temp_cell = sapm_cell(poa_global, t2m, v_h0, **params, irrad_ref=1000.0)

    # airmass
    airmass_absolute = (sp / 101325) / np.cos(
        np.radians(solar_zenith) + 0.5 * (93.885 - solar_zenith) ** (-1.253)
    )

    # effective irradiance
    effective_irradiance = sapm_effective_irradiance(
        poa_direct, poa_diffuse, airmass_absolute, aoi_ndarray, pv_module
    )

    # power for one module
    power = sapm(effective_irradiance, temp_cell, pv_module)["p_mp"]

    return power


def calc_capacity_factor(
    power: pd.Series,
    updates_monthly: list,
    updates_yearly: list,
    updates_hourly: list,
    year: int,
    mastrid: str,
    net_capacity: float,
    curtailment: float = None,
) -> tuple:
    """
    Calculate the capacity factor and update the results for hourly, monthly, and yearly intervals.

    Parameters
    ----------
    power : pd.Series
        Series of power values.
    updates_monthly : list
        List to store monthly updates.
    updates_yearly : list
        List to store yearly updates.
    updates_hourly : list
        List to store hourly updates.
    year : int
        The year of the data.
    mastrid : str
        Identifier for the master record.
    net_capacity : float
        Net capacity of the power unit.
    curtailment : float, optional
        Curtailment factor to be applied to the power values, by default None.

    Returns
    -------
    tuple
        Updated lists of hourly, monthly, and yearly updates.
    """

    power_h_module = np.round(power.to_numpy(), 4)
    power_h_module = np.nan_to_num(power_h_module, nan=0.0)  # replace 'nan' with 0.0

    if curtailment:
        power_h_module = np.round(power.to_numpy(), 4) * curtailment

    # cf_rounded_hourly = cf_hourly_module = cf_hourly_mastrid
    cf_hourly = np.round((power_h_module / pvm_max_power), 4)

    # scale with Nettonennleistung
    power_mastrid_hourly = cf_hourly * net_capacity  # unit: kWh

    ## Fill results dicts
    power_sum_masterid_monthly = (
        np.round(compute_monthly_statistics(power_mastrid_hourly, operation="sum"), 4)
        .flatten()
        .tolist()
    )
    cf_ave_monthly = (
        np.round(compute_monthly_statistics(cf_hourly, operation="mean"), 4)
        .flatten()
        .tolist()
    )

    # results yearly
    power_sum_mastrid_yearly = float(np.round(np.sum(power_mastrid_hourly), 0))
    cf_ave_yearly = float(np.round(np.mean(cf_hourly), 4))

    power_mastrid_hourly = power_h_module.tolist()
    cf_hourly = cf_hourly.tolist()

    if os.getenv('SAVE_HOURLY_DATA') == 'True':
        updates_hourly.append(create_results_dict(mastrid, year, power_mastrid_hourly, cf_hourly, "h"))
    updates_monthly.append(create_results_dict(mastrid, year, power_sum_masterid_monthly, cf_ave_monthly, "m"))
    updates_yearly.append(create_results_dict(mastrid, year, power_sum_mastrid_yearly, cf_ave_yearly, "y"))

    return updates_hourly, updates_monthly, updates_yearly


def load_calculation_solar_data(query_limit: int,
                                incremental: bool,
                                years: List[int]) -> tuple:
    """
    Load solar mastr unit data from the database with a specified query limit.

    Parameters
    ----------
    query_limit : int
        The maximum number of records to retrieve.
    incremental : bool, 
        True, means infcremantal mode
    years : [int], only necessary, if incremental = True
        Array of integer for years
        
    Returns
    -------
    tuple
        A tuple containing the query result and the session object.
    """
    logger.info(locals())
    if len(years) != 1 and incremental:
       raise ValueError(f"Parameter years is {years}, but on incremental mode only one year is excepted!!!")

    with session_scope(engine=engine) as session:
        wc_query = query_table(
            session,
            table=Calculation_solar,
            column_names=[
                "EinheitMastrNummer",
                "azimuth_angle_mapped",
                "tilt_angle_mapped",
                "Nettonennleistung",
                "era5_ags_lat",
                "era5_ags_lon",
                "Breitengrad",
                "Laengengrad",
            ],
            limit = query_limit,
            incremental = incremental,
            year = years[0],
        )

    return wc_query, session

def load_calculation_solar_data_angles() -> tuple:

    with session_scope(engine=engine) as session:
        wc_query = query_table(
            session,
            table=Calculation_solar_angles,
            column_names=[
                "year",
                "lat_lon",
                "era5_lat",
                "era5_lon",
                "solar_zenith",
                "solar_azimuth",
            ],
        )

    # Convert the list of tuples to a dictionary
    solar_angles_dict = {
        item[1]: {
            "era5_lat": item[2],
            "era5_lon": item[3],
            "solar_zenith": pd.Series(item[4]),
            "solar_azimuth": pd.Series(item[5]),
        }
        for item in wc_query
    }

    return solar_angles_dict
