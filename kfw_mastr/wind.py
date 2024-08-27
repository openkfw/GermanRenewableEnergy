import os

import numpy as np
import pandas as pd

from kfw_mastr.utils.config import setup_logger, get_engine, session_scope
from kfw_mastr.utils.constants import DEFAULT_TURBINE_TYPE, RHO_0, R
from kfw_mastr.utils.helpers import compute_monthly_statistics, create_results_dict
from kfw_mastr.utils.orm import *
from kfw_mastr.utils.session_funcs import query_table

logger = setup_logger()

engine, metadata = get_engine()


class WindCalc:

    # variable abbreviations
    # fsr - forecast_surface_roughness
    # v_h0 - wind speed 100 / 10m (pytagoras of u&v wind component)
    # hh - hub height
    # sp - surface pressure@2m

    def __init__(self, input_path: str = "input") -> None:
        # Initialise power_curve lookup table
        self.power_curve_df = pd.read_csv(
            os.path.join(os.getenv('REPO_ROOT'), "input", "power_curves_interpolated.csv"), sep=";"
        ).set_index("turbine_type")

    def calculate_v_hh_norm_formula_substituted(
        self,
        u: np.ndarray,
        v: np.ndarray,
        hh: float,
        h0: float,
        fsr: float,
        sp: np.ndarray,
        t2m: np.ndarray,
        R: float,
        RHO_0: float,
    ) -> np.ndarray:
        """
        Calculate the normalized wind speed at hub height.

        Parameters
        ----------
        u : np.ndarray
            U-component of wind speed.
        v : np.ndarray
            V-component of wind speed.
        hh : float
            Hub height.
        h0 : float
            Reference height of wind speed (100m).
        fsr : float
            Surface roughness.
        sp : np.ndarray
            Surface pressure.
        t2m : np.ndarray
            2m temperature.
        R : float
            Specific gas constant.
        RHO_0 : float
            Reference air density.

        Returns
        -------
        np.ndarray
            Normalized wind speed at hub height.
        """
        # calculate wind speed at hub height
        v_h0 = np.sqrt(u**2 + v**2)
        v_hh = v_h0 * np.log(hh / fsr) / np.log(h0 / fsr)
        p_hh = sp * (1 - 0.0065 * hh / t2m) ** 5.25
        t_hh = t2m - (6.5 * hh / t2m)
        rho_hh = p_hh / (t_hh * R)

        # Calculate final normalized wind speed
        return v_hh * (rho_hh / RHO_0) ** (1 / 3)

    def get_power(
        self,
        v_hh_norm: float,
        turbine_type: str = DEFAULT_TURBINE_TYPE,
        cut_off: bool = False,
        turbine_cut_off_wind_speed_flex: int = 1,
    ) -> tuple:
        """
        Calculate the power output of a wind turbine given the wind speed at hub height.

        Parameters
        ----------
        v_hh_norm : float
            Normalized wind speed at hub height.
        turbine_type : str, optional
            The type of wind turbine, by default DEFAULT_TURBINE_TYPE.
        cut_off : bool, optional
            Whether to apply a cut-off for high wind speeds, by default False.
        turbine_cut_off_wind_speed_flex : int, optional
            The flexibility for turbine cut-off wind speed, by default 1.
        round_power : int, optional
            The precision for rounding power output, by default 4.

        Returns
        -------
        tuple
            The power output at the given wind speed and the maximum power output of the turbine.
        """

        # get power outputs per turbine type at corresponding wind speed
        power_outputs = np.array(self.power_curve_df.loc[turbine_type], dtype=float)
        wind_speeds = np.array(self.power_curve_df.columns, dtype=float)

        # Get the maximum power output turbine for normalisation, ignoring NaNs
        power_output_turbine_max = np.nanmax(power_outputs)

        # Handle nan values by using interpolation for existing wind speeds
        valid_power_indices = ~np.isnan(power_outputs)
        wind_speeds_valid = wind_speeds[valid_power_indices]
        power_outputs_valid = power_outputs[valid_power_indices]

        # Wind turbine cut off, when wind speed is too high.
        last_valid_power_index = len(power_outputs_valid) - 1
        wind_speed_at_last_power = wind_speeds_valid[last_valid_power_index]

        # Default: Turbine cut off at 1m/s above last wind speed with power value
        if (
            cut_off
            and v_hh_norm > wind_speed_at_last_power + turbine_cut_off_wind_speed_flex
        ):
            return 0

        # Interpolate power for v_hh_norm
        power_output_at_vhh = np.interp(
            v_hh_norm, wind_speeds_valid, power_outputs_valid
        )

        return power_output_at_vhh, power_output_turbine_max

    def calculate_power(
        self,
        u: float,
        v: float,
        hh: float,
        h0: float,
        fsr: float,
        sp: np.ndarray,
        t2m: np.ndarray,
        turbine_type: str,
    ) -> tuple:
        """
        Wrapper function to calculate turbine power output.

        Parameters
        ----------
        u : float
            U-component of wind speed.
        v : float
            V-component of wind speed.
        hh : float
            Hub height.
        h0 : float
            Reference height.
        fsr : float
            Surface roughness.
        sp : np.ndarray
            Surface pressure.
        t2m : np.ndarray
            2m temperature.
        turbine_type : str
            The type of wind turbine.

        Returns
        -------
        tuple
            The calculated power output and the maximum power output of the turbine.
        """

        v_hh_norm = self.calculate_v_hh_norm_formula_substituted(
            u, v, hh, h0, fsr, sp, t2m, R, RHO_0
        )
        power, power_output_turbine_max = self.get_power(v_hh_norm, turbine_type)

        return power, power_output_turbine_max

    def load_turbine_data(self, query_limit: int = None) -> list[dict]:
        """
        Load wind turbine mastr data from the database with a specified query limit.

        Parameters
        ----------
        query_limit : int
            The maximum number of records to retrieve.

        Returns
        -------
        list[dict]
            A list of dictionaries containing the turbine data.
        """
        with session_scope(engine=engine) as session:
            wc_query = query_table(
                session,
                table=Calculation_wind,
                column_names=[
                    "EinheitMastrNummer",
                    "turbine_mapped",
                    "hub_height_mapped",
                    "Nettonennleistung",
                    "era5_ags_lat",
                    "era5_ags_lon",
                    "Breitengrad",
                    "Laengengrad",
                ],
                limit=query_limit,
            )
        return wc_query, session

    def calc_capacity_factor_wind(
            self,
            power: pd.Series,
            max_power_power_curve,
            updates_monthly: list,
            updates_yearly: list,
            updates_hourly: list,
            year: int,
            mastrid: str,
            net_capacity: float,
            curtailment: float = None,
    ):
        """
        Calculate and update capacity factors for wind energy units.

        This function calculates hourly, monthly, and yearly capacity factors for a wind
        energy unit based on its power output and updates the provided lists with the results.

        Parameters
        ----------
        power : pd.Series
            Series containing power output data.
        max_power_power_curve : float
            Maximum power output according to the power curve.
        updates_monthly : list
            List to store monthly updates.
        updates_yearly : list
            List to store yearly updates.
        updates_hourly : list
            List to store hourly updates.
        year : int
            Year for which the calculations are performed.
        mastrid : str
            ID of the wind energy unit.
        net_capacity : float
            Net capacity of the wind energy unit.
        curtailment : float, optional
            Curtailment factor to adjust power output (default is None).

        Returns
        -------
        updates_hourly : list
            Updated list with hourly capacity factor results.
        updates_monthly : list
            Updated list with monthly capacity factor results.
        updates_yearly : list
            Updated list with yearly capacity factor results.
        """

        # hourly in kWh for one turbine of mapped turbine type
        max_power_power_curve = max_power_power_curve / 1000
        power_hourly = np.round(power, 4) / 1000
        power_hourly = np.nan_to_num(power_hourly, nan=0.0)

        if curtailment:
            power_hourly = np.round(power, 4) * curtailment

        cf_hourly = power_hourly / max_power_power_curve
        cf_hourly = np.round(cf_hourly, 4)

        # yearly results
        power_sum_mastrid_yearly = float(np.round(np.sum(power_hourly), 0))
        cf_ave_yearly = float(np.round(np.mean(cf_hourly), 4))

        ## Fill results dicts
        power_sum_masterid_monthly = (
            np.round(compute_monthly_statistics(power_hourly, operation="sum"), 4)
            .flatten()
            .tolist()
        )
        cf_ave_monthly = (
            np.round(compute_monthly_statistics(cf_hourly, operation="mean"), 4)
            .flatten()
            .tolist()
        )

        # energy production for mastr wind unit with its net capacity
        power_mastrid_hourly = (
            cf_hourly * net_capacity
        )  # there might be differences to power_hourly, when the mapping of the turbine wasn't exact. E.g. when the mastr unit has 2000 kW net capacity and the mapped turbine 2500 kW net capacity. However scaling the mastr net capacity with the capacity factor is more realistic to estimate the real-actual eletricity generation
        power_mastrid_hourly = power_mastrid_hourly.tolist()
        cf_hourly = cf_hourly.tolist()

        if os.getenv('SAVE_HOURLY_DATA') == 'True':
            updates_hourly.append(
                create_results_dict(mastrid, year, power_mastrid_hourly, cf_hourly, "h")
            )
        updates_monthly.append(
            create_results_dict(
                mastrid, year, power_sum_masterid_monthly, cf_ave_monthly, "m"
            )
        )
        updates_yearly.append(
            create_results_dict(
                mastrid, year, power_sum_mastrid_yearly, cf_ave_yearly, "y"
            )
        )

        return updates_hourly, updates_monthly, updates_yearly
