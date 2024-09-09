"""
Control calculations with help of config file
"""
import os
from kfw_mastr.calculate_cf_solar import calculate_cf_solar
from kfw_mastr.calculate_cf_wind import calculate_cf_wind
from kfw_mastr.aggregator import aggregate_solar, aggregate
from kfw_mastr.utils.config import setup_logger
from kfw_mastr.utils.helpers import export_and_copy_files, log_downloaded_mastr_version

logger = setup_logger()
log_downloaded_mastr_version()

def main():
    """
    Main function to read environment variables, and execute
    calculations for wind and solar capacity factors.

    This function performs the following steps:
    1. Reads environment variables to determine the program specifications:
       - YEARS: List of years for which to perform calculations.
       - BATCH_SIZE: Size of the data batches to process.
       - LIMIT_MASTR_UNITS: Limit on the number of units to process (optional).
    2. Depending on the environment variables, it calculates capacity factors
       for wind and solar energy and exports the results to CSV files.

    Environment Variables
    ---------------------
    YEARS : str
        Comma-separated list of years.
    BATCH_SIZE : str
        Batch size for processing data.
    LIMIT_MASTR_UNITS : str
        Limit on the number of units to process, 'None' if no limit.
    CALC_WIND : str
        Flag to determine if wind calculations should be performed ('True'/'False').
    CALC_SOLAR : str
        Flag to determine if solar calculations should be performed ('True'/'False').
    AGGREGATE_SOLAR : str
        Flag to determine if solar calculations should be performed ('True'/'False').H
    AGGREGATE_WIND : str
        Flag to determine if solar calculations should be performed ('True'/'False').

    Returns
    -------
    None
    """

    incremental = None
    
    # read environment variables for programme specifications
    if "YEARS" in os.environ:
        YEARS = [year.strip() for year in os.getenv("YEARS").split(",")]
    if "EXPORT_YEARS" in os.environ:
        EXPORT_YEARS = [year.strip() for year in os.getenv("EXPORT_YEARS").split(",")]
    if "BATCH_SIZE" in os.environ:
        BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
    if "EXPORT_BATCH_SIZE" in os.environ:
        EXPORT_BATCH_SIZE = int(os.getenv('EXPORT_BATCH_SIZE'))
    if "LIMIT_MASTR_UNITS" in os.environ:
        LIMIT_MASTR_UNITS = os.getenv("LIMIT_MASTR_UNITS")
        incremental = (LIMIT_MASTR_UNITS == "incremental")
        if LIMIT_MASTR_UNITS == "None" or LIMIT_MASTR_UNITS == "incremental":
            LIMIT_MASTR_UNITS = None
        else:
            LIMIT_MASTR_UNITS = int(LIMIT_MASTR_UNITS)

    if os.getenv("CALC_WIND") == "True":
        logger.info("Calculating wind electricity generation and  capacity factors")
        calculate_cf_wind(years=YEARS, batch_size=BATCH_SIZE, limit_mastr_units=LIMIT_MASTR_UNITS)

    if os.getenv("CALC_SOLAR") == "True":
        logger.info("Calculating solar capacity factors!")
        calculate_cf_solar(
            years=YEARS,
            batch_size=BATCH_SIZE,
            limit_mastr_units=LIMIT_MASTR_UNITS,
            incremental=incremental,
        )    
    # export
    export_info = f"EXPORT_YEARS: {EXPORT_YEARS}; EXPORT_UNITS: {os.getenv('EXPORT_UNITS')}; EXPORT_BATCH_SIZE: {os.getenv('EXPORT_BATCH_SIZE')}"

    if os.getenv("EXPORT_WIND") == "True" and EXPORT_YEARS[0] != "None":
        logger.info(
            f"Exporting wind results to csv. {export_info}")
        export_and_copy_files(years=EXPORT_YEARS, export_batch_size=EXPORT_BATCH_SIZE, tech="wind")

    if os.getenv("EXPORT_SOLAR") == "True" and EXPORT_YEARS[0] != "None":
        logger.info(f"Exporting solar results to csv. {export_info}")
        export_and_copy_files(years=EXPORT_YEARS, export_batch_size=EXPORT_BATCH_SIZE, tech="solar")

    if os.getenv("AGGREGATE_SOLAR") == "True":
        aggregate('solar')
        
    if os.getenv("AGGREGATE_WIND") == "True":
        aggregate('wind')
        

if __name__ == "__main__":
    main()


