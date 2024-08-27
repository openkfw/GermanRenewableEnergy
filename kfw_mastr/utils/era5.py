import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cdsapi
import matplotlib.pyplot as plt
import netCDF4 as nc

from kfw_mastr.utils.config import setup_logger
from kfw_mastr.utils.constants import VARIABLES_SOLAR, VARIABLES_WIND

weather_data = set(VARIABLES_SOLAR + VARIABLES_WIND)

logger = setup_logger()

c = cdsapi.Client()

def get_hourly_data(request, year):
    path = os.path.join(os.environ['INPUT_PATH'], "era5", "hourly", fr'{year}_{request}.nc')
    try:
        logger.info(f"Requesting year: {year} - {request}")
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'variable': request,
                'year': year,
                'month': [
                    '01', '02', '03', '04', '05', '06',
                    '07', '08', '09', '10', '11', '12'
                ],
                'day': [
                    '01', '02', '03', '04', '05', '06',
                    '07', '08', '09', '10', '11', '12',
                    '13', '14', '15', '16', '17', '18',
                    '19', '20', '21', '22', '23', '24',
                    '25', '26', '27', '28', '29', '30', '31'
                ],
                'time': [
                    '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
                ],
                'area': [
                    60, 5, 45, 16,
                ],
                'format': 'netcdf',
            },
            path
        )
        logger.info(f"Saved request to: {path}")
    except Exception as e:
        logger.error(f"Error requesting year: {year} - {request}. Error: {e}")


def visualise_downloaded_region():
    file_path = r"C:\Users\christoph.muschner\CWM\Python\mastr-kfw\input\era5\DE_monthly\2000_all.nc"
    # Open the NetCDF file
    dataset = nc.Dataset(file_path)

    for variable in dataset.variables:
        print(variable, dataset.variables[variable].shape)

    # Extract latitude and longitude
    # Replace 'lat' and 'lon' with the actual variable names in your NetCDF file
    lon = dataset.variables['longitude'][:]
    lat = dataset.variables['latitude'][:]

    lon_grid, lat_grid  = np.meshgrid(lon, lat)

    # Create a figure and a map projection
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Add features to the map
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')

    # Plot the meshgrid points
    plt.scatter(lon_grid, lat_grid, color='blue', s=1.5, transform=ccrs.PlateCarree())

    # Set titles and labels
    plt.title('ERA5 Germany')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    # Display the plot
    plt.show('lat_lon_meshgrid_plot11.png')

    # Create a DataFrame
    df = pd.DataFrame({
        'Longitude': lon_grid.flatten(),
        'Latitude': lat_grid.flatten()
    })

    logger.info(f"Downloading weather data for:\n")
    logger.info(f"Longitude min:{df['Longitude'].min()}")
    logger.info(f"Longitude max:{df['Longitude'].max()}")
    logger.info(f"Latitude min:{df['Latitude'].min()}")
    logger.info(f"Latitude max:{df['Latitude'].max()}")



def download_era5_data(years, weather_data):
    """
    Download ERA5 weather data for specified years and data types.

    This function submits parallel requests to the ERA5 API to download
    weather data for the given years and data types, ensuring that a
    maximum of 20 requests are processed concurrently.

    Parameters
    ----------
    years : list of int
        List of years for which to download weather data.
    weather_data : list of str
        List of weather data types to request.

    Returns
    -------
    None
    """

    # ERA5 API restricted to max. 20 parallel requests
    with ThreadPoolExecutor(max_workers=19) as executor:
        futures = []
        for request in weather_data:
            for year in years:
                futures.append(executor.submit(get_hourly_data, request, year))

        for future in as_completed(futures):
            future.result()  # Ensures any exceptions are raised


if __name__ == "__main__":
    years = [year for year in range(2000, 2024)]
    download_era5_data(years, weather_data)
