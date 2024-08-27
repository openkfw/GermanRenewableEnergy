# Download ERA5 weather data via API

1. CDS API on Windows is already installed, if environment was created from `environment.yaml` (package https://pypi.org/project/cdsapi/). If not, follow this [steps](https://confluence.ecmwf.int/display/CKB/How+to+install+and+use+CDS+API+on+Windows).
2. Make an [account](https://cds.climate.copernicus.eu/user/register?destination=/api-how-to) 
3. Login [here](https://cds.climate.copernicus.eu/user/login?destination=/api-how-to)
4. Click on your name in the top-right corner and bring you API Key details in the following format:
```
url: https://cds.climate.copernicus.eu/api/v2
key: {uid}:{api-key}
```
For example:
```
url: https://cds.climate.copernicus.eu/api/v2
key: 311117:481dd8bc-224e-4g9b-8gdh-4heh4f432ae5
```

5. Paste the information into a text editor and save the file as `.cdsapirc` in folder: <br>
`$HOME/.cdsapirc` (LINUX) or <br>
`C:\Users\<user>\.cdsapirc` (WINDOWS).

6. Now run [era5.py](https://github.com/chrwm/kfw-mastr/blob/main/kfw_mastr/utils/era5.py)
```
python kfw_mastr/utils/era5.py
```

Note: If `python kfw_mastr/utils/era5.py` doesn't work, try to run it with path to config.yaml
```
python kfw_mastr/setup_database.py --config_path "C:\Users\user\your\favourite\path\config_file_can_have_any_name.yaml"
```
Specify the years to download in parameter `years` [here](https://github.com/chrwm/kfw-mastr/blob/2c7f363d5df6765ac32dcdeff4ce51bb3a672ed2/kfw_mastr/utils/era5.py#L138)
e.g. `years = [2024]` <br>

By default, [all necessary weather data for wind and solar](https://github.com/chrwm/kfw-mastr/blob/2c7f363d5df6765ac32dcdeff4ce51bb3a672ed2/kfw_mastr/utils/era5.py#L13) will be downloaded and saved in `INPUT_PATH/era5/hourly`.