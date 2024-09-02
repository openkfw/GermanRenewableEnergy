## :running: Run the programm

:warning: The following steps assume that the previous steps have been performed already successfully - specifically:
* Docker postgre database created successfully
* Marstammdatenregister imported into database
* German municipalities imported into database
* Calculation tables for wind and solar are successfully created

### Step-by-step guide to run programme

#### :one: Starting docker container

Docker needs to be started before the programme can run successfully. <br>
On Windows just start `Docker Desktop` (might need to be started with administrator rights, if problems occur) and start the docker container.

#### :two: Configure and start programme

##### :green_book: Working with terminal or command line

1. Make sure [python path](docs/setup_and_install.md#3-set-python-path) is set correctly to REPO_ROOT
2. Make sure no other scripts are running within kfw-mastr, as they will block the deletion of `kfw-mastr.log` and yield an unclean exported log.
3. Configure [config.yaml](config.yaml) for the run. Visit the [`Config.yaml documentation`](#configyaml-documentation) below for more setting details and how to use config.yaml.
4. Run 
```
python main.py
```
Optional: Define path to config.yaml
```
python main.py --config_path "C:\Users\user\your\favourite\path\config_file_can_have_any_name.yaml"
```

##### :orange_book: Working with an integrated development environment (IDE) [e.g. Pycharm or Visual Studio Code]

1. Set `kfw-mastr` as environment in your IDE
2. Make sure no other scripts are running within kfw-mastr, as they will block the deletion of `kfw-mastr.log` and yield an unclean exported log.
3. Configure [config.yaml](config.yaml) for the run. Visit the [`Config.yaml documentation`](#configyaml-documentation) below for more setting details and how to use config.yaml.
4. Run 
```
python main.py
```
Optional: Define path to config.yaml in `Script parammeters`: `--config_path=C:\_prog\_code\kfw-mastr\config.yaml`

![ide_script_parameters.png](docs/ide_script_parameters.png)



### Config.yaml documentation
For running the programme you can use a yaml file to configure the run in `config.yaml`:

| Variable             | Value                                      | Default             | Example                                             | Explanation                                                                                                                                                                    |
|----------------------|--------------------------------------------|---------------------|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| REPO_ROOT            | "C:\\Users\\user\\user\\Python\\kfw-mastr" | Error if not set    |                                                     | Specify path to cloned repo.                                                                                                                                                   |
| NUMBER_THREADS       | "4"                                        | "4"                 | "32"                                                | Specify number of threads for `get_solarposition()`.                                                                                                                           |
| INPUT_PATH           | "C:\\somewhere\\input"                     | Error if not set    |                                                     | Specify path to input folder with weather data.                                                                                                                                |
| OUTPUT_PATH          | "C:\\somewhere\\output"                    | Error if not set    |                                                     | Specify path to output folder.                                                                                                                                                 |
| POSTGRESQL_HOST      | "None"                                     | "None"              |                                                     | Optional: Needed if database is not setup locally, but e.g. in AZURE cloud.                                                                                                    |
| POSTGRESQL_DB_NAME   | "kfw-mastr"                                | "kfw-mastr"         |                                                     | DB name                                                                                                                                                                        |
| POSTGRESQL_USER      | "postgres"                                 | "postgres"          |                                                     | DB user                                                                                                                                                                        |
| POSTGRESQL_PASSWORD  | "postgres"                                 | "postgres"          |                                                     | DB password                                                                                                                                                                    |
| POSTGRESQL_PORT      | "5512"                                     | "5512"              |                                                     | Postgresql host port                                                                                                                                                           |
| SOFTWARE_VERSION     | "can_be_any_string"                        | "1_0_0"             | "1_12_0"                                            | Identifier for programme run.                                                                                                                                                  |
| OUTFILE_POSTFIX      | "can_be_any_string"                        | str(datetime.now()) | "2024-06-26_19-29-20"                               | Identifier for programme run.                                                                                                                                                  |
| CONFLICT_ACTION      | "update"                                   | "update"            | "update" or "skip_existing_row"                     | Decide what to do when calculation is performed for unit that exists already in result table                                                                                   |
| YEARS                | "2000"                                     | "2023"              | "2000, 2001, 2003, 2012, 2022, 2023"                | Specify years to calculate.                                                                                                                                                    |
| BATCH_SIZE           | "200000"                                   | "200000"            | "200"                                               | Defines number of units committed to db in batch.                                                                                                                              |
| LIMIT_MASTR_UNITS    | "None"                                     | "None"              | "21200"                                             | Limit how many MaStR units are loaded from Calculation_wind or Calculation_solar tables.                                                                                       |
| YEAR_SOLAR_ANGLE     | "2000"                                     | "2000"              | "2024"                                              | Defines both: 1. the year in `setup_database.py` for which the solar angles are pre-calculated and 2. the year solar angles are read from db tables `Calculation_solar_angles` |
| SAVE_HOURLY_DATA     | "False"                                    | "False"             | "True"                                              | If "True" the hourly calculated data will be saved to database. This will slow down the calculation process for many units.                                                    |
| CALC_SOLAR           | "False"                                    | "False"             |                                                     | If True, solar electricity generation and capacity factors will be calculated.                                                                                                 | 
| SPECIFIC_SOLAR_UNITS | "None"                                     | "None"              | "SEE900000245244, SEE900000175985, SEE900000473975" | Insert MaStR units. MaStR units from wind, will not yield any results (assuming each ID was only assigned once by BNetzA).                                                     |
| CURTAILMENT_SOLAR    | "None"                                     | "None"              | "0.2"                                               | Value between [0, 1]. Curtailment 20% (20% electricity will be dumped). Each hour will be multiplied with 0.8, which equals 80% of maximal theoretical electricity production. |
| CALC_WIND            | "True"                                     | "False"             |                                                     | If True, wind electricity generation and capacity factors will be calculated.                                                                                                  |
| SPECIFIC_WIND_UNITS  | "SEE900002935310, SEE900003441513"         | "None"              | "SEE900018842459, SEE900019954427, SEE900052108511" | Insert MaStR units. MaStR units from Solar, will not yield any results (assuming each ID was only assigned once by BNetzA).                                                    |
| CURTAILMENT_WIND     | "0.2"                                      | "None"              | "0.2"                                               | Value between [0, 1]. Curtailment 20% (20% electricity will be dumped). Each hour will be multiplied with 0.8, which equals 80% of maximal theoretical electricity production. |
| EXPORT_WIND          | "False"                                    | "False"             | "True"                                              | If "True" wind units are exported.                                                                                                                                             |
| EXPORT_SOLAR         | "False"                                    | "False"             | "True"                                              | If "True" solar units are exported.                                                                                                                                            |
| EXPORT_BATCH_SIZE    | "100000"                                   | "100000"            | "250"                                               | Especially the solar export can be very memory demanding with 4 million rows. Thus, EXPORT_BATCH_SIZE offers the option to export in batches for older machines.               |
| EXPORT_UNITS         | "SEE914274638794, SEE900019954427"         | "all"               | "SEE900006739006, SEE900067974880"                  | Option to export specific MaStR units. Wind and solar units can be inserted together in EXPORT_UNITS.                                                                          |
| EXPORT_YEARS         | "2000, 2001"                               | "None"              | "2000, 2001, 2022, 2023"                            | Defines the years to export from database                                                                                                                                      |
| AGGREGATE_SOLAR      | "False", "None" or "True"                  | "False"             | "True"                                              | Defines wheater an aggreation of solar panels results should be perfomaned, see /ouput/AGGREGATION.md for details                                                              |
| AGGREGATE_WIND       | "False", "None" or "True                   | "False"             | "True"                                              | Defines wheater an aggreation of wind turbines results should be perfomaned, see /ouput/AGGREGATION.md for details                                                             |


The estimated calculation time of the is highly dependent on the `config.yaml` settings and your machine.
