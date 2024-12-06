# Installation

**Note**: Except for point 1, installing _miniconda_, the following description is based on using the Anaconda prompt (miniconda) on a Windows system. If you use a different tool for executing commands or another operating system, some modifications might be necessary.

#### 1. Install package manager _miniconda_

Download the latest miniconda version [here](https://docs.anaconda.com/miniconda/).

#### 2. Clone repository

Navigate to a folder where you wish to clone the repo (REPO_ROOT). Then use: 

```
git clone https://github.com/chrwm/kfw-mastr.git
```

#### 3. Set python path

2. Set python path to repository root
   
```
set PYTHONPATH=C:\path\to\your\folder_where_you_cloned_the_repo_into
```

2. Check if it worked. 

```
echo %PYTHONPATH%
```

The console should print the path you defined. 


#### 4. Setup environment and install packages

Navigate to the REPO_ROOT (the folder where you cloned the repo into) and run the 
conda code below: 

```
conda install mamba -n base -c conda-forge
```
```
mamba env create -f environment.yaml
```

This creates a virtual environment named `kfw-mastr` with Python 3.11 and all necessary packages to run the code.

#### 5. Install docker & create database

1. Download docker for Windows [here](https://docs.docker.com/desktop/install/windows-install/).
1. Start docker as administrator.
1. Navigate to repo root.

After ensuring that your docker deamon is active, run: 

```
docker-compose up
```

This creates the docker infrastructure: 

1. A container named `kfw-mastr`.
2. A volume named `kfw-mastr_kfwmastrDatabaseVolume`. This volume is used by a PostGIS PostgreSQL database which right now is quite empty besides of some object referring to the PostGIS service.

The console output might look like that: 

```
[+] Running 2/3
 ✔ Network kfw-mastr_default                  Created                                                              0.0s
 ✔ Volume "kfw-mastr_kfwmastrDatabaseVolume"  Created                                                              0.0s
 - Container postgis-database                 Created                                                              0.1s
Attaching to postgis-database
postgis-database  | The files belonging to this database system will be owned by user "postgres".
postgis-database  | This user must also own the server process.
postgis-database  |
postgis-database  | The database cluster will be initialized with locale "en_US.utf8".
postgis-database  | The default database encoding has accordingly been set to "UTF8".
postgis-database  | The default text search configuration will be set to "english".
postgis-database  |
postgis-database  | Data page checksums are disabled.
postgis-database  |
postgis-database  | fixing permissions on existing directory /var/lib/postgresql/data ... ok
postgis-database  | creating subdirectories ... ok
postgis-database  | selecting dynamic shared memory implementation ... posix
postgis-database  | selecting default max_connections ... 100
postgis-database  | selecting default shared_buffers ... 128MB
postgis-database  | selecting default time zone ... Etc/UTC
postgis-database  | creating configuration files ... ok
postgis-database  | running bootstrap script ... ok
postgis-database  | performing post-bootstrap initialization ... ok
postgis-database  | initdb: warning: enabling "trust" authentication for local connections
postgis-database  | initdb: hint: You can change this by editing pg_hba.conf or using the option -A, or --auth-local and --auth-host, the next time you run initdb.
postgis-database  | syncing data to disk ... ok
postgis-database  |
postgis-database  |
postgis-database  | Success. You can now start the database server using:
postgis-database  |
postgis-database  |     pg_ctl -D /var/lib/postgresql/data -l logfile start
postgis-database  |
postgis-database  | waiting for server to start....2024-11-21 16:11:27.806 UTC [48] LOG:  starting PostgreSQL 15.4 (Debian 15.4-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
postgis-database  | 2024-11-21 16:11:27.808 UTC [48] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
postgis-database  | 2024-11-21 16:11:27.815 UTC [51] LOG:  database system was shut down at 2024-11-21 16:11:26 UTC
postgis-database  | 2024-11-21 16:11:27.818 UTC [48] LOG:  database system is ready to accept connections
postgis-database  |  done
postgis-database  | server started
postgis-database  | CREATE DATABASE
postgis-database  |
postgis-database  |
postgis-database  | /usr/local/bin/docker-entrypoint.sh: sourcing /docker-entrypoint-initdb.d/10_postgis.sh
postgis-database  | CREATE DATABASE
postgis-database  | Loading PostGIS extensions into template_postgis
postgis-database  | CREATE EXTENSION
postgis-database  | CREATE EXTENSION
postgis-database  | You are now connected to database "template_postgis" as user "postgres".
postgis-database  | CREATE EXTENSION
postgis-database  | CREATE EXTENSION
postgis-database  | Loading PostGIS extensions into kfw-mastr
postgis-database  | CREATE EXTENSION
postgis-database  | CREATE EXTENSION
postgis-database  | You are now connected to database "kfw-mastr" as user "postgres".
postgis-database  | CREATE EXTENSION
postgis-database  | CREATE EXTENSION
postgis-database  |
postgis-database  | waiting for server to shut down....2024-11-21 16:11:31.789 UTC [48] LOG:  received fast shutdown request
postgis-database  | 2024-11-21 16:11:31.791 UTC [48] LOG:  aborting any active transactions
postgis-database  | 2024-11-21 16:11:31.792 UTC [48] LOG:  background worker "logical replication launcher" (PID 54) exited with exit code 1
postgis-database  | 2024-11-21 16:11:31.793 UTC [49] LOG:  shutting down
postgis-database  | 2024-11-21 16:11:31.795 UTC [49] LOG:  checkpoint starting: shutdown immediate
postgis-database  | .2024-11-21 16:11:33.735 UTC [49] LOG:  checkpoint complete: wrote 4469 buffers (27.3%); 0 WAL file(s) added, 0 removed, 2 recycled; write=0.375 s, sync=1.557 s, total=1.942 s; sync files=963, longest=1.003 s, average=0.002 s; distance=34812 kB, estimate=34812 kB
postgis-database  | 2024-11-21 16:11:33.743 UTC [48] LOG:  database system is shut down
postgis-database  |  done
postgis-database  | server stopped
postgis-database  |
postgis-database  | PostgreSQL init process complete; ready for start up.
postgis-database  |
postgis-database  | 2024-11-21 16:11:33.816 UTC [1] LOG:  starting PostgreSQL 15.4 (Debian 15.4-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
postgis-database  | 2024-11-21 16:11:33.816 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
postgis-database  | 2024-11-21 16:11:33.817 UTC [1] LOG:  listening on IPv6 address "::", port 5432
postgis-database  | 2024-11-21 16:11:33.821 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
postgis-database  | 2024-11-21 16:11:33.827 UTC [72] LOG:  database system was shut down at 2024-11-21 16:11:33 UTC
postgis-database  | 2024-11-21 16:11:33.832 UTC [1] LOG:  database system is ready to accept connections
```

Even if the script does not return to the prompt by itself it successfully runs. You can check by using a database tool (DBeaver or similar) to connect to the database. Connection infos you find within `docker-compose.yaml`. The port is 5512. 

To force a return to the prompt you can use `Ctrl+c`. But be careful: The command also stops your docker container. 


# Setup & prepare database for calculations

After you have finished the preceding steps, you can start setting up your database. For doing so: 

1. Set some paths within `config.yaml`. These are:
- REPO_ROOT
- INPUT_PATH
- OUTPUT_PATH
2. Start your docker container, for example via the docker desktop.
3. Activate your python environment named `kfw-mastr` using:

```
conda activate kfw-mastr
```

If you get an error like 

```
EnvironmentNameNotFound: Could not find conda environment: kfw-mastr
You can list all discoverable environments with `conda info --envs`.
```

, the aliasing of the environment did not work. In that case you can activate the environment using its path: 

```
conda activate your\favourite\path\miniconda3\Library\envs\kfw-mastr
```

Alternatively, you can try a re-setup of your environment. 

In some cases there might rise another error message within the console: 

```
Error while loading conda entry point: conda-libmamba-solver (module 'libmambapy' has no attribute 'QueryFormat')
```

In that case have a look at [https://github.com/conda/conda-libmamba-solver/issues/540](https://github.com/conda/conda-libmamba-solver/issues/540).

After you have done 1. to 3., from project root run:
   
```
python kfw_mastr/setup_database.py
```

**This creates the database, downloads the Marktstammdatenregister, and sets up the basic tables for calculations.**

Note: If `python kfw_mastr/setup_database.py` doesn't work, try to run it with path to config.yaml

```
python kfw_mastr/setup_database.py --config_path "your\favourite\path\config_file_can_have_any_name.yaml"
```

If you run into an error, this might be the case because of the following reasons: 

- Your environment is not set up correctly: Delete your environment -- manually or via console -- and repeat the corresponding steps. 
- The automatic download from Marktstammdatenregister does not work (server is down; connection fails; ...): You can file the *Gesamtdatenexport* by hand into the folder your\repo\folder\GermanRenewableEnergy\output\data\xml_download .
Tip: In the case of a re-run, it is not necessary to download the MaStR data again. The programm looks if the corresponding ZIP is within the folder containing to actual date within its name, e.g. *Gesamtdatenexport_20241127.zip*. If yes, no additional download is started. <br/>
IMPORTANT: Out script does not work with MaStR data newer then October 2024. This is because they contain some new data structures (tables) which can't be handled by the used baseline package which is [https://github.com/OpenEnergyPlatform/open-MaStR](https://github.com/OpenEnergyPlatform/open-MaStR) . 
- You don't have all the raw data needed: For processing reasons, we decided to make a lot of pre-calculations within the setup script. Therefore, all weather data is needed at that step already.
Since that data is quite large (> 10 GB), it is not part of this github repo. To solve that issue, we will find another storage for that data and will provide the corresponding link. 

If everything worked fine, your console output will look similar to that: 

```
File 'Netze.xml' is parsed.
Data is cleansed.
Bulk download and data cleansing were successful.
2024-11-27 10:14:32,708 - INFO - File vg5000_1231.zip already downloaded
2024-11-27 10:14:33,615 - INFO - Geo-boundaries loaded to database
2024-11-27 10:14:33,618 - INFO - File vg5000_1231.zip already downloaded
2024-11-27 10:14:34,811 - INFO - Geo-boundaries loaded to database
2024-11-27 10:14:34,994 - INFO - Created tables: ['results_wind_hourly', 'results_wind_monthly', 'results_wind_yearly', 'results_solar_hourly', 'results_solar_monthly', 'results_solar_yearly', 'unique_era5_coordinates', 'Calculation_wind', 'Calculation_solar', 'Calculation_solar_angles']
2024-11-27 10:14:34,994 - INFO - Load unique era5 coordinates to db
2024-11-27 10:14:35,910 - INFO - Table: Calculation_wind is updated. Columns: ['EinheitMastrNummer', 'EinheitBetriebsstatus', 'Nettonennleistung', 'Nabenhoehe', 'Typenbezeichnung', 'Laengengrad', 'Breitengrad', 'Postleitzahl', 'Gemeindeschluessel', 'Inbetriebnahmedatum'] are inserted from wind_extended into Calculation_wind
Inserting data to existing db: 100%|█████████████████████████████████████████| 37183/37183 [00:09<00:00, 3970.56rows/s]
2024-11-27 10:15:21,313 - INFO - Table: Calculation_solar is updated. Columns: ['EinheitMastrNummer', 'EinheitBetriebsstatus', 'Nettonennleistung', 'Hauptausrichtung', 'HauptausrichtungNeigungswinkel', 'Laengengrad', 'Breitengrad', 'Postleitzahl', 'Gemeindeschluessel', 'Inbetriebnahmedatum'] are inserted from solar_extended into Calculation_solar
Inserting data to existing db: 100%|█████████████████████████████████████| 4642261/4642261 [18:57<00:00, 4082.16rows/s]
2024-11-27 10:34:19,859 - INFO - Muncipality centroids calculated
Update Calculation_wind table with the nearest coordinates of ERA5 location points: 100%|█| 1/1 [00:03<00:00,  3.17s/it
Update Calculation_solar table with the nearest coordinates of ERA5 location points: 100%|█| 1/1 [08:52<00:00, 532.49s/
2024-11-27 10:43:16,400 - INFO - Batch-Mapping turbine types and hub heights into table: Calculation_wind. Batch size: 2000
2024-11-27 10:43:16,408 - INFO - SELECT "Calculation_wind"."Nabenhoehe" AS "Calculation_wind_Nabenhoehe", "Calculation_wind"."Nettonennleistung" AS "Calculation_wind_Nettonennleistung", "Calculation_wind"."Typenbezeichnung" AS "Calculation_wind_Typenbezeichnung", "Calculation_wind"."EinheitMastrNummer" AS "Calculation_wind_EinheitMastrNummer"
FROM "Calculation_wind"
Updating turbine types and hub heights. Committing do db in chunks: 100%|██████| 37183/37183 [00:11<00:00, 3123.34it/s]
2024-11-27 10:43:28,824 - INFO - Mapped turbine types and hub heights
2024-11-27 10:43:28,830 - INFO - Batch-Mapping azimuth and tilt angles into table: Calculation_solar. Batch size: 2000
2024-11-27 10:43:28,832 - INFO - SELECT "Calculation_solar"."Hauptausrichtung" AS "Calculation_solar_Hauptausrichtung", "Calculation_solar"."HauptausrichtungNeigungswinkel" AS "Calculation_solar_HauptausrichtungNeigungswinkel", "Calculation_solar"."EinheitMastrNummer" AS "Calculation_solar_EinheitMastrNummer"
FROM "Calculation_solar"
Updating azimuth and tilt angles. Committing do db in chunks: 100%|████████| 4642261/4642261 [10:19<00:00, 7488.95it/s]
2024-11-27 10:54:11,072 - INFO - Mapped azimuth and tilt angles
2024-11-27 10:54:11,563 - INFO - Computing solar positions for year: 2000
Computing solar positions for each ERA5 coordinate:   0%|                                     | 0/2745 [00:00<?, ?it/s]C:\Users\svenb\miniconda3\Library\envs\kfw-mastr\Lib\site-packages\pvlib\solarposition.py:263: UserWarning: Reloading spa to use numba
  warnings.warn('Reloading spa to use numba')
Computing solar positions for each ERA5 coordinate: 100%|██████████████████████████| 2745/2745 [00:41<00:00, 65.42it/s]
2024-11-27 10:54:53,802 - INFO - Writing solar angles to database

(kfw-mastr) D:\GRE\GermanRenewableEnergy>
```


## **Now follow the [`Step-by-step guide to run programme`](../README.md#step-by-step-guide-to-run-programme)**
