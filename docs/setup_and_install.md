# Installation

#### 1. Install package manager _miniconda_

Download the latest miniconda version [here](https://docs.anaconda.com/miniconda/).

#### 2. Clone repository

```
git clone https://github.com/chrwm/kfw-mastr.git
```
#### 3. Set python path

##### Using `command line`

1. Check what your python path is
```
echo %PYTHONPATH%
```
2. Set python path to repository root
```
set PYTHONPATH=C:\path\to\your\folder_where_you_cloned_the_repo_into
```

##### Using `Powershell`

1. Check what your python path is
```
echo $env:PYTHONPATH
```
2. Set python path to repository root
```
$env:PYTHONPATH = "C:\path\to\your\python\modules"
```

#### 4. Setup environment and install packages

In miniconda terminal: Navigate to the REPO_ROOT (the folder where you cloned the repo into) and run the 
conda code below: 
```
conda install mamba -n base -c conda-forge
```
```
mamba env create -f environment.yaml
```

This yields a virtual environment named `kfw-mastr` with Python 3.11 and all necessary packages to run the code.

#### 5. Install docker & create database

1. Download docker for Windows [here](https://docs.docker.com/desktop/install/windows-install/).
1. Start docker as administrator.
1. Open `command line` as administrator.
1. Navigate to repo root.
1. Run
```
docker-compose up
```

This creates the docker infrastructure.

# Setup & prepare database for calculations

1. In your IDE set `kfw-mastr` as environment.
2. If you're using the miniconda terminal or `command line` to run the code, then make sure to activate the `kfw-mastr` environment with:
```
conda activate kfw-mastr
```
3. From project root, run 
```
python kfw_mastr/setup_database.py
```

**This creates the database, downloads the Marktstammdatenregister, and sets up the basic tables for calculations.**

Note: If `python kfw_mastr/setup_database.py` doesn't work, try to run it with path to config.yaml

```
python kfw_mastr/setup_database.py --config_path "C:\Users\user\your\favourite\path\config_file_can_have_any_name.yaml"
```



## **Now follow the [`Step-by-step guide to run programme`](../README.md#step-by-step-guide-to-run-programme)**