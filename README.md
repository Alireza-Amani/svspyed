# svspyed — Python Wrapper for the SVS Land-Surface Model

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**svspyed** is a Python interface for the [SVS (Soil, Vegetation, and Snow)](https://wiki.gccollab.ca/MESH/MESH-SVS) land-surface model developed by Environment and Climate Change Canada (ECCC). It simplifies the preparation of model inputs, execution of single or ensemble simulations, and post-processing of outputs — all within a Pythonic workflow.

> **Important:** You must supply your own compiled SVS executable. The SVS source code is not redistributed here.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Package Structure](#package-structure)
- [Quick Start](#quick-start)
  - [Single Run](#single-run)
  - [Ensemble / Sensitivity Analysis Run](#ensemble--sensitivity-analysis-run)
- [Key Classes & API](#key-classes--api)
  - [ModelInputData](#modelinputdata)
  - [SVSModel](#svsmodel)
  - [PerturbAndRun](#perturbandrun)
- [Output Variables](#output-variables)
- [Obtaining the SVS Model](#obtaining-the-svs-model)
- [Contact](#contact)

---

## Features

| Feature | Description |
|---|---|
| **Input Management** | Automatically creates all SVS input files (`MESH_parameters.txt`, `MESH_input_run_options.ini`, `MESH_input_soil_levels.txt`, `basin_forcing.met`) from Python objects. |
| **Output Handling** | Reads SVS CSV output directly into `pandas` DataFrames, with automatic hourly → daily aggregation and spin-up period removal. |
| **Parallel Ensemble Runs** | Perturb one or more parameters across many scenarios and run them concurrently using `ProcessPoolExecutor`. Checkpoint output is saved as compressed Feather files. |
| **Copy & Clone Instances** | Deep-copy an `SVSModel` instance to a new working directory to create parameter-perturbed variants without repeating setup. |

---

## Requirements

- Python ≥ 3.10
- Dependencies (installed automatically):

| Package | Version |
|---|---|
| numpy | 1.26.4 |
| pandas | 2.2.0 |
| dask | 2024.2.0 |
| pyarrow | 15.0.0 |
| setuptools | 68.2.2 |

---

## Installation

```bash
git clone https://github.com/Alireza-Amani/svspyed
cd svspyed
pip install .
```

To install dependencies from the lock file instead:

```bash
pip install -r requirements.txt
```

---

## Package Structure

```
svspyed/
├── model/
│   └── svs_model.py          # SVSModel class — runs SVS and reads output
├── input_preparation/
│   ├── prep_svs.py            # PrepSVS base class + ModelInputData dataclass
│   ├── mesh_parameters.py     # MESHParameters — writes MESH_parameters.txt
│   └── mesh_input_run_options.py  # InputRunFile — writes MESH_input_run_options.ini
├── ensemble_run/
│   └── perturb_run.py         # PerturbAndRun — parallel ensemble simulations
└── utils/
    └── helper_functions.py    # Shared utilities
```

---

## Quick Start

### Single Run

```python
from pathlib import Path
from collections import namedtuple

from svspyed.utils.helper_functions import get_layering_df, populate_layering_df
from svspyed.input_preparation.prep_svs import ModelInputData
from svspyed.model.svs_model import SVSModel

# --- 1. Define soil types inline (no external CSV needed) -------------------

# Container for the physical properties of one soil type
SoilType = namedtuple("SoilType", [
    "code",
    "sand", "clay",           # percent
    "wsat", "wfc", "wwilt",   # volumetric fraction
    "ksat",                   # m s⁻¹
    "psisat", "bcoef",        # mH₂O, unitless
    "rhosoil",                # kg m⁻³
])

# Container that pairs a layering string with the soil types it references
Enclosure = namedtuple("Enclosure", ["layering", "soil_layers"])

# Container for site-specific (location/geometry) parameters
SiteParams = namedtuple("SiteParams", [
    "deglat", "deglng",
    "slop", "zusl", "ztsl",
    "observed_forcing", "draindens", "vf_type",
])

# Define one soil type
sandy_loam = SoilType(
    code    = "SL",
    sand    = 66.2, clay  = 7.7,
    wsat    = 0.329, wfc  = 0.05, wwilt = 0.01,
    ksat    = 9.49e-06,
    psisat  = 0.34, bcoef = 1.48,
    rhosoil = 1604.91,
)

# Layering string: "<total depth cm>:<layer thickness cm>:<soil code>"
# Multiple blocks can be combined: e.g. "15:2.5:SL + 175:5:SL"
enclosure = Enclosure(
    layering    = "190:5:SL",   # 190 cm total, 5 cm layers, all sandy_loam
    soil_layers = [sandy_loam],
)

site_params = SiteParams(
    deglat = 45.82, deglng = -72.37,
    slop   = 0.02,  zusl   = 10.0, ztsl = 1.5,
    observed_forcing = "height", draindens = 0.0, vf_type = 13,
)

# Build the soilcover DataFrame from the objects above
dfenc = get_layering_df(enclosure)
dfenc = populate_layering_df(dfenc, enclosure, site_params)

# Parameter names used in dfenc (all SoilType fields except 'code', plus SiteParams)
layer_params = tuple(f for f in sandy_loam._fields if f != "code")

# --- 2. Bundle all inputs ---------------------------------------------------

required_data = ModelInputData(
    work_dir_path  = Path("/path/to/working/dir"),
    host_dir_name  = "my_svs_run",
    soilcover_info = dfenc,          # DataFrame built above; a Path to a CSV also works
    metfile_path   = Path("/path/to/meteo.csv"),
    exec_file_path = Path("/path/to/SVS_executable"),
    meteo_col_names = {
        "utc_dtime":            "datetime_utc",
        "air_temperature":      "Tair_degC",
        "precipitation":        "Precip_mm",
        "wind_speed":           "Wind_ms",
        "atmospheric_pressure": "Pres_Pa",
        "shortwave_radiation":  "SW_Wm2",
        "longwave_radiation":   "LW_Wm2",
        "specific_humidity":    "q_kgkg",
        "relative_humidity":    "RH_pct",
    },
    param_col_names = {
        **{k: k for k in layer_params},
        **{k: k for k in site_params._fields},
    },
    model_params = {
        "SCHMSOL": "SVS", "lsoil_freezing_svs1": ".TRUE.", "soiltext": "NIL",
        "read_user_parameters": 1, "KFICE": 0, "OPT_SNOW": 2,
    },
    start_date      = "2018-183-04-00",   # YYYY-JDAY-HH-MM
    end_date        = "2019-001-00-00",
    spinup_end_date = "2019-01-01 00:00:00",
    time_zone       = "America/Montreal",
    model_tsetp     = 5,
)

# --- 3. Instantiate — creates all SVS input files automatically -------------
svs = SVSModel(required_data, verbose=True)

# --- 4. Run -----------------------------------------------------------------
svs.run_svs()

# --- 5. Inspect output ------------------------------------------------------
print(svs.dfhourly_out.head())
print(svs.dfdaily_out.head())
```

### Ensemble / Sensitivity Analysis Run

```python
from svspyed.ensemble_run.perturb_run import PerturbAndRun

parameter_scenarios = {
    "scenario_1": {"sand": [10, 10, 10], "clay": [4, 4, 4]},
    "scenario_2": {"sand": [14, 14, 14], "clay": [6, 6, 6]},
}

met_paths = [Path("/path/to/forcing_0.met"), Path("/path/to/forcing_1.met")]

runner = PerturbAndRun(
    svs_default_input=required_data,
    parameter_scenarios=parameter_scenarios,
    met_paths=met_paths,
    njobs=4,         # number of parallel CPU cores
    verbose=True,
)

runner.run_all_parallel(output_time_scale="daily")

# Combined output across all scenarios
print(runner.dfoutput.head())
# Parameter scenario table
print(runner.dfscenarios)
```

---

## Key Classes & API

### `ModelInputData`

A `dataclass` that bundles all information needed to configure an SVS run.

| Parameter | Type | Description |
|---|---|---|
| `work_dir_path` | `Path` | Directory where the run folder is created. |
| `host_dir_name` | `str` | Name of the run folder created inside `work_dir_path`. |
| `soilcover_info` | `Path` or `DataFrame` | Soil layer info (columns: `thickness`, `depth`, and parameter columns). |
| `metfile_path` | `Path` | CSV with hourly meteorological forcing data. |
| `exec_file_path` | `Path` | Path to the compiled SVS executable. |
| `meteo_col_names` | `dict` | Mapping from required met variable keys to CSV column names. |
| `param_col_names` | `dict` | Mapping from SVS parameter names to column names in `soilcover_info`. |
| `model_params` | `dict` | Additional internal model parameters (e.g. `KFICE`). |
| `init_conds` | `str` or `dict` | Initial state variable values. Use `"auto"` for spin-up. |
| `start_date` | `str` | Simulation start: `YYYY-JDAY-HH-MM`. |
| `end_date` | `str` | Simulation end: `YYYY-JDAY-HH-MM`. |
| `spinup_end_date` | `str` | End of spin-up period: `YYYY-MM-DD HH:MM:SS`. |
| `time_zone` | `str` | Local time zone string (e.g. `"America/Montreal"`). |
| `copy_metfile` | `Path` | Optionally copy a pre-built `.met` file instead of generating one. |
| `model_tsetp` | `int` | Model time step in minutes (default: `5`). |

### `SVSModel`

Inherits from `PrepSVS`. On instantiation it prepares all input files. Key methods:

| Method | Description |
|---|---|
| `run_svs()` | Run SVS synchronously; blocks until finished and populates `dfhourly_out` / `dfdaily_out`. |
| `run_svs_parallel()` | Launch SVS in a child process (non-blocking); returns the `Process` object. |
| `read_output()` | Re-read output CSV into `dfhourly_out` and recompute `dfdaily_out`. |
| `remove_host_folder_after_run()` | Delete the host directory and all its contents. |

Key attributes after `run_svs()`:

| Attribute | Description |
|---|---|
| `dfhourly_out` | `DataFrame` of hourly output variables (UTC index). |
| `dfdaily_out` | `DataFrame` of daily aggregated output (local time index, spin-up removed). |
| `host_dir_path` | Absolute path to the run directory. |

### `PerturbAndRun`

Runs many parameter scenarios in parallel.

| Method | Description |
|---|---|
| `run_all_parallel(output_time_scale, keepcols, effort_id)` | Create instances, run them in batches of `njobs`, collect output. |
| `create_param_scen_df()` | Build `dfscenarios` DataFrame from `parameter_scenarios`. |

Key attributes after `run_all_parallel()`:

| Attribute | Description |
|---|---|
| `dfoutput` | Combined output `DataFrame` for all scenarios (column `member` identifies the scenario). |
| `dfscenarios` | `DataFrame` summarising the parameter values for each scenario. |

---

## Output Variables

After a run, `dfdaily_out` contains (among others):

| Column | Description |
|---|---|
| `ET` | Daily evapotranspiration |
| `DRAI` | Daily drainage |
| `PCP` | Daily precipitation |
| `OVFLW` | Daily overland flow |
| `WSOIL_N` | Mean volumetric soil moisture for layer N |
| `TPSOIL_N` | Mean soil temperature for layer N |
| `SNOMA` | Snow mass |
| `SNODP` | Snow depth |

The hourly `dfhourly_out` retains all columns from `svs1_soil_hourly.csv` plus a `date_utc` column.

---

## Obtaining the SVS Model

The SVS land-surface model is developed and maintained by **Environment and Climate Change Canada (ECCC)**. Its source code is not included in this repository.

For information on obtaining the SVS source code and compiling the executable, please visit:
👉 https://wiki.gccollab.ca/MESH/MESH-SVS

---

## Contact

Questions, bug reports, or collaboration inquiries are welcome. Feel free to open an issue or reach out directly via GitHub.

