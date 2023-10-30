'''
Same as the test_run.ipynb notebook, but in a script format.

It runs SVS for a given case study.
'''

# (1) imports  ----------------------------------------------------------------
from pathlib import Path
from collections import namedtuple

from svspyed.utils.helper_functions import (
    check_csv_columns, get_layering_df, populate_layering_df,
)
from svspyed.input_preparation.prep_svs import ModelInputData
from svspyed.model.svs_model import SVSModel
# (1) imports  _________________________________________________________________

# (2) variables  --------------------------------------------------------------
# relevant paths
paths = dict(
    # input meteorological data required to drive SVS
    hourly_meteo = Path(
        "/Users/alireza_amani/repos/svspyed/svspyed/tests/test_case_study/data/"
        "hourly_meteo.csv"
    ),

    # SVS executable (compiled from Fortran source code)
    svs_exec = Path(
        "/Users/alireza_amani/repos/svspyed/svspyed/tests/test_case_study/svs_sep11"
    ),

    # the working dir where the run-time files will be stored
    working_dir = Path(
        "/Users/alireza_amani/repos/svspyed/svspyed/tests/test_case_study/"
    ),
)
# --------------------------------------------------------------------------- #

# a container for the properties of a soil type
SoilType = namedtuple("SoilType", [
    'code',
    'sand', 'clay',
    'wsat', 'wfc', 'wwilt',
    'ksat',
    'psisat', 'bcoef',
    'rhosoil',
])

# a container for a soil cover
Enclosure = namedtuple("Enclosure", [
    'layering',
    'soil_layers'
])

# a container for site-specific parameters required to configure SVS
SiteParams = namedtuple("SiteParams", [
    'deglat', 'deglng',
    'slop', 'zusl', 'ztsl',
    'observed_forcing', 'draindens', 'vf_type'
])

# a container for SVS internal parameters
SVSParams = namedtuple("SVSParams", [
    'SCHMSOL', 'lsoil_freezing_svs1', 'soiltext', 'read_user_parameters',
    'save_minutes_csv', 'water_ponding', 'KFICE', 'OPT_SNOW', 'OPT_FRAC',
    'OPT_LIQWAT', 'WAT_REDIS', 'tperm', 'user_wfcdp'
])
# --------------------------------------------------------------------------- #

# checks #
# check if the paths lead to existing files/directories
for key, path in paths.items():
    if not path.exists():
        raise FileNotFoundError(f"{key} path does not exist: {path}")

# (2) variables  ______________________________________________________________

# (3) setting case study parameters  ------------------------------------------
# Cover Material soil
CMsoil = SoilType(
    code = "CM",
    sand = 66.2, clay = 7.7, # percent
    wsat = 0.329, wfc = 0.05, wwilt = 0.01, # volumetric fraction
    ksat = 9.49E-06, # m.s-1
    psisat = 0.34, bcoef = 1.48, # mH2O, unitless
    rhosoil=1604.91, # kg.m-3
)


E1 = Enclosure(
    layering    = "190:5:CM",
    soil_layers = [CMsoil],

    # "190:5:CM" -> 190 cm total depth, divided into 5 cm increments, each with
    # the properties of CMsoil
    # could be changed to, for example, `15:2.5:CM + 175:5:CM` to have a 15 cm
    # top layer with 2.5 cm increments, and a 175 cm bottom layer with 5 cm
)

# parameters describing the site, identical for all enclosures
site_params = SiteParams(
    deglat = 45.82, deglng = -72.37, # degrees latitude and longitude

    slop = 0.02, zusl = 10.0, ztsl = 1.5, # slope, heights for momentum and temp
    observed_forcing = "height", draindens = 0.0, vf_type = 13
    # drainage density, vegetation fraction type
)

# check SVS source code and doc for more info on these parameters
svs_params = SVSParams(
    SCHMSOL = "SVS", lsoil_freezing_svs1 = ".TRUE.", soiltext = "NIL",
    read_user_parameters = 1, save_minutes_csv = 0, water_ponding = 0,
    KFICE = 0, OPT_SNOW = 2, OPT_FRAC = 1, OPT_LIQWAT = 1, WAT_REDIS = 0,
    tperm = 280.15, user_wfcdp = 16.90 * 0.01
)

# name of the directory containing the enclosure
HOST_DIR_NAME = "test_run"

# mapping of the required meteo variables to the labels in the forcing file
# needed for the `save_csv_as_met` function
meteo_cols = {
    "utc_dtime": "datetime_utc",
    "air_temperature": "Air temperature (degC)",
    "precipitation": "Precipitation (mm)",
    "wind_speed": "Wind speed (m.s-1)",
    "atmospheric_pressure": "Atmospheric pressure (Pa)",
    "shortwave_radiation": "Shortwave radiation (W.m-2)",
    "longwave_radiation": "Longwave radiation (W.m-2)",
    "specific_humidity": "Specific humidity (kg.kg-1)",
    "relative_humidity": "Relative humidity (%)"
}

DEWP_LABEL = "Dew point temperature (degC)"

# simulation start_date
START_DATE = "2018-182-04-00"
END_DATE = "2020-182-04-00"

# spinup end date
SPINUP_END_DATE = "2019-07-01 00:00:00"

# spinup date timezone
SPINUP_END_DATE_TZ = "America/Montreal"

# name of the parameters to assign for each layer: all fields except `code`
layer_params = tuple(field for field in CMsoil._fields if field != 'code')

# --------------------------------------------------------------------------- #
# processing the information to create the required files # ----------------- #

# read layering information and populate the dataframe
dfenc = get_layering_df(E1)
dfenc = populate_layering_df(dfenc, E1, site_params)

input_object = ModelInputData(
    work_dir_path=paths['working_dir'],
    soilcover_info=dfenc,
    metfile_path=paths['hourly_meteo'],
    exec_file_path=paths['svs_exec'],
    host_dir_name=HOST_DIR_NAME,
    meteo_col_names=meteo_cols,
    param_col_names={
        **{k: k for k in layer_params},
        **{k: k for k in site_params._fields},
    },
    model_params=svs_params._asdict(),
    start_date=START_DATE,
    end_date=END_DATE,
    spinup_end_date=SPINUP_END_DATE,
    time_zone=SPINUP_END_DATE_TZ,
)

# Checks # ------------------------------------------------------------------ #
# check if the columns in the meteo file are correct
print("Checking meteo file columns...")
check_csv_columns(paths['hourly_meteo'], meteo_cols)
# (3) setting case study parameters  __________________________________________

# (4) running SVS  -----------------------------------------------------------
# create an SVS instance
svs_for_case = SVSModel(input_object, True, True)

# run the model
svs_for_case.run_svs()

print("first five rows of the daily aggregated output")
print(svs_for_case.dfdaily_out.head())

print("first five rows of the hourly output")
print(svs_for_case.dfhourly_out.head())
# (4) running SVS  ____________________________________________________________
