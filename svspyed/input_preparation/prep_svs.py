# <<< doc >>> -----------------------------------------------------------------
'''
This module holds the definition of:
    - PrepSVS: a class that takes care of preparing SVS input files.

    - ModelInputData: a dataclass that stores the info needed to instantiate an
    SVSModel object.

    - several helper functions used inside PrepSVS.

To do:
 - check whether the spinup_end_date is not after simulation end_date
'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------
from dataclasses import dataclass
from datetime import datetime
from copy import deepcopy
import os
from pathlib import Path
import shutil
from typing import Union

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

from .mesh_input_run_options import InputRunFile
from .mesh_parameters import MESHParameters
from ..utils.helper_functions import assert_file_exist, assert_dir_exist, remove_file_dir
from ..utils.helper_functions import check_start_end_date_format
from ..utils.helper_functions import check_spinup_end_date_format
# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------


class PrepSVS:
    '''
    Creates the required files needed to run SVS.

    Parameters
    ----------
    required_data : ModelInputData
        A dataclass that contains the required information for creating the
        necessary input files for SVS.

    remove_old_host_folder : bool, default=False
        If True, the pre-existing host directory, that has the exact name
        as the one that are to be created for the instance, will be removed.

    verbose : bool, default=False
        If set to True, the user will recieve report after performing each
        task.

    Attributes
    ----------
    host_dir_path : str
        The absolute path to the folder that contains the SVS input files and
        output.

    dfmet : pandas Dataframe
        The dataframe that contains the hourly meteorological data that will
        be used to create the basin_forcing.met file.

    first_tstep : str
        The first time-step in `dfmet` with the format of YYYYMMDDHH.
        This will be used in the creation of `MESH_input_run_options.ini`.

    last_tstep : str
        The last time-step in `dfmet` with the format of YYYYMMDDHH.

    input_run_file : Input_Run_File
        Embodies the `MESH_input_run_options.ini` file.

    mesh_param_file : MESH_parameters
        Embodies the `MESH_parameters.txt` file.

    svs_exe_name : str
        The name of the SVS executable file inside the host folder.

    Methods
    -------
    create_host_folder()
        Create the host folder in which the SVS model will run.

    create_soil_file()
        Create `MESH_input_soil_levels.txt` file.

    create_met()
        Create `basin_forcing.met` file for SVS and save it in the host folder.

    remove_host_folder_after_run()
        Remove the host folder and all its content.
    '''

    def __init__(
        self, required_data,
        remove_old_host_folder: bool = False,
        verbose: bool = False
    ):

        self.required_data = deepcopy(required_data)
        self.remove_old_host_folder = remove_old_host_folder
        self.verbose = verbose

        # create the host folder, soil layering, basing forcing file
        self.create_host_folder()
        self.create_soil_file()

        if self.required_data.copy_metfile is not None:
            copy_metfile_to_host_dir(
                self.required_data.copy_metfile, self.host_dir_path,
                verbose=self.verbose
            )
            self.first_tstep = self.required_data.metfile_t0.replace('-', '')
            self.last_tstep = self.required_data.metfile_tn.replace('-', '')

            self.first_tstep, self.last_tstep = get_first_last_timestep_from_met_file(
                self.required_data.copy_metfile
            )
        else:
            self.create_met()

        # create an instant of `Input_Run_File` class
        # this will auto. create and saves the `MESH_input_run_options.ini`
        # inside the host folder
        self.input_run_file = InputRunFile(
            host_dir_path=self.host_dir_path,
            first_tstep=self.first_tstep, last_tstep=self.last_tstep,
            start_date=self.required_data.start_date,
            end_date=self.required_data.end_date,
            model_tsetp=self.required_data.model_tsetp,
            verbose=self.verbose
        )

        # create an instant of `MESH_parameters` class
        # this will auto. create and saves the `MESH_parameters.txt`
        # inside the host folder
        self.mesh_param_file = MESHParameters(
            lyinfo=self.required_data.soilcover_info,
            param_names_cols=self.required_data.param_col_names,
            model_params=self.required_data.model_params,
            host_dir_path=self.host_dir_path,
            init_conds=self.required_data.init_conds,
            verbose=self.verbose
        )

        # copy the SVS exe into the host folder
        self.svs_exe_name = copy_exec_file(
            exec_file_path=self.required_data.exec_file_path,
            host_dir_path=self.host_dir_path,
            verbose=self.verbose,
            exec_file_name=self.required_data.exec_file_name
        )

        # create the output folder
        create_output_folder(self.host_dir_path)

    def remove_host_folder_after_run(self):
        '''
        Remove the host folder and all its content.
        '''

        remove_file_dir(self.host_dir_path)

    def create_host_folder(self) -> None:
        '''
        Create the host folder in which the SVS model will run.
        '''

        # the abs path to the host folder (to be created)
        self.host_dir_path = os.path.join(
            self.required_data.work_dir_path, self.required_data.host_dir_name
        )

        # remove the pre-existing host folder with the same name
        if self.remove_old_host_folder:
            remove_file_dir(self.host_dir_path)

        # create the host folder
        # first make sure it does not already exist
        assert (not os.path.isdir(self.host_dir_path)), (
            F"A folder with the name: `{self.required_data.host_dir_name}` already "
            F"exists in the dir:`{self.required_data.work_dir_path}`.\n"
            F"Please consider changing the value of the `host_dir_name` parameter."
        )

        try:
            os.mkdir(self.host_dir_path)
        except FileExistsError:
            pass

        if self.verbose:
            # report back
            message = (
                F"\na folder named `{self.required_data.host_dir_name}` is created inside the "
                F"dir: `{self.required_data.work_dir_path}`\n"
            )
            print(message)

    def create_soil_file(self):
        '''
        Create `MESH_input_soil_levels.txt` file.
        '''

        # read the dataframe that contains the info for the soil layering
        if not isinstance(self.required_data.soilcover_info, pd.DataFrame):
            lyinfo_df = pd.read_csv(
                self.required_data.soilcover_info, index_col=False).dropna(axis=0)
        else:
            lyinfo_df = self.required_data.soilcover_info.copy()

        # the first column of the soil file must be the thicknesses
        # and the second will be the depth
        # Lets select the desired columns
        lyinfo_df = lyinfo_df.loc[:, ["thickness", "depth"]]

        # convert from cm to m
        lyinfo_df["depth"] /= 100
        lyinfo_df["thickness"] /= 100

        # save it as a text file, columns separated by several spaces
        path_save = os.path.join(self.host_dir_path,
                                 "MESH_input_soil_levels.txt")

        lyinfo_df.to_csv(path_save, sep="\t", header=False, index=False,
                         float_format="%.3f")

        if self.verbose:
            # report back
            message = (
                F"\nThe `MESH_input_soil_levels.txt` file is created inside "
                F"the host folder located at \ndir: `{self.host_dir_path}`.\n"
                F"In this file {len(lyinfo_df['thickness'])} soil layers "
                F"are defined.\n"
            )
            print(message)

    def create_met(self):
        '''
        Create `basin_forcing.met` file for SVS and save it in the host folder.
        '''

        col_names = self.required_data.meteo_col_names
        metfile_path = self.required_data.metfile_path

        # read the hourly meteo dataframe
        assert_file_exist(metfile_path)
        dfmet = pd.read_csv(metfile_path)

        # check units of the variables
        check_metvar_units(dfmet, col_names)

        # add the required time columns in the dataframes
        dfmet, self.first_tstep, self.last_tstep = create_dtime_vars(
            dfmet, col_names["utc_dtime"]
        )

        # process the precipitation for the dataframe
        dfmet = process_pcpn(dfmet, col_names["precipitation"], calc_psnow,
                             **dict(air_temp=dfmet.loc[:, col_names["air_temperature"]],
                                    rel_humidity=dfmet.loc[:,
                                                           col_names["relative_humidity"]]
                                    )
                             )

        # let the instance own the dataframe
        self.hourlymeteo_data = dfmet.copy()

        # order the dataframe columns
        dfmet = order_dfmet(dfmet, col_names)

        # let the instance own the text file
        self.met_file = dfmet.to_string(
            header=False, col_space=4, index=False)

        # save the file
        save_path = os.path.join(self.host_dir_path, "basin_forcing.met")
        with open(save_path, "w", encoding="utf-8") as mfile:
            mfile.write(self.met_file)

        if self.verbose:
            # report back
            message = (
                F"\nThe `basin_forcing.met` is created inside the "
                F"dir: `{self.host_dir_path}`\n"
            )
            print(message)


@dataclass
class ModelInputData:
    '''
    A dataclass to encapsulate all the information needed to prepare the
    required input files for running SVS.

    Parameters
    ----------
    work_dir_path : Union[str, Path]
        The absolute path to the directory in which the `SVSModel` instance
        will create a directory (folder) to host the files required and
        produced by the model run.

    soilcover_info : Union[Path, DataFrame]
        The absolute path to the .csv file that contains info on the parameters
        of the soil cover of interest.
        OR
        A pandas dataframe that contains the same info as the .csv file.

    metfile_path : Union[str, Path]
        The absolute path to the dataframe that contains the hourly meteo
        data required by SVS, in the correct unit. Additionally it must
        contain a date-time (utc) column.
        Check the model online guide for the expected units.

    exec_file_path : Union[str, Path]
        The absolute path to the SVS executable file. It will be used to copy
        it into the newly created host folder.

    host_dir_name : str
        The name to assign to the host folder which contains the files
        required and produced by the model run. This folder will be created
        inside `work_dir_path`.

    meteo_col_names : dict
        A dict that its values are the names of the columns in the dataframe
        that contains the hourly meteorological variables required to create
        the .met file for SVS.
        Please define and provide values for the following keys:

        ['utc_dtime', 'air_temperature', 'precipitation', 'wind_speed',
        'atmospheric_pressure', 'shortwave_radiation', 'longwave_radiation',
        'specific_humidity', 'relative_humidity']

        * Relative humidity will be used in the precipitation phase
        discrimination.

    param_col_names : dict
        A dict having the name of columns corresponding to the expceted
        parameters which are to be read from `dy_lyinfo`.
        Example:
        >>> # read values for the SVS `sand` parameter from a column named
        >>> # `the_sand` inside the `dy_lyinfo`:
        >>> param_names_cols = dict(sand = 'the_sand')

    model_params : dict
        A dictionary that contains the values for some of the internal model
        parameters that are placed inside `MESH_parameters.txt`.
        e.g. `KFICE`, `user_wfcdp`, ...

    init_conds : Union[str, dict], default="auto"
        If the model is going to go through a spin-up period, then the
        default value of `auto` should be the choise. This means that some
        reasonable values will be assigned to the state variables.
        Otherwise, provide a dict with values for one or more of the state
        variables you wish to set differently.
        Example:
        >>> init_conds = dict(wsoil=[0.15, 0.15, 0.15, 0.15])

        * Bear in mind that in the example above, `len(init_conds["wsoil"])`
        must be equal to the number of soil layers.

    start_date : str, default=""
        The simulation start time step; provide it as: `YYYY-JDAY-HH-MM`.
        Example:
        >>> start_date = "2017-184-05-00"
        `start_date` must occur on or after `first_tstep`.

        * JDAY is the day of the year.

    end_date : str, default=""
        The simulation end time step; provide it as: `YYYY-JDAY-HH-MM`.
        Example:
        >>> end_date = "2017-184-05-00"

    output_csvfile_name : str, default="svs1_soil_hourly.csv"
        The name of the output csv file that contains the hourly output
        variables. This file is located in the output folder inside the
        `host_dir_path`.

        Note: In my version of SVS all the output variables, flux and state,
        are located in a single file.

    spinup_end_date : str, default='2018-07-01 00:00:00'
            The end date of the spinup period (one day after it actually),
            in the following format: '2018-07-01 00:00:00'.
            Its time zone must be the site-local tz.
            Any daily aggregated data before this date is considered to be
            part of the spin-up period and will not be present in `dfdaily_out`.
            Provide an empty string in case there was no spinup period.

    time_zone : str, default='America/Montreal'
        The local time zone of the case study (site).
        This will be used to convert `spinup_end_date` to a UTC datetime value.

    exec_file_name : str, default="SVS_exe"
        The name of the SVS executable file which is saved inside the host dir.

    copy_metfile : PATH, default=None
        If you wish to copy the meteo file into the host dir, provide its
        absolute path here. This is useful if you wish to speed up the process
        of creating a .met file based on a large meteo dataframe.

    metfile_t0 : str, default=""
        The first time-step of the meteo file. Provide it as: `YYYY-JDAY-HH-MM`.

    metfile_tn : str, default=""
        The last time-step of the meteo file. Provide it as: `YYYY-JDAY-HH-MM`.

    '''

    work_dir_path: Union[str, Path]
    soilcover_info: Union[Path, DataFrame]
    metfile_path: Union[str, Path]
    exec_file_path: Union[str, Path]
    host_dir_name: str
    meteo_col_names: dict
    param_col_names: dict
    model_params: dict
    init_conds: Union[str, dict] = "auto"
    start_date: str = ""
    end_date: str = ""
    output_csvfile_name: str = "svs1_soil_hourly.csv"
    spinup_end_date: str = "2018-07-01 00:00:00"
    time_zone: str = "America/Montreal"
    exec_file_name: str = "SVS_exe"
    copy_metfile: Union[str, Path] = None
    metfile_t0: str = ""
    metfile_tn: str = ""
    model_tsetp: int = 5  # in minutes

    # post initiation checks

    def __post_init__(self):
        # dir and file exist assertions
        assert_dir_exist(self.work_dir_path)
        assert_file_exist(self.exec_file_path)

        if isinstance(self.soilcover_info, Path) or isinstance(self.soilcover_info, str):
            assert_file_exist(self.soilcover_info)

        if self.copy_metfile is not None:
            assert_file_exist(self.copy_metfile)
        else:
            assert_file_exist(self.metfile_path)
            check_meteo_colnames_dict(self.meteo_col_names)

        check_necessary_parameters(self.param_col_names)

        # check the format of the `spinup_end_date`
        check_spinup_end_date_format(self.spinup_end_date)

        # check model time step cannot be less than 1
        assert (self.model_tsetp >= 1), (
            F"The model time step cannot be less than 1. "
            F"Current value: {self.model_tsetp}"
        )
        assert (isinstance(self.model_tsetp, int)), (
            F"The model time step must be an integer. "
            F"Current value: {self.model_tsetp}"
        )

    # modifying the setattr to check for date formats
    def __setattr__(self, name, value):
        if (name in ["start_date", "end_date", "metfile_t0", "metfile_tn"]):
            if value:  # if its not an empty string
                check_start_end_date_format(value)
                object.__setattr__(self, name, value)
        object.__setattr__(self, name, value)


def copy_metfile_to_host_dir(
    metfile_path: Path, host_dir_path: Path, verbose: bool = False
):
    """Copy the metfile into the host dir.

    Parameters
    ----------
    metfile_path : Path
        The path to the metfile.

    host_dir_path : Path
        The path to the host dir.

    verbose : bool, default=False
        If True, print the path to the copied metfile.

    """
    metfile_path = Path(metfile_path)
    host_dir_path = Path(host_dir_path)
    metfile_name = metfile_path.name
    new_metfile_path = host_dir_path / metfile_name

    if new_metfile_path.stem != "basin_forcing":
        new_metfile_path = new_metfile_path.with_name("basin_forcing.met")

    shutil.copy(metfile_path, new_metfile_path)
    if verbose:
        print(f".met file copied to: {new_metfile_path}")


def copy_exec_file(
    exec_file_path: str, host_dir_path: str, verbose: bool = False,
    exec_file_name: str = "SVS_exe"

) -> str:
    '''
    Copy the SVS executable file into the host folder.

    Parameters
    ----------
    exec_file_path : str or path object
        The absolute file to the SVS executable file.

    host_dir_path : str
        The absolute path to the folder that contains the SVS input files and
        output.

    verbose : bool
        If set to True, will report back when function is called.

    Returns
    -------
    svs_exe_name : str
        The name of the SVS executable file inside the host folder.
    '''

    # we choose our own name for the SVS exec name inside the host folder
    the_dst = os.path.join(host_dir_path, exec_file_name)
    shutil.copy(src=exec_file_path, dst=the_dst)

    if verbose:
        print(
            F"The SVS executable file is copied into the host folder. "
            F"named `{exec_file_name}`.\n"
        )

    return exec_file_name


def create_output_folder(host_dir_path: str, folder_name: str = "output"):
    '''
    Create the output folder in the host folder.

    Parameters
    ----------
    host_dir_path : str
        The absolute path to the folder that contains the SVS input files and
        output.

    folder_name : str
        The name of the folder that SVS will store its output files in.

    '''
    try:
        os.mkdir(os.path.join(host_dir_path, folder_name))
    except FileExistsError:
        print(
            F"The folder `{folder_name}` already exists in the host folder. "
            F"SVS will overwrite any existing files in this folder."
        )


def check_meteo_colnames_dict(meteo_col_names: dict) -> None:
    '''
    Check if the `meteo_col_names` dict has the required keys.

    Parameter
    ---------
    meteo_col_names : dict
        A dict that contains the names of the relevant columns in the dataframe
        that contains the hourly meteorological variables required to create
        the .met file for SVS.

    '''

    set_dict_keys = set(meteo_col_names.keys())
    set_required_keys = set(['utc_dtime', 'air_temperature', 'precipitation',
                             'wind_speed', 'atmospheric_pressure', 'shortwave_radiation',
                             'longwave_radiation', 'specific_humidity', 'relative_humidity'
                             ])
    assert (set_required_keys.issubset(set_dict_keys)), (
        F"Make sure the `meteo_col_names` dict include the following keys: \n"
        F"{set_required_keys}\n"
        F"Missing key(s): {set_required_keys.difference(set_dict_keys)}\n"
    )


def check_necessary_parameters(param_col_names: dict) -> None:
    '''
    Check if the `param_col_names` dict has the required keys.

    Parameter
    ---------
    param_col_names : dict
        A dict having the name of columns corresponding to the expceted
        parameters which are to be read from `dy_lyinfo`.

    '''

    set_dict_keys = set(param_col_names.keys())
    set_required_keys = {
        "sand", "clay", "slop", "observed_forcing", "zusl", "ztsl", "deglat",
        "deglng", "vf_type", "draindens"
    }
    assert (set_required_keys.issubset(set_dict_keys)), (
        F"Make sure the `param_col_names` dict include the following keys: \n"
        F"{set_required_keys}\n"
        F"Note: these are the necessary parameters that must exist inside the\n"
        F"`MESH_parameters.txt` file.\n"
        F"Missing key(s): {set_required_keys.difference(set_dict_keys)}\n"
    )


def order_dfmet(dfmet, col_names: dict):
    '''
    Order the `dfmet` dataframe per the ordering expected by SVS.
    Additionally, will round the values for some of the variables.

    Parameters
    ----------
    dfmet : pandas Dataframe
        The dataframe that contains the meteo variables for SVS plus a column
        named `dtime_col_name` that is convertible to pd.Timestamp.

    col_names : dict
        A dict that contains the names of the relevant columns in the dataframe
        that contains the hourly meteorological variables required to create
        the .met file for SVS. Please define and provide values for the
        following keys:
        ['utc_dtime', 'air_temperature', 'precipitation', 'wind_speed',
        'atmospheric_pressure', 'shortwave_radiation', 'longwave_radiation',
        'specific_humidity', 'relative_humidity']
        Relative humidity will be used in the precipitation phase discrimination.

    Returns
    -------
    dfmet : pandas Dataframe

    '''

    # order the dataframe as expected by SVS
    ordered_cols = [
        "HOUR", "MINS", "JDAY", "YEAR",
        col_names["shortwave_radiation"], col_names["longwave_radiation"],
        "Precipitation rate (mm/s)", col_names["air_temperature"],
        col_names["specific_humidity"], col_names["wind_speed"],
        col_names["atmospheric_pressure"],
        "Rainfall rate (mm/s)", "Snowfall rate (mm/s)"
    ]
    dfmet = dfmet.loc[:, ordered_cols]

    # adjust the digits and precision of the variables, to help writing a clean
    # .met file
    # radiation vars, temperature, wind speed and pressure
    #  two decimals vars
    twod_vars = [
        col_names["shortwave_radiation"], col_names["longwave_radiation"],
        col_names["air_temperature"], col_names["wind_speed"],
        col_names["atmospheric_pressure"]
    ]
    dfmet.loc[:, twod_vars] = np.round(dfmet.loc[:, twod_vars], 2)

    return dfmet


def check_metvar_units(dfmet, col_names: dict) -> None:
    '''
    Check the hourly meteorological data to ensure they have the correct
    units before creating the .met file

    Parameters
    ----------
    dfmet : pandas Dataframe
        The dataframe that contains the meteo variables for SVS.

    col_names : dict
        A dict that contains the names of the relevant columns in the dataframe
        that contains the hourly meteorological variables required to create
        the .met file for SVS. Please define and provide values for the
        following keys:
        ['utc_dtime', 'air_temperature', 'precipitation', 'wind_speed',
        'atmospheric_pressure', 'shortwave_radiation', 'longwave_radiation',
        'specific_humidity', 'relative_humidity']
        Relative humidity will be used in the precipitation phase discrimination.

    '''

    dfmet = dfmet.loc[:, :]

    # extract the variables of interest
    air_temp = dfmet.loc[:, col_names["air_temperature"]].to_numpy()
    shortwave_rad = dfmet.loc[:, col_names["shortwave_radiation"]].to_numpy()
    longwave_rad = dfmet.loc[:, col_names["longwave_radiation"]].to_numpy()
    wind_speed = dfmet.loc[:, col_names["wind_speed"]].to_numpy()
    atms_press = dfmet.loc[:, col_names["atmospheric_pressure"]].to_numpy()
    precipitation = dfmet.loc[:, col_names["precipitation"]].to_numpy()
    shumidity = dfmet.loc[:, col_names["specific_humidity"]].to_numpy()
    relhumidity = dfmet.loc[:, col_names["relative_humidity"]].to_numpy()
#     date_utc = dfmet.loc[:, col_names["utc_dtime"]].to_numpy()

    # check the values of the meteo variables for their unit and potential invalid
    # values.
    assert (np.all(air_temp > -50) and np.all(air_temp < 60)), (
        "Please check the values of the air temperature column."
    )
    assert (np.all(shortwave_rad >= 0) and np.all(shortwave_rad < 2000)), (
        "Please check the values of the shortwave radiation column."
    )
    assert (np.all(longwave_rad >= 0) and np.all(longwave_rad < 2000)), (
        "Please check the values of the longwave radiation column."
    )
    assert (np.all(wind_speed >= 0) and np.all(wind_speed < 200)), (
        "Please check the values of the wind speed column."
    )
    assert (np.all(atms_press >= 70000) and np.all(atms_press < 110000)), (
        "Please check the values of the atmospheric pressure column.\n"
        "Make sure that the values are in Pascal."
    )
    assert (np.all(precipitation >= 0) and np.all(precipitation < 500)), (
        "Please check the values of the precipitation column."
    )
    assert (np.all(shumidity >= 0.00001) and np.all(shumidity < 0.3)), (
        "Please check the values of the specific humidity column."
    )
    assert (np.all(relhumidity >= 0) and np.all(relhumidity <= 100)), (
        "Please check the values of the relative humidity column."
    )


def create_dtime_vars(dfmet, dtime_col_name: str):
    '''
    Create the four date-time variables used in the .met file for SVS.

    Parameters
    ----------
    dfmet : pandas Dataframe
        The dataframe that contains the meteo variables for SVS plus a column
        named `dtime_col_name` that is convertible to pd.Timestamp.

    dtime_col_name : str
        The name of the date-time (utc) column in `dfmet`.

    Returns
    -------
    dfmet : pandas Dataframe

    first_tstep : str
        The first time-step in `dfmet` with the format of YYYYMMDDHH.
        This will be used in the creation of `MESH_input_run_options.ini`.

    last_tstep : str
        The last time-step in `dfmet` with the format of YYYYMMDDHH.

    '''

    dfmet = dfmet.copy()

    # create columns for HOUR, MINS, JDAY, YEAR
    # first convert the datetime column into a proper dtype
    dfmet[dtime_col_name] = pd.to_datetime(
        dfmet[dtime_col_name], utc=True
    )

    # now create the required columns
    dfmet["HOUR"] = dfmet[dtime_col_name].dt.hour
    dfmet["MINS"] = dfmet[dtime_col_name].dt.minute
    dfmet["JDAY"] = dfmet[dtime_col_name].dt.dayofyear
    dfmet["YEAR"] = dfmet[dtime_col_name].dt.year

    # extract the first(/last) time step in the appropriate format to be used in the
    # creation process of `MESH_input_run_options.ini`
    first_tstep = dfmet.loc[dfmet.index[0], dtime_col_name]
    last_tstep = dfmet.loc[dfmet.index[-1], dtime_col_name]

    # convert it into string: YYYYMMDDHH
    first_tstep = (
        F"{first_tstep.year}{first_tstep.month:02d}{first_tstep.day:02d}"
        F"{first_tstep.hour:02d}"
    )
    last_tstep = (
        F"{last_tstep.year}{last_tstep.month:02d}{last_tstep.day:02d}"
        F"{last_tstep.hour:02d}"
    )

    return dfmet, first_tstep, last_tstep


def process_pcpn(dfmet, pcpn_col_name, func_psnow, **func_psnow_kwargs):
    '''
    Converts the precipitation values from mm/hour to mm/s and applies phase
    discrimination.
    This will create a new column for precipitation rate, and two columns
    for rainfall and snowfall.

    Parameters
    ----------
    dfmet : pandas Dataframe
        The dataframe that contains the meteo variables for SVS plus a column
        named `dtime_col_name` that is convertible to pd.Timestamp.

    pcpn_col_name : str
        The name of the precipitation column in `dfmet`.

    func_psnow : function
        The function that can calculate the probability of snowfall at each
        time step.

    **func_psnow_kwargs : dict
        Keyword argument to be passed to `func_psnow`.

    Returns
    -------
    dfmet : pandas Dataframe
    '''

    # convert the precipitation values from mm/hour to mm/second
    # (Total precipitation rate at the surface)
    dfmet["Precipitation rate (mm/s)"] = np.divide(dfmet[pcpn_col_name], 3600)
    pcpn_rate = dfmet.loc[:, "Precipitation rate (mm/s)"].to_numpy()

    # calc the probability of snowfall
    psnow = func_psnow(**func_psnow_kwargs)

    # apply phase discrimination
    # if psnow < 0.5, then we will have liquid precipitation
    dfmet["Rainfall rate (mm/s)"] = np.multiply(psnow < 0.5, pcpn_rate)
    dfmet["Snowfall rate (mm/s)"] = np.multiply(psnow >= 0.5, pcpn_rate)

    return dfmet


def calc_psnow(
    air_temp, rel_humidity, coeff_1=-10.04, coeff_2=1.41, coeff_3=0.09
):
    '''
    Calculate the probablity of snowfall using air temperature and relative
    humidity data.

    ref: https://www.nature.com/articles/s41467-018-03629-7
    The default coefficients are included in the supplementary file of the ref.

    Parameter
    ---------
    air_temp : ndarray
        The air temperature in deg Celcius.

    rel_humidity : ndarray
        The relative humidity in %.

    coeff_1, coeff_2, coeff_3 : float
        The fitting coefficients.

    Returns
    -------
    psnow : ndarray
        The probability of the precipitation phase being solid.
    '''

    # calculate psnow using the bivariate logistic equation
    psnow = np.exp(coeff_1 + (coeff_2 * air_temp) + (coeff_3 * rel_humidity))
    psnow = np.power(1 + psnow, -1)

    return psnow


def phase_disc(pcpn_rate, psnow):
    '''
    Apply phase discrimination on the precipitation rate data.

    Parameters
    ----------
    pcpn_rate : ndarray
        The precipitation rate data in mm/s.

    psnow : ndarray
        The probability of snowfall in each time step.

    Returns
    -------
    rainfall : ndarray
        The rainfall rate in mm/s.

    snowfall : ndarray
        The snowfall rate in mm/s.
    '''

    # if psnow < 0.5, then we will have liquid precipitation
    rainfall = np.multiply(psnow < 0.5, pcpn_rate)
    snowfall = np.multiply(psnow >= 0.5, pcpn_rate)

    return rainfall, snowfall


def get_first_last_timestep_from_met_file(met_file: str):
    """
    Reads the first line of a .met file and returns the first timestep.

    Parameters
    ----------
    met_file : str
        path to the .met file

    Returns
    -------
    first_timestep : str
        first timestep in the format YYYYMMDDHH

    last_timestep : str
        last timestep in the format YYYYMMDDHH
    """

    with open(met_file, "r", encoding="utf-8") as f:
        first_line = f.readline()
        year = int(first_line.split()[3])
        jday = int(first_line.split()[2])
        hour = int(first_line.split()[0])

        last_line = f.readlines()[-1]
        last_year = int(last_line.split()[3])
        last_jday = int(last_line.split()[2])
        last_hour = int(last_line.split()[0])

    # convert jday to month and day
    date = datetime.strptime(f"{year} {jday}", "%Y %j")
    month = date.month
    day = date.day

    last_date = datetime.strptime(f"{last_year} {last_jday}", "%Y %j")
    last_month = last_date.month
    last_day = last_date.day

    first_tstep = f"{year}{month:02}{day:02}{hour:02}"
    last_tstep = f"{last_year}{last_month:02}{last_day:02}{last_hour:02}"

    return first_tstep, last_tstep
# ________________________________________________________________ <<< main >>>
