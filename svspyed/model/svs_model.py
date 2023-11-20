# <<< doc >>> -----------------------------------------------------------------
'''
The Class representing the SVS land-surface model.
'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------
from multiprocessing import Process
import os
from subprocess import Popen, run
from pathlib import Path
from copy import deepcopy
from shutil import copytree

import pandas as pd

# import the base class that takes care of the file preperation for SVSModel class.
# they are in the same folder, part of a package.
from ..input_preparation.prep_svs import PrepSVS
# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------

# SVS main class
class SVSModel(PrepSVS):
    '''
    The SVS land surface model.

    An instance of this class can take care of the following tasks:
        - creates the input files required for running SVS
        - runs the SVS model
        - processes the output files after the run is finished
        -

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

    dfhourly_out : Dataframe
        A dataframe that holds the hourly output variables.

    dfdaily_out : Dataframe
        A dataframe that holds the daily output variables.
        Note that the daily aggregation is done based on the local time-zone.

    run_process : CompletedProcess
            Represents a process that has finished running SVS.
            It will be created only when `run_SVS` method is called.

    child_process   : multiprocessing.Process
            The Popen instance tasked with running an SVS run.
            It will be created only when `run_SVS_parallel` method is called.

    Methods
    -------
    run_svs()
        Run the SVS executable inside the `host_dir_path`.

    run_svs_parallel()
        This method creates a child process for running SVS.

    read_output()
        Import the hourly output variables into a dataframe

    out_to_daily(
        spinup_end_date="2018-07-01 00:00:00", time_zone="America/Montreal"
    )
        Convert the hourly output to daily
    '''

    def __init__(
        self, required_data, remove_old_host_folder: bool = False,
        verbose: bool = False
        ):

        # initialize the base class
        super().__init__(
           required_data=required_data,
            remove_old_host_folder=remove_old_host_folder,
            verbose=verbose
        )

        # the dataframes that will hold the hourly/daily outputs
        self.dfhourly_out = pd.DataFrame()
        self.dfdaily_out = pd.DataFrame()

        # the separate process to run the model in parallel
        self.child_process = None # will be multiprocessing.Process(...)

        # the subporcess.run object
        self.run_process = None

        if self.verbose:
            print("SVSModel instance created.\n")


    def run_svs(self) -> None:
        '''
        Run the SVS executable file inside the host folder.
        This is the appropriate method for running a single SVS instance. Since,
        it will wait for the process to finish. Look at `run_SVS_parallel` for
        when you plan to run multiple instances of SVS in parallel.

        Returns
        -------
        run_process : CompletedProcess
            Represents a process that has finished running SVS.
        '''

        # save the current working dir before changing it in the next part
        org_wdir = os.getcwd()

        # change the working dir to the host dir
        os.chdir(self.host_dir_path)

        # run the SVS model
        if self.verbose:
            print("Running SVS ...\n")

        self.run_process = run(
            F"./{self.svs_exe_name}", capture_output=True, check=True, text=True
        )

        # go back to the original working dir
        os.chdir(org_wdir)

        # read the output vars
        self.read_output()

        if self.verbose:
            print("SVS run is finished and output variables are cached.\n")


    def create_svs_run(self):
        '''
        A function to use for SVS parallel runs.
        '''

        self.run_process = run(
            F"./{self.svs_exe_name}", capture_output=True, check=True, text=True
        )


    def run_svs_parallel(self) -> Popen:
        '''
        This method creates a child process for running SVS.
        It will not wait for the process to finish and will return the child
        process.

        Returns
        -------
        child_process   : Popen
            The Popen instance tasked with running an SVS run.
        '''

        # save the current working dir before changing it in the next part
        org_wdir = os.getcwd()

        # change the working dir to the host dir
        os.chdir(self.host_dir_path)


        self.child_process = Process(target=self.create_svs_run)
        self.child_process.start()

        # go back to the original working dir
        os.chdir(org_wdir)

        # run the SVS model
        if self.verbose:
            print("Created a child process to run SVS with.\n")

        return self.child_process

    def read_output(self):
        '''
        import the hourly output variables into a dataframe.
        '''

        file_path = os.path.join(
            self.host_dir_path, "output", self.required_data.output_csvfile_name
        )
        dfhourly_out = pd.read_csv(
            file_path, index_col=False, skipinitialspace=True
        )

        # make a date-time column
        dfhourly_out["date_utc"] = (dfhourly_out["YEAR"] * 1000) + dfhourly_out["JDAY"]
        dfhourly_out["date_utc"] *= 100
        dfhourly_out["date_utc"] += dfhourly_out["HOUR"]
        dfhourly_out["date_utc"] = pd.to_datetime(
            dfhourly_out["date_utc"], format="%Y%j%H", utc=True
        )

        # if there is a column that starts with `Unnamed`, then remove it
        for col in dfhourly_out.columns:
            if col.startswith("Unnamed"):
                dfhourly_out = dfhourly_out.drop(columns=col)

        # if there is a column with all NA values, then remove it
        for col in dfhourly_out.columns:
            if dfhourly_out[col].isna().all():
                print(
                    F"Removing {col} from the output dataframe. "
                    "It has all NA values."
                )
                dfhourly_out = dfhourly_out.drop(columns=col)

        self.dfhourly_out = dfhourly_out

        # create a daily dataframe based on the hourly outputs
        self.out_to_daily(self.mesh_param_file.parameters["nsoil_layers"])

    def out_to_daily(self, nsoil_layer: int):
        '''
        Convert the hourly output to daily.
        This function must be modified, should the names of the columns inside
        the SVS output csv file change.

        Parameter
        ---------
        nsoil_layer : int
            The number of soil layer defiend in the model.
            This for example helps to select the `WSOIL_#` columns.
        '''

        dfhourly = self.dfhourly_out.copy()

        # create a column holding datetime of the local time zone
        dfhourly["dtime_local"] = (
            dfhourly["date_utc"].dt.tz_convert(self.required_data.time_zone)
        )

        # accumulated flux variables (column names)
        acc_columns = ["ACC_DRAI", "ACC_ET", "ACC_OVFLW", "ACC_PCP"]

        # create de-cumulated columns for each
        for acc_col in acc_columns:
            dfhourly[acc_col[4:]] = dfhourly[acc_col].diff()
            dfhourly.loc[0, acc_col[4:]] = dfhourly.loc[0, acc_col]

        # drop the accumulated flux columns
        dfhourly = dfhourly.drop(columns=acc_columns)

        if self.required_data.spinup_end_date:
            # remove the spin-up period data
            time_cond = (
                dfhourly["dtime_local"]
                >=
                pd.Timestamp(
                    self.required_data.spinup_end_date,
                    tz=self.required_data.time_zone
                )
            )

            dfhourly = dfhourly.loc[time_cond, :].reset_index(drop=True)


        # set index to the local time
        dfhourly.set_index("dtime_local", inplace=True)

        # these columns will be summed to daily values
        to_be_sum = ["DRAI", "ET", "PCP", "OVFLW"]

        dfsumvar = dfhourly.loc[:, to_be_sum]
        dfsumvar = dfsumvar.resample("1D").sum()
        dfsumvar["date"] = dfsumvar.index.date
        dfsumvar = dfsumvar.reset_index(drop=True)

        # these cols will be averaged to daily values
        to_be_ave = [F"WSOIL_{i1}" for i1 in range(1, nsoil_layer+1)]
        to_be_ave += [F"TPSOIL_{i1}" for i1 in range(1, nsoil_layer+1)]
        to_be_ave += [F"ISOIL_{i1}" for i1 in range(1, nsoil_layer+1)]
        to_be_ave += [
            'TGROUND_1', 'TGROUND_2', 'TVEG_1', 'TVEG_2', 'WVEG', 'SNOMA', 'SNODP',
            'SNODEN', 'SNOALB', 'WSNO', 'TSNO_1', 'TSNO_2', 'SNVMA', 'SNVDP',
            'SNVDEN', 'SNVALB', 'WSNV', 'TSNV_1', 'TSNV_2',
        ]

        dfavevar = dfhourly.loc[:, to_be_ave]
        dfavevar = dfavevar.resample("1D").mean()
        dfavevar["date"] = dfavevar.index.date
        dfavevar = dfavevar.reset_index(drop=True)

        # merge into one
        dfdaily = pd.merge(dfsumvar, dfavevar, on="date")

        self.dfdaily_out = dfdaily.copy()
        self.dfdaily_out.set_index("date", inplace=True)



def copy_svs_model(svs_model: SVSModel, new_host_name: str) -> SVSModel:
    '''
    Copy the SVS model to a new host directory and creates the required files
    for running the model.

    Parameters
    ----------
    svs_model: SVSModel
        The SVSModel instance to be copied.

    new_host_name: str
        The name of the new host directory.

    Returns
    -------
    svs_model   : SVSModel
        The SVSModel instance of the copied model.
    '''

    # make a copy of the SVSModel instance
    new_svs_model = deepcopy(svs_model)
    new_svs_model.required_data.host_dir_name = new_host_name
    new_svs_model = new_svs_model.__class__(new_svs_model.required_data, True)


    return new_svs_model
# ________________________________________________________________ <<< main >>>
