# <<< doc >>> -----------------------------------------------------------------
'''
A class to run the SVS model for multiple parameteric scenarios.
    * useful for sensitivity analysis
    * useful for enesmble runs

Notes:
    - Possibly there are faster alternative strategies to hold the output data,
        instead of appending them to a dataframe, as dataframes.
        How about save them as text and later process them at once?
        'at once': every loop of Process.join().

    - Make the SVSModel instantiation parallel. Would save time for SA, e.g.
        * although, the process is fast enough if met files are provided.

    - make the read_output parallel too.
'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------

from copy import deepcopy
from pathlib import Path
from concurrent.futures import as_completed, ProcessPoolExecutor
from multiprocessing import Manager
import re

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from pyarrow.feather import write_feather
import dask
import dask.dataframe as dd

from ..model.svs_model import SVSModel
from ..input_preparation.prep_svs import ModelInputData
from ..utils.helper_functions import assert_dir_exist
# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------


class PerturbAndRun:
    '''
    Preturb one or more of the SVS parameters and run the instances in parallel.

    Parameters
    ----------
    svs_default_input : ModelInputData
        An instance of ModelInputData with default values for the parameters.

    parameter_scenarios : dict
        A dictionary that contains the parameter scenarios each of which are
        dictionaries on their own that will be used to perturb an already
        created SVS instance and create a new and modified one.
        The 'already created SVS instance' is instantiated using the default
        parameters of the case study. Each of the model parameters present in
        the scenarios, will be changed.
        An example of such dict can be:
        >>> p_scn_dict = dict(
        ...     scenario_1 = {'sand': [10, 10, 10], 'clay': [4, 4, 4, 4]}
                scenario_2 = {'sand': [14, 14, 14], 'clay': [6, 6, 6, 6]}
        ... )

    different_met : Path
        Path to the dir containing set of different met files to be used for
        different parameter scenarios. Each of the met files must be named as
        `basin_forcing_{scn_nr}.met`, where `scn_nr` is the scenario number
        starting from 0.

        If `different_met` is None, the default met file will be used for all
        the scenarios.

    njobs : int, default=2
        The max. number of CPU cores to use.

    Attributes
    ----------
    svs_instances : dict
        A dict to store the newly created SVS instances.

    dfscenarios : DataFrame
        A dataframe to hold the parameter scenarios.

    dfoutput : DataFrame
        A dataframe to hold the output of the SVS instances.

    Methods
    -------
    create_instances()
        Creates an SVS instace based on each of the parameter scenarios.

    run_all_parallel()
        Run several SVS instances in parallel.

    Raises
    ------
    KeyError
        If the key of the parameter scenarios is not among the SVS parameters

    '''

    def __init__(
        self, svs_default_input: ModelInputData, parameter_scenarios: dict,
        different_met: Path = None, njobs: int = 2,
        starting_scenario: int = 1, select_scenarios: list = None,
    ):
        # assert that elements stored in `parameter_scenarios` are dict
        # check the first element
        assert (
            isinstance(
                parameter_scenarios[next(iter(parameter_scenarios))], dict
            )
        ), ("Elements of `parameter_scenarios` attribute must be of type dict!")

        self.svs_default_input = deepcopy(svs_default_input)
        self.parameter_scenarios = parameter_scenarios
        self.different_met = different_met
        self.njobs = njobs

        # create and store SVS instances
        self.svs_instances = dict()
        # self.create_instances()

        # dataframe to hold the parameter scenarios
        self.dfscenarios = pd.DataFrame()

        # dataframe to hold the output of the SVS instances
        self.dfoutput = pd.DataFrame()

        if self.different_met:
            assert_dir_exist(self.different_met)
            self.different_met = Path(self.different_met)

        # create a checkpoint folder in working dir
        self.checkpoint_folder = self.svs_default_input.work_dir_path / "ens_checkpoint"
        self.checkpoint_folder.mkdir(exist_ok=True)

        self.scenario_names = sort_strings_by_number(list(self.parameter_scenarios))
        self.processed_instances = 0 + (starting_scenario - 1)

        if select_scenarios:
            self.scenario_names = select_scenarios
            self.processed_instances = 0

    def create_single_instance(self, scn_nr, scenario, svs_instances_proxy):
        '''
        Create a single SVS instance based on a parameter scenario.

        Parameters
        ----------
        scn_nr : int
            The scenario number.

        scenario : dict
            A dictionary that contains the parameter scenario.

        Returns
        -------
        new_input.host_dir_name : str
            The name of the host folder.

        new_svs : SVSModel
            The newly created SVS instance.
        '''

        new_input = deepcopy(self.svs_default_input)

        # change the host folder name
        new_input.host_dir_name = str(scenario)

        # change the name of the SVS exec file
        new_input.exec_file_name = F"{scn_nr}_{new_input.exec_file_name}"

        # change the path to .met file (if needed)
        if self.different_met:
            new_path_met = Path(
                self.different_met, F"basin_forcing_{scn_nr}.met"
            )
            new_input.copy_metfile = new_path_met

        # create an SVS instance
        new_svs = SVSModel(new_input, True, False)

        # change the values of the SVS parameters
        for key, value in self.parameter_scenarios[scenario].items():
            if key in new_svs.mesh_param_file.parameters:
                new_svs.mesh_param_file.parameters[key] = value
            elif key in new_svs.mesh_param_file.state_vars:
                new_svs.mesh_param_file.state_vars[key] = value
            else:
                raise KeyError(
                    F"`{key}` is not among SVS parameters or state variables"
                    F".\n"
                )

        # update the parameter file
        new_svs.mesh_param_file.update_file()

        # Instead of updating self.svs_instances directly, you'll update the proxy
        svs_instances_proxy[scenario] = new_svs

        return scenario

    def create_instances(self, scenario_chunks: list = None):
        '''
        Creates an SVS instace based on each of the parameter scenarios.
        '''

        # Initialize a Manager and a proxy dictionary
        manager = Manager()
        svs_instances_proxy = manager.dict()

        # Prepare the list of scenarios to avoid modifying the dictionary during iteration
        # scenarios_list = list(self.parameter_scenarios.items())
        scenarios_list = deepcopy(scenario_chunks)

        # using these keys, get the items from the dict
        scenarios_list = {
            key: self.parameter_scenarios[key] for key in scenarios_list}
        scenarios_list = list(scenarios_list.items())

        with ProcessPoolExecutor(max_workers=self.njobs) as executor:
            # Schedule the execution of each instance creation
            futures = [
                executor.submit(self.create_single_instance,
                                scn_nr, scenario, svs_instances_proxy)
                for scn_nr, (scenario, _) in enumerate(scenarios_list)
            ]

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    scenario = future.result()
                    print(
                        f"The SVS instance modified based on scenario {scenario}.\n"

                    )
                except Exception as exc:
                    print(f"Scenario generated an exception: {exc}")

        # After all processes are done, convert the proxy dictionary back to a regular dictionary
        self.svs_instances = dict(svs_instances_proxy)

    def run_all_parallel(
        self, output_time_scale: str = "daily", keepcols=None,
        effort_id: str = "",
    ):
        '''
        Run several SVS instances in parallel.

        Parameters
        ----------
        output_time_scale : str, default="daily"
            Either "daily" or "hourly".
            If "daily", it will collect the `dfdaily_out` attributes of the SVS
            instances, otherwise `dfhourly_out`.
            These are daily and hourly output dataframes of the model.

        keepcols : list, default=None
            A list of columns to keep from the output dataframe.

        effort_id : str, default=""
            A string to be added to the name of the output folder.


        Returns
        -------
        dfall_outputs : pandas Dataframe
            All of the `df{output_time_scale}_out` attributes into a single dataframe.
        '''

        # assertion
        assert (output_time_scale in ["daily", "hourly"]), (
            "`output_time_scale` must be either 'daily' or 'hourly'"
        )

        if not effort_id:
            # if no effort_id is provided, use current time as the id
            effort_id = "effort_"
            effort_id += pd.Timestamp.now().strftime("%Y%m%d_%H")

        # collect the child processes in a list
        children = []

        # collect the hourly output dataframes
        dfall_outputs = pd.DataFrame()

        # collect all the keys of `svs_instances`; will be used for del obj
        # all_svs_keys = deepcopy(list(self.svs_instances))
        # remaining_keys = deepcopy(all_svs_keys)

        while len(self.scenario_names) > self.processed_instances:

            # counter variable
            scenario_chunks = self.scenario_names[
                self.processed_instances:self.processed_instances + self.njobs
            ]

            # create the instances
            self.create_instances(scenario_chunks)

            for j, svs in enumerate(scenario_chunks):

                # create a child processes: i.e. initiate an SVS simulation run
                children.append(self.svs_instances[svs].run_svs_parallel())
                print(F"Running {svs} SVS instance ...\n")


            if len(children) == self.njobs:
                print(F"Waiting for the {self.njobs} processes to finish ...")
            else:
                print(
                    F"Waiting for the last {len(children)} process(es) to finish ..."
                )

            self.processed_instances += self.njobs

            # wait for the processes to finish
            for child in children:
                child.join()

            print("Finished!\n")
            children = []  # empty the child collector

            # process the output now and del the svs instance
            for svs_key in scenario_chunks:
                model = self.svs_instances[svs_key]

                # get the output dataframe
                model.read_output()
                dfout = getattr(model, F"df{output_time_scale}_out").copy()
                dfout["member"] = str(svs_key)
                if keepcols:
                    dfout = dfout.loc[:, keepcols]
                    # print(F"Keeping only {keepcols} columns of the output.")


                # save the dataframe using feather in the checkpoint folder
                save_name = F"{effort_id}{svs_key}_{output_time_scale}_out.feather"
                save_path = self.checkpoint_folder / save_name

                # remove duplicate columns
                dfout = dfout.loc[:, ~dfout.columns.duplicated()]
                write_feather(dfout, save_path, compression="zstd")

                # remove the host folder
                model.remove_host_folder_after_run()
                self.svs_instances[svs_key] = "Done_folder_deleted"


        # create a dataframe based on the parameter scenarios
        self.create_param_scen_df()

        # read all the feather files in the checkpoint folder
        # IMPORTANT: only files starting with `effort_id` will be read
        file_pattern = F"{effort_id}*{output_time_scale}_out.feather"
        feather_files = list(self.checkpoint_folder.glob(file_pattern))
        delayed_reads = [read_feather_file(file) for file in feather_files]

        ddf = dd.from_delayed(delayed_reads)
        dfall_outputs = ddf.compute()

        self.dfoutput = dfall_outputs.copy()

    def create_param_scen_df(self):
        '''
        Create a dataframe based on the parameter scenarios.
        '''

        parameter_scenarios = deepcopy(self.parameter_scenarios)

        # I need to determine and define name of the cols first
        # I need only one of the scenarios
        scn_0 = parameter_scenarios[list(parameter_scenarios)[0]]

        # name and number of the columns
        names_list = []
        values_list = []
        for key, value in scn_0.items():
            value = np.atleast_1d(value)

            if len(value) == 1:
                values_list += list(value)
                names_list += [key]

            elif len(value) > 1:
                values_list += list(value)
                names_list += [F"{key}_{i+1}" for i in range(len(value))]

        # create a dataframe as tall as the number of scenarios
        dfscenarios = pd.DataFrame(
            index=parameter_scenarios, columns=names_list
        )

        # fill in the dataframe
        for scn in parameter_scenarios:
            for key, value in parameter_scenarios[scn].items():
                value = np.atleast_1d(value)
                value_len = len(value)

                if value_len == 1:
                    dfscenarios.loc[scn, key] = value[0]

                elif value_len > 1:
                    col_names = [F"{key}_{i+1}" for i in range(value_len)]
                    dfscenarios.loc[scn, col_names] = value

        self.dfscenarios = deepcopy(dfscenarios)


@dask.delayed
def read_feather_file(file):
    '''
    Read a feather file and return a dataframe.

    Parameters
    ----------
    file : Path
        Path to the feather file.
    '''

    return pd.read_feather(file)

def extract_number(s):
    """Extracts the first number found in a string."""
    match = re.search(r'\d+', s)
    return int(match.group()) if match else 0

def sort_strings_by_number(strings):
    """Sorts a list of strings based on the first number found in each string."""
    return sorted(strings, key=extract_number)

# # Example usage
# strings = ["item3", "item12", "item1", "item20", "item2"]
# sorted_strings = sort_strings_by_number(strings)
# print(sorted_strings)

# ________________________________________________________________ <<< main >>>
