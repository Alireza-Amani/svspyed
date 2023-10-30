# <<< doc >>> -----------------------------------------------------------------
'''
A class representing the MESH_parameters.txt file

Notes:
    - The code breaks if the user does not provide values for Wfc, since it is used
        in setting a default value for the state variables.

'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------
from collections import OrderedDict
from pathlib import Path
from copy import deepcopy
import os
from typing import Union

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------


class MESHParameters:
    '''
    MESH_parameters.txt file.

    Parameters
    ----------
    lyinfo : str, path object | DataFrame
        The absolute path to the .csv file that contains info on the parameters
        of the lysimeter of interest.

    param_names_cols : dict
        A dict having the name of columns corresponding to the expceted parameters
        which are to be read from `dy_lyinfo`.

    model_params : dict
        A dictionary that contains the values for some of the internal model
        parameters  that are read from `MESH_parameters.txt`.
        e.g. `KFICE`, `user_wfcdp`, ...

    host_dir_path : str
        The absolute path to the folder that contains the SVS input files and
        output.

    init_conds : str or dict, default="auto"
        If the model is going to go through a spin-up period, then the
        default values of `auto` should be the choise. This means that some
        reasonable values will be assigned to the state variables.
        Otherwise, provide a dict with values for any of the state variables
        you wish to set differently.

        Example:
        >>> init_conds = dict(wsoil=[0.15, 0.15, 0.15, 0.15])

        Bear in mind that in the example above, `len(init_conds["wsoil"])`
        must be equal to the number of soil layers.

    verbose : bool, default=False
        If True, will report back after saving the file.

    Attributes
    ----------
    parameters : dict
        A dict to store the values of the parameters that are read from the
        `df_lyinfo` dataframe.

    state_vars : dict
        A dict to hold the values for the state variables.

    df_lyinfo : Dataframe
        The dataframe read from `lyinfo_path`.

    all_lines : OrderedDict
        A dict to store the lines which compose the MESH_parameters.txt file.

    list_of_state_vars : list
        The list of the names of the model state variables initiated inside
        MESH_parameters.txt file.

    Methods
    -------
    read_csvfile()
        Read the csv file which contains info on the parameters and cache
        the values of the parameters into the `parameters` attribute.

    set_state_vars()
        Set the values for the state variables.

    create_file()
        Compose the MESH_parameters.txt file and save it inside the host folder.

    update_file()
            In the case where the state variables are changed, the file will
            be re-created and saved as a replacement for the previous one.

    sanity_check()
        Check whether the parameters/variables have reasonable values.

    '''

    def __init__(self, lyinfo: Union[Path, DataFrame] , param_names_cols: dict,
                 model_params: dict, host_dir_path: str,
                 init_conds: Union[str, dict] = "auto", verbose: bool = False):

        self.lyinfo = lyinfo
        self.param_names_cols = param_names_cols
        self.model_params = model_params
        self.init_conds = init_conds
        self.host_dir_path = host_dir_path
        self.verbose = verbose

        # list of the names of the state variables to be initiated
        self.list_of_state_vars = [
            "wsoil", "isoil", "tpsoil", "tvege", "tground", "tsnowveg", "tsnow",
            "snvdp", "snvden", "snval", "wsnv", "snodpl", "snoden", "snoal",
            "wsnow"
        ]

        # a dict to store the values of the parameters
        self.parameters = {}

        # a dict to store the lines which compose the file
        self.all_lines = OrderedDict()

        # a dict to store the values of the state variables
        self.state_vars = {}

        # read the dataframe containing the parameters info
        self.read_csvfile()

        # set the values for the state variables
        self.set_state_vars()

        # create and save the file
        self.create_file()
        if self.verbose:
            print(
                F"MESH_parameters.txt created in the host folder "
                F"at: `{self.host_dir_path}`\n"
            )

    def read_csvfile(self):
        '''
        Read the csv file which contains info on the parameters and cache
        the values of the parameters into the `parameters` attribute.
        '''

        # read the dataframe and store the parameter values in the dict
        if not isinstance(self.lyinfo, pd.DataFrame):
            self.df_lyinfo = pd.read_csv(self.lyinfo, index_col=False).dropna()
        else:
            self.df_lyinfo = self.lyinfo.copy()

        # read parameters from the file
        for key, value in self.param_names_cols.items():
            self.parameters[key] = self.df_lyinfo.loc[:, value].values

        self.parameters["nsoil_layers"] = len(self.df_lyinfo)

        # read the model internal parameter - (This is misplaced) -
        for key in self.model_params:
            self.parameters[key] = self.model_params[key]



    def set_state_vars(self):
        '''
        Set the values for the state variables.
        '''

        # a look-up dict to choose default values for the state variables
        # im using `wfc` values to set values for some of the state variables
        # in case the user did not define `wfc` parameter:
        
        # filling_var = self.parameters.get(
        #     "wfc",
        #     np.full(self.parameters["nsoil_layers"], 0.15)
        # )

        # use 0.5Wsat as default value for `wsoil`
        if self.parameters.get("wsoil", None) is None:
            filling_var = np.full(self.parameters["nsoil_layers"], 0.2)

        else:
            filling_var = self.parameters["wsoil"]


        default_vals = dict(
            wsoil=filling_var, isoil=filling_var * 0,
            tpsoil=filling_var + 293, tvege=[293, 293],
            tground=[293, 293], tsnowveg=[0., 0.], tsnow=[0., 0.],
            snvdp=0., snvden=0., snval=0., wsnv=0., snodpl=0., snoden=0.,
            snoal=0., wsnow=0.
        )
        # a check to make sure all the state vars are included in the
        # `default_vals` dict
        assert (set(default_vals.keys()) == set(self.list_of_state_vars)), (
            "Check the keys of the `default_vals` dict!"
        )

        # store a copy of `init_conds` attribute
        init_conds = deepcopy(self.init_conds)

        if (isinstance(init_conds, str) and init_conds == "auto"):
            # re-define it as a dict and assign "auto" to all of the state vars
            init_conds = {}
            for st_var in self.list_of_state_vars:
                init_conds[st_var] = "auto"
                self.state_vars[st_var] = default_vals[st_var]

        # if its a dict, assign "auto" to those variables that are not included
        # in the `self.init_conds` dict. If they are included, then get their
        # values and store them in `state_vars` attribute.
        elif isinstance(init_conds, dict):
            for st_var in self.list_of_state_vars:
                if st_var not in init_conds:
                    init_conds[st_var] = "auto"
                    self.state_vars[st_var] = default_vals[st_var]
                else:
                    self.state_vars[st_var] = init_conds[st_var]
                    # shoud we not call self.set state in update?

    def create_file(self):
        '''
        Compose the MESH_parameters.txt file and save it inside the host folder.
        '''

        # list of static param that do not change by the layer and has single value
        static_param = [
            "slop", "observed_forcing", "zusl", "ztsl", "deglat", "deglng",
            "draindens"
        ]

        self.all_lines["L1"] = "! site-specific information: --------------"
        for line_i, param_i in enumerate(self.param_names_cols.keys()):

            if param_i == "vf_type":
                self.parameters[param_i] = np.atleast_1d(
                    self.parameters[param_i]
                )
                vf_values = np.zeros(26)
                vf_values[int(self.parameters[param_i][0] - 1)] = 1.0
                vf_values = (4 * " ").join([str(j) for j in vf_values])
                self.all_lines[F"L{1 + line_i + 1}"] = (
                    F"vf{(30 - len('vf')) * ' '}{vf_values}"
                )

            elif param_i in ["z0v", "d50", "d95"]:
                self.parameters[param_i] = np.atleast_1d(
                    self.parameters[param_i]
                )
                p_values = np.zeros(26)
                p_values[int(self.parameters["vf_type"][0] - 1)] = self.parameters[param_i][0]
                p_values = (4 * " ").join([str(j) for j in p_values])
                self.all_lines[F"L{1 + line_i + 1}"] = (
                    F"{param_i}{(30 - len(param_i)) * ' '}{p_values}"
                )


            else:
                p1_values = np.atleast_1d(self.parameters[param_i])
                if param_i in static_param:
                    p1_values = np.atleast_1d(self.parameters[param_i][0])

                p1_values = (4 * ' ').join(
                    [(str(j) + (10 - len(str(j))) * ' ') for j in p1_values]
                )

                self.all_lines[F"L{1 + line_i + 1}"] = (
                    F"{param_i}{(30 - len(param_i)) * ' '}{p1_values}"
                )

        # determine the next line nr
        l_l = 3 + line_i
        self.all_lines[F"L{l_l}"] = "\n! model internal parameters: --------------"
        for line_i, param_i in enumerate(self.model_params.keys()):
            self.all_lines[F"L{l_l + line_i + 1}"] = (
                F"{param_i}{(30 - len(param_i)) * ' '}{self.parameters[param_i]}"
            )

        # set `KHYD`
        self.all_lines[F"L{l_l + line_i + 2}"] = (
            F"KHYD{(30 - len('KHYD')) * ' '}{self.parameters['nsoil_layers']}"
        )

        # determine the next line nr
        l_l = l_l + line_i + 4
        self.all_lines[F"L{l_l}"] = "\n! state variables: --------------"
        for line_i, param_i in enumerate(self.state_vars.keys()):
            p1_values = np.atleast_1d(self.state_vars[param_i])
            p1_values = (4 * ' ').join(
                [(str(j) + (10 - len(str(j))) * ' ') for j in p1_values]
            )
            self.all_lines[F"L{l_l + line_i + 1}"] = (
                F"{param_i}{(30 - len(param_i)) * ' '}{p1_values}"
            )

        # check and save the file
        self.sanity_check()
        save_path = os.path.join(
            self.host_dir_path, "MESH_parameters.txt"
        )
        with open(save_path, "wt", encoding="utf-8") as runfile:
            for _, value in self.all_lines.items():
                runfile.write(value + "\n")

    def update_file(self):
        '''
        In the case where the state variables are changed, the file will
        be re-created and saved as a replacement for the previous one.
        '''

        # create and save the file
        self.create_file()
        if self.verbose:
            print("MESH_parameters.txt updated.\n")

    def sanity_check(self):
        '''
        Check whether the parameters/variables have reasonable values.
        It will aslo check if a parameter like 'sand', for example, has as many
        values as the number of soil layers defined in the model.
        '''

        # check the length of the parameter-arrays for the following:
        check_len = [
            "sand", "clay", "wsat", "wfc", "wwilt", "ksat", "psisat", "rhosoil",
            "bcoef",
            # state variables
            "wsoil", "isoil", "tpsoil"
        ]

        # combine all the state vars and params into a single dict
        pv_dict = deepcopy(self.parameters)
        pv_dict.update(deepcopy(self.state_vars))

        for key, value in pv_dict.items():
            if key not in check_len:
                continue

            assert (len(value) == pv_dict["nsoil_layers"]), (
                F"The parameter `{key}` must have as many values as the number "
                F"of soil layers defined in the model, i.e. "
                F"{pv_dict['nsoil_layers']}\n"
                F"You have assigned {len(value)} values to `{key}`.\n"
                F"Current `{key}`: {value}\n"
            )

        # check values

        # parameters that must be between 0 and 1
        zero_one_p = ["wsat", "wfc", "wwilt", "wsoil", "isoil", "user_wfcdp"]
        for param in zero_one_p:
            if param not in pv_dict:
                continue

            assert (
                np.all(np.atleast_1d(pv_dict[param]) < 1) and
                np.all(np.atleast_1d(pv_dict[param]) >= 0)
            ), (F"Check the values for the `{param}` parameter/variable!\n"
                F"{param} values: {pv_dict[param]}"
                )

        # parameters between 0 and 100
        zero_cent_p = ["sand", "clay"]
        for param in zero_cent_p:
            assert (
                np.all(np.atleast_1d(pv_dict[param]) <= 100) and
                np.all(np.atleast_1d(pv_dict[param]) >= 0)
            ), (F"Check the values for the `{param}` parameter!\n"
                F"{param} values: {pv_dict[param]}"
                )

        # temperature parameters must be in Kelvin
        temp_pv = ["tpsoil", "tperm", "tvege", "tground", "tsnowveg", "tsnow"]
        for param in temp_pv:
            if np.all(np.atleast_1d(pv_dict[param]) == 0.0):
                continue

            assert (
                np.all(np.atleast_1d(pv_dict[param]) <= 340) and
                np.all(np.atleast_1d(pv_dict[param]) >= 200)
            ), (F"Check the values for the `{param}` parameter!\n"
                F"{param} values: {pv_dict[param]}"
                )

        # individual checks
        ksat_values = np.atleast_1d(pv_dict.get("ksat", 0.666))
        assert (
            np.all(ksat_values > 1e-9) and
            np.all(ksat_values < 1)
        ), (
            F"Check the values for the `ksat` parameter!\n"
            F"`ksat` values: {ksat_values}"
        )

        psisat_values = np.atleast_1d(pv_dict.get("psisat", 0.666))
        assert (
            np.all(psisat_values > 0) and
            np.all(psisat_values < 20)
        ), (
            F"Check the values for the `psisat` parameter!\n"
            F"`psisat` values: {psisat_values}"
        )

        rhosoil_values = np.atleast_1d(pv_dict.get("rhosoil", 666))
        assert (
            np.all(rhosoil_values > 100) and
            np.all(rhosoil_values < 4000)
        ), (
            F"Check the values for the `rhosoil` parameter!\n"
            F"`rhosoil` values: {rhosoil_values}"
        )

        if np.alltrue([key_i in pv_dict for key_i in ["wsat", "wfc", "wwilt"]]):
            assert (
                np.all(np.atleast_1d(pv_dict['wsat'])
                       > np.atleast_1d(pv_dict['wfc']))
                and
                np.all(np.atleast_1d(pv_dict['wsat'])
                       > np.atleast_1d(pv_dict['wwilt']))
                and
                np.all(np.atleast_1d(pv_dict['wfc'])
                       > np.atleast_1d(pv_dict['wwilt']))
            ), (
                "Please check the `wsat`, `wfc`, `wwilt` values relative "
                "to each other!"
            )

    def __repr__(self) -> str:
        for _, value in self.all_lines.items():
            print(value + "\n")

        return " "
# ________________________________________________________________ <<< main >>>
