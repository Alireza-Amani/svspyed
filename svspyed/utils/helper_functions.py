# <<< doc >>> -----------------------------------------------------------------
'''
This is the module in which I keep the definition of the helper functions that
are used in other modules.

'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------
import os
from pathlib import Path
import re
from typing import Union

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------


def assert_file_exist(the_path: str) -> None:
    '''
    A function that performs an assertion to check whether the provided
    path points to an existing file.

    Parameters
    ----------
    the_path : str, path object
        the path to the file.
    '''

    assert_message = F"`{the_path}` does not point to an existing file!"
    assert os.path.isfile(the_path), assert_message


def assert_dir_exist(abs_path: str) -> None:
    '''
    A function that performs an assertion to check whether the provided
    absolute path points to an existing directory.

    Parameters
    ----------
    abs_path : str
        the absolute path to the directory.
    '''

    assert_message = F"`{abs_path}` does not point to an existing directory!"
    assert os.path.isdir(abs_path), assert_message


def print_dict_nice(
    the_dict: dict, report_header: str = "", kv_gap: int = 20
) -> None:
    '''
    Prints the key, value pair of a dict in a neat way.

    Parameters
    ----------
    the_dict : dict
        The dictionary that we want to print its content.

    report_header : str
        The header that needs to printed before `the_dict` content.

    kv_gap : int
        The gap between the key-value pairs printed out.

    '''

    if report_header:
        print("\n" + report_header + "\n")

    for key, value in the_dict.items():
        message = F"\t{key}: "
        message += (kv_gap - len(key)) * "-" + "> " + F"{value}\n"
        print(message)


def get_optimal_set(gsearch_results: DataFrame, print_set: bool = True) -> dict:
    '''
    Returns the optimal values for the tuning hyperparameters.

    Parameters
    ----------
    gsearch_results : DataFrame
        The sorted dataframe that contains the cv_results_ of a GridSearchCV.

    print_set : bool
        If True, it will print the hyperparameters and their optimal values.
    '''

    # a dict to hold the name and optimal value for the h parameters
    dict_best_params = dict()

    # list of the column-labels corresponding to the tuning hparameters
    list_param_cols = list(
        filter(re.compile("^param_(.*$)").findall, gsearch_results.columns)
    )

    for ith_param in list_param_cols:
        dict_best_params[ith_param.removeprefix(
            "param_")] = gsearch_results.loc[:, ith_param].iloc[0]

    if print_set:
        hdr_message = "The optimal values for the tuning hyperparameters"
        print_dict_nice(dict_best_params, hdr_message)

    return dict_best_params


def remove_file_dir(path: Union[str, Path]) -> None:
    '''
    Removes a file, or a directory and its content.

    Parameters
    ----------
    path : Union[str, Path]
        The absolute path to the directory that should be removed.

    Returns
    -------
    out : None
    '''

    # change it from str to Path if necessary
    if isinstance(path, str):
        path = Path(path)

    # if `path` points to a file, then remove the file.
    if path.is_file():
        try:
            path.unlink()
        except FileNotFoundError:
            pass

        return

    # if `path` points to an existing dir
    if os.path.isdir(path):
        # next line is necessary for Windows users
        os.chmod(path, 0o777)

        # remove the contents (files) of this dir by recursion
        for file_i in path.iterdir():
            remove_file_dir(file_i)

        # now remove the empty dir
        try:
            path.rmdir()
        except FileNotFoundError:
            pass


def check_start_end_date_format(date: str) -> None:
    '''
    Check if the date string is in the desired format.
    The desired format is suitable for `MESH_input_run_options`.
    It has four digits for the year, up to three digits for the day of the year,
    two digits for the hour and miniute of the time step; all separated by '-'.
    Example:
    >>> start_date = "2018-183-04-00"

    Parameters
    ----------
    date : str
        The date value as a string.
    '''

    try:
        pd.to_datetime(date, format="%Y-%j-%H-%M", utc=True)
    except Exception as exc:
        raise ValueError(
            F"Make sure your date (i.e. `{date}`) is a string with the following format: "
            F"%Y-%m-%d %H-%M-%S\n"
        ) from exc


def check_spinup_end_date_format(date: str) -> None:
    '''
    Check if the simulation date string is in the desired format.
    This is the date that marks the beggining of the simulation and
    end of the spin-up period.

    Example:
    >>> sim_date = "2018-07-01 13:00:00"

    Parameters
    ----------
    date : str
        The date value as a string.
    '''

    try:
        pd.to_datetime(date, format="%Y-%m-%d %H:%M:%S", utc=True)

    except Exception as exc:
        raise ValueError(
            F"Make sure your date (i.e. `{date}`) is a string with the following format: "
            F"%Y-%m-%d %H-%M-%S\n"
        ) from exc

def round_params(p_dict):
    '''
    Round the values of the parameters in the `p_dict`.

    Parameters
    ----------
    p_dict : dict
        A dictionary with the parameters.

    Returns
    -------
    p_dict : dict
        A dictionary with the rounded parameters.
    '''

    for key, val in p_dict.items():
        match key:
            case "wsat" | "wfc" | "wwilt" | "wunfrz" | "user_wfcdp" | "psisat":
                p_dict[key] = np.round(val, 4)

            case "tperm" | "z0v" | "d95" | "d50" | "conddry" | "condsld" | "bcoef":
                p_dict[key] = np.round(val, 2)

            case "ksat":
                p_dict[key] = [float(F"{kval:.2e}") for kval in val]

    return p_dict

def get_layering_df(enclosure):
    """
    Returns a layering dataframe based on the provided Enclosure object.

    Parameters:
        enclosure (Enclosure): The Enclosure object containing layering information.

    Returns:
        pd.DataFrame: Layering dataframe with columns 'depth', 'thickness', and 'soil_type'.
    """
    dfe = pd.DataFrame(columns=['depth', 'thickness', 'soil_type'])

    l1 = enclosure.layering.replace(" ", "").split("+")

    depth = 0.0 # cm
    for i, l in enumerate(l1):
        ith_block_thickness, ith_block_step, ith_block_soil = l.split(":")
        ith_block_thickness = float(ith_block_thickness)
        ith_block_step = float(ith_block_step)

        ith_block_depths = np.arange(
            depth + ith_block_step,
            depth + ith_block_thickness + ith_block_step,
            ith_block_step
        )
        depth += ith_block_thickness

        if i == 0:
            dfe = pd.DataFrame({
                'depth': ith_block_depths,
                'thickness': ith_block_step,
                'soil_type': ith_block_soil
            })

        else:
            dfe = pd.concat([
                dfe,
                pd.DataFrame({
                    'depth': ith_block_depths,
                    'thickness': ith_block_step,
                    'soil_type': ith_block_soil
                })
            ])

    dfe.reset_index(drop=True, inplace=True)
    return dfe


def populate_layering_df(dfe, enclosure, site_params):
    """
    Populates the layering dataframe with soil and site parameters.

    Parameters:
        dfe (pd.DataFrame): The layering dataframe.
        enclosure (Enclosure): The Enclosure object containing soil layer information.
        site_params (SiteParameters): The SiteParameters object containing site parameters.

    Returns:
        pd.DataFrame: Populated layering dataframe.
    """
    for st in dfe.soil_type.unique():
        soil_layer = [sl for sl in enclosure.soil_layers if sl.code == st][0]

        for lp in soil_layer._fields:
            if lp == "code":
                continue
            dfe.loc[dfe.soil_type == st, lp] = getattr(soil_layer, lp)

    for sp in site_params._fields:
        dfe[sp] = getattr(site_params, sp)

    return dfe

def check_csv_columns(csv_path, expected_cols):
    """
    Check if the expected columns are present in the given CSV file.

    Parameters:
    - csv_path (str): The path to the CSV file.
    - expected_cols (dict): Dictionary of expected columns and their corresponding names in CSV.

    Returns:
    - list: A list of missing columns, or an empty list if all columns are found.

    Usage Example:
    ```python
    csv_path = "path/to/csv.csv"
    expected_cols = {
        "utc_dtime": "datetime_utc",
        "air_temperature": "Air temperature (degC)"
        # ... other columns
    }
    missing_cols = check_csv_columns_single(csv_path, expected_cols)
    ```
    """
    try:
        df = pd.read_csv(csv_path, nrows=1)  # Read only the first row to get column names
    except FileNotFoundError:
        return ["File not found"]

    csv_cols = set(df.columns)
    expected_csv_cols = set(expected_cols.values())

    missing = expected_csv_cols - csv_cols
    if missing:
        print(F"Missing columns in {csv_path}: {missing}")
    else:
        print("All columns found!")
# ________________________________________________________________ <<< main >>>
