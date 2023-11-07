# <<< doc >>> -----------------------------------------------------------------
'''
A class to represent the MESH_input_run_options.ini file.
'''
# _________________________________________________________________ <<< doc >>>

# <<< imports >>> -------------------------------------------------------------

from collections import OrderedDict
import os

import pandas as pd

from ..utils.helper_functions import check_start_end_date_format
# _____________________________________________________________ <<< imports >>>

# <<< main >>> ----------------------------------------------------------------


class InputRunFile:
    '''
    MESH_input_run_options.ini file for SVS.

    Parameters
    ----------
    host_dir_path : str, path object
        The absolute path to the folder that contains the SVS input files and
        output.

    first_tstep : str
        The first time-step in `dfmet` with the format of YYYYMMDDHH.
        This will be used in the creation of `MESH_input_run_options.ini`.

    last_tstep : str
        The last time-step in `dfmet` with the format of YYYYMMDDHH.
        Used for the sake of reporting.

    start_date : str, default=""
        The simulation start time step; provide it as: `YYYY-JDAY-HH-MM`.
        Example:
        >>> start_date = "2017-184-05-00"
        `start_date` must occur on or after `first_tstep`.

    end_date : str, default=""
        The simulation end time step; provide it as: `YYYY-JDAY-HH-MM`.
        Example:
        >>> start_date = "2017-184-05-00"

    verbose : bool, default=False
        If True, will report back after saving the file.

    Attributes
    ----------
    all_lines : OrderedDict
        A dict to contain all the lines that will be written into the file.

    cntrl_flags : dict
        A dict to hold the control flags and their values.

    Methods
    -------
    add_flags()
        Add the necessary and intended control flags.

    write_lines()
        Insert the lines into the `all_lines` dict.

    save_file()
        Save the file in the host folder.

    update_file()
        Recreate the file with the updated info and replace the previous one.
    '''

    def __init__(self, host_dir_path: str, first_tstep: str, last_tstep: str,
                 start_date: str = "", end_date: str = "", model_tsetp: int = 5,
                 verbose: bool = False):

        self.host_dir_path = host_dir_path
        self.first_tstep = first_tstep
        self.last_tstep = last_tstep
        self.start_date = start_date
        self.end_date = end_date
        self.model_tsetp = model_tsetp
        self.verbose = verbose

        # create an empty dict to contain all the lines that will be written
        # into a file
        self.all_lines = OrderedDict()

        # a dict to hold the control flags
        self.cntrl_flags = dict()

        # fill in the dict with the relevant info, and save it
        self.add_flags()
        self.write_lines()
        self.save_file()

        # report back
        if self.verbose:
            message = (
                F"\nThe MESH_input_run_options.ini is created inside the host "
                F"folder\nat: `{self.host_dir_path}`\n"
            )
            message += self.report_simulation_time()

            print(message)

    def add_flags(self):
        '''
        Add the necessary and intended control flags.
        '''

        self.cntrl_flags["BASINFORCINGFLAG"] = (
            F"met rr_sr start_date={self.first_tstep} hf=60"
        )

        self.cntrl_flags["SHDFILEFLAG"] = "2"
        self.cntrl_flags["INPUTPARAMSFORMFLAG"] = "only txt"
        self.cntrl_flags["SOILINIFLAG"] = "0"
        self.cntrl_flags["NRSOILAYEREADFLAG"] = "1"
        self.cntrl_flags["RUNMODE"] = "runsvs noroute"
        self.cntrl_flags["DIAGNOSEMODE"] = "off"
        self.cntrl_flags["TIMESTEPFLAG"] = F"{self.model_tsetp}"

    def write_lines(self):
        '''
        Insert the lines into the `all_lines` dict.
        '''

        # It is necessary to fill something in the first 3 lines
        self.all_lines["L1"] = "MESH input file created! Two points: "
        self.all_lines["L2"] = (
            "\t-The number of control flags integer must be at line 4!"
        )
        self.all_lines["L3"] = (
            "\t-There must be two lines gap between the last control flag and "
            "the number of output points integer!"
        )

        # add number of the control flags at line 4, and write the flags at
        # the ensuing lines
        cflags = self.cntrl_flags.keys()
        self.all_lines["L4"] = (
            4 * " " + str(len(cflags)) + " # number of control flags"
            # the leading spaces are needed. min. 4
        )

        for line_i, k in enumerate(cflags):
            self.all_lines[F"L{4 + line_i + 1}"] = (
                F"{k}" +
                (30 - len(k)) * " " +
                F"{self.cntrl_flags[k]}"
            )

        # the line number of the last control flag
        line_no = 4 + line_i + 1

        # fill in the rest of the lines after the control flags
        self.all_lines[F"L{line_no + 1}"] = " "
        self.all_lines[F"L{line_no + 2}"] = " "
        self.all_lines[F"L{line_no + 3}"] = (
            4 * " " + "1" + "\t\t# Number of output points"
            # the leading spaces are needed. min. 4
        )
        self.all_lines[F"L{line_no + 4}"] = " "
        self.all_lines[F"L{line_no + 5}"] = "1" + "\t\t# Grid number"
        self.all_lines[F"L{line_no + 6}"] = "1" + "\t\t# GRU (if applicable)"
        self.all_lines[F"L{line_no + 7}"] = (
            "output" + 10 * " " + "# Output directory"  # spaces matter
        )
        self.all_lines[F"L{line_no + 8}"] = " "
        self.all_lines[F"L{line_no + 9}"] = " "
        self.all_lines[F"L{line_no + 10}"] = (
            "output" + 10 * " " + " # Output directory for total-basin files"
        )
        self.all_lines[F"L{line_no + 11}"] = (
            "# Simulation start and end date-times: "
        )
        self.all_lines[F"L{line_no + 12}"] = "# Format: YYYY JDAY HH MM"

        if self.start_date:
            self.all_lines[F"L{line_no + 13}"] = self.start_date.replace(
                "-", 4*" ")
        else:
            self.all_lines[F"L{line_no + 13}"] = "0    0    0    0"
        self.all_lines[F"L{line_no + 13}"] += "    # start from"

        if self.end_date:
            self.all_lines[F"L{line_no + 14}"] = self.end_date.replace(
                "-", 4*" ")
        else:
            self.all_lines[F"L{line_no + 14}"] = "0    0    0    0"
        self.all_lines[F"L{line_no + 14}"] += "    # end before"

    def save_file(self):
        '''
        Save the file in the host folder.
        '''
        save_path = os.path.join(
            self.host_dir_path, "MESH_input_run_options.ini"
        )
        with open(save_path, "wt", encoding="utf-8") as runfile:
            for value in self.all_lines.values():
                runfile.write(value + "\n")

    def update_file(self):
        '''
        If any of the varialbes:
            - first_tstep
            - start_date
            - end_date
        change, then we will re-create the file and replace it with the
        previous one.
        '''

        # check the dates
        if self.start_date:
            check_start_end_date_format(self.start_date)

        if self.end_date:
            check_start_end_date_format(self.end_date)

        # fill in the dict with the relevant info, and save it
        self.add_flags()
        self.write_lines()
        self.save_file()

        if self.verbose:
            message = "\nThe MESH_input_run_options.ini is updated.\n"
            message += self.report_simulation_time()
            print(message)

    def report_simulation_time(self):
        '''
        Report the simulation start and end times.

        Returns
        -------
        message : str
            The message to be printed to inform the user.
        '''

        message = ""
        if self.start_date:
            message += "Simulatuin start time (UTC): "
            message += F"{pd.to_datetime(self.start_date, format='%Y-%j-%H-%M')}\n"
        else:
            message += (
                "Simulatuin start time (UTC): "
                F"{str(pd.to_datetime(self.first_tstep, format='%Y%m%d%H'))}\n"
            )

        if self.end_date:
            message += "Simulatuin end time (UTC): "
            message += F"{pd.to_datetime(self.end_date, format='%Y-%j-%H-%M')}\n"
        else:
            message += (
                F"Simulatuin end time (UTC): "
                F"{str(pd.to_datetime(self.last_tstep, format='%Y%m%d%H'))}\n"
            )

        return message

    def __repr__(self) -> str:
        for value in self.all_lines.values():
            print(value + "\n")

        return " "
# ________________________________________________________________ <<< main >>>
