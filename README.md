**A Python Wrapper for the SVS Land-Surface Model**

**Description**

This project provides a Python interface for interacting with the SVS (Soil, Vegetation, and Snow) land-surface model (written in Fortran). This wrapper simplifies the process of configuring input parameters and executing the SVS model within a Pythonic workflow.

<b> To use this wrapper, you must provide the compiled SVS executable. </b>

**Key Features**

* **Input Management:** Streamlined configuration of SVS model parameters and input files.
* **Output Handling:** Convenient access to SVS model output files via Pandas DataFrames.
* **Parallel Execution:** Run ensemble SVS simulations in parallel using Python's concurrent.futures module.

**Prerequisites**

* Python 3.10

```bash
pip install -r requirements.txt
```

**Installation**



**Usage**

```bash
git clone hhhtps://github.com/Alireza-Amani/svspyed
cd svspyed
pip install .
```

**Note**
<p>
The SVS model is developed and maintained by Environment and Climate Change Canada (ECCC).
The SVS source code is not included in this repository.
Please consult the following website for information on obtaining the SVS source code: https://wiki.gccollab.ca/MESH/MESH-SVS

Please do not hesitate to contact me if you have any questions or are interested in using this wrapper for your research.
</p>
