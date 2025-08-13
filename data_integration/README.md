# data-integration
This folder contains all of the scripts which clean and convert our raw source data into our standardized OGIM schema. The intent is that, by running all of the Python scripts present in this folder, ALL of the raw data that contributes to a version of the OGIM are integrated, and the outputs are saved in one place.

## Using `run_integration_scripts.bat`

 1. Open an Anaconda Prompt window. Confirm that this command prompt program is capable of running Python. You can test this by typing `python` into the command prompt and pressing enter. If you get a message like the following, this command prompt can execute your Python scripts. 

`>> Python 3.9.18 (main, Sep 11 2023, 14:09:26) [MSC v.1916 64 bit (AMD64)] on win32`
`>> Type "help", "copyright", "credits" or "license" for more information.`

(Be sure to type `exit()` after this test to leave the interactive Python you started in the command prompt.)
If you're using a command prompt program that cannot run Python, such as Window's default Command Prompt, you might get a message like this:

`'python' is not recognized as an internal or external command, operable program or batch file.`

 2. Activate the correct Python environment. This ensures that your scripts run inside of an environment that has necessary packages like geopandas and pandas already installed.
 
 `conda activate my_environment_name`

 3. Navigate to the directory containing the Python scripts you want to run, a.k.a. the `data_integration` folder of this repo, on your local machine, using the `cd` command.

`cd C:\path\to\folder\ogim-msat\data_integration`

 4. Run the batch file by writing the name of the batch file, followed by a space and then the path to this repo's `functions` folder on your local machine. The path to the `functions` folder is the one parameter required by the batch file, and this parameter is necessary so that each data integration script that gets run is able to properly load the custom functions we have written and housed in the `functions` folder.

`run_integration_scripts.bat C:\path\to\folder\ogim-msat\functions`