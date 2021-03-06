# UNLP - Data intelligence Specialization - Final work

## Objective

## How to run the notebooks

Please, select a folder of your interest and download this repository to it. Remember that Anaconda python distribution should be installed prior to the execution of any content. To install it, please follow the instructions here:

https://docs.anaconda.com/anaconda/install/index.html

Once you have downloaded this repository, execute the following command to create an ad-hoc Conda environment to work:

`conda env create -f environment.yml --prefix ./eth_returns_forecaster_env`

After running this command, you will see a message saying that, in order to activate this new environment, you should execute  

`conda activate full/path/to/eth_returns_forecaster_env`

Please, do as requested.  

Finally, in order to be able to access this environment from within a Jupyter Lab Session execute the following command,

`python -m ipykernel install --user --name eth_returns_forecaster_env --display-name="eth_returns_forecaster_ker"`

followed by 

`jupyter lab`

and select the notebook of interest.
