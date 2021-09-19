# Data intelligence specialiation - Final work

## Objective

To learn how to use Python's statsmodel, pmdarima and fbprophet Packages to analyze and forecast time series. 

## How to run the notebooks

Please, select a folder of your interest and download this repository to it. Remember that Anaconda python distribution should be installed prior to the execution of any content. To install it, please follow the instructions here:

https://docs.anaconda.com/anaconda/install/index.html

Once you have downloaded this repository, go inside the `data-intelligence-specialization-final-work` folder and execute the following command to create an ad-hoc environment to work:

`conda env create -f environment.yml --prefix ./data-intelligence-specialization-final-work-env`

After running this command, you will see a message saying that, in order to activate this new environment, you should execute  

`conda activate full/path/to/data-intelligence-specialization-final-work-env`

Please, do as requested.  

Finally, in order to be able to access this environment from within a Jupyter Lab Session execute the following command,

`python -m ipykernel install --user --name data-intelligence-specialization-final-work-env --display-name="data-intelligence-specialization-final-work-ker"`

followed by 

`jupyter lab`

and select the notebook of interest.
