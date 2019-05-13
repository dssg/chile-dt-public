"""
Main executable

Command line options:
    - '--all' to run everything
    - '--load-data' if you want to perform the data loading process
    - '--eda' if you want to print some simple descriptive statistics
    - '--models' or '--model' to build models
"""
                
# Import modules from pipeline/
# and from utils/
import pipeline.sql as plsql
import pipeline.models as plmod
import pipeline.results as plres
import utils.load_data as uload
import utils.eda as ueda
import utils.models as umod
import utils.clean_data as uclean
import utils.stage_data as ustage
import utils.stage_data as ustage

import utils.temp_clean as utclean

from sys import argv
from os import path

from random import choice

def main():

    # Check for and load the correct .yaml files
    if not path.exists('config.yaml'):
        print('Please create or rename your .yaml file to config.yaml.')
        exit()
    config = plsql.read_yaml('config.yaml')

    # Allow for custom config_experiment file,
    # but default to config_experiments.yaml
    config_exp_path_ls = [path for path in argv if '.yaml' in path]
    if len(config_exp_path_ls) != 0:
        config_exp_path = config_exp_path_ls[0]
    else:
        config_exp_path = 'config_experiments.yaml'
    if not path.exists(config_exp_path):
        print('Missing experiment configuration yaml file.')
        exit()
    config_exp = plsql.read_yaml(config_exp_path)

    # Create sqlalchemy engine for SQL queries
    engine = plsql.create_engine('config.yaml')


    ######### Load data part of pipeline
    if '--load-data' in argv:
        print("\nLOADING DATA.......")

        print("WARNING: this part of the pipeline is incomplete.")

        # Get data locations
        uload.load_data(engine, config['data_dirs'])

    ########## Clean data part of pipeline
    if '--clean-data' in argv or '--all' in argv:

        # Make raw files usable
        print("Currently only cleaning data, not loading.")
        uclean.clean_data(engine)
        
    ######### Staging data
    if '--stage-data' in argv or '--all' in argv:
        print("\nSTAGING DATA........")
        ustage.stage_data(engine)

    ######### Model
    if '--model' in argv or '--models' in argv or '--all' in argv:

        print("\nMODELING.......")

        # Create models
        umod.create_models(engine, config_exp, config['output_dir'])
        

    # TODO: Silly test, don't leave this here
    print(choice(["I wish Chile had qualified for the World Cup.",
        "Wow, Team Chile is the best team!",
        "Let's go to Kilwin's!", 
        "wow look at all these great results!!!!", 
        "Who is guarding DSaPP's hot Cheetos?", 
        ":viola:"]))

if __name__ == "__main__":
    main()

