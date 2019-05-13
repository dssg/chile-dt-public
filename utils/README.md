# Utils

This directory is for pipeline functions that are not generalizable to other projects. Effectively, it is just a way to prevent `run.py` from becoming very long.

Files in this directory:

## `load_data.py`: 
Code to load data into the `raw` schema. This is not currently being run from the pipeline. Future work would complete and automate this process.

* `clean_csv(data_dirs):` Cleans `taxes` and `inspections` csv files received to postgres friendly format by turning column names to lower case, removing spaces, and removing/ replacing other unwanted signs from columns names.
* `load_data(engine, data_dirs):` Loads the cleaned csv files into the database by creating a schema and an empty table in the database. The data from cleaned csv files is then copied to this empty table. 

## `clean_data.py`: 
Code to move data from `raw` schema into `cleaned` schema.

* `clean_data(engine):` 
  * Creates a cleaner version (`cleaned.inspections_se`) of the inspections dataset from the raw schema by:
    * Converting arbitrary null values (99, 0, -1, 3 etc) to `NULL`
    * Adding inspector IDs from `raw.inspectors`
  * Merges `raw.comunaglosa` with `cleaned.inspections_se`
  * Adds column `enitity_id`, proxy for facility to `cleaned.inspections_se`
  * Merges `raw.office_data` with `cleaned.inspections_se`
  * Creates long tables of inspected and infracted matters using 
  
       `create_long_table(engine, role,` 
       `old_schema, old_table, new_table, `
       ` array_col, selected_cols, new_schema,`
       `new_col='new', sep=',')` from `pipeline\sql.py`
  * Creates a cleaner version (`cleaned.taxes`) of the taxes dataset from raw schema by:
    * Changing `Sin Comuna` to `NULL`
    * Renaming certain comunas to match comuna values in `raw.comunaglosa`
    
## `stage_data.py`: 
Code to move data from `cleaned` schema into `staging` schema.

* `stage_data(engine):` 
  * Creates `entity_id` for all companies in `cleaned.inspections_se` and `cleaned.taxes`
  * Creates a table for all dates with 1 month increment starting from `2006-01-01` to `2016-12-01`
  * Creates a table for all rutmasks from `cleaned.inspections_se` with the comuna that company falls in (by datereg) and the     dates when an inspection was registered for that company
  * Creates 
    * tmp_ins_ind - industries for all `entity_id` from `cleaned.inspections_se`
    * tmp_taxes_ind - industries for all `entity_id` from `cleaned.taxes`
  
## `models.py`: 
Code to create models. This is arguably the most important part of our pipeline.

* `get_table(engine, y, X, start_month, end_month, label_query,` 
        `    nulls = True, sample = False, sample_level = .8):` Creates tables for training/ testing. The function is called later in `create_models` and uses parameters specified in `config_experiments.yaml`.
* `check_table(engine, start_date, end_date, drop_table = False):` Checks if the training tables required to run the model already exist. If the table doesn't exist or has been dropped (`config_experiments.yaml`) the table will be made. This function helps save time and memory while running the model by not re-making tables that have been made before.
* `create_grid(config):` This function creates a grid of models with parameters specified in `config_experiments.yaml`. This helps run multiple `model_types` -- todo
* `create_models(engine, config, output_dir = './'):` This function performs the following tasks:
  * Pulls features and label from `config_experiments.yaml`
  * Creates train-test splits basis temporal validation parameters specified in `config_experiments.yaml`
  * Loops through time folds for all models specified in `config_experiments.yaml`, and trains and tests models
  * Writes results to `results.schema`

## `tables.py`: 
One big query to create tables for each training or test split. This will need to be edited if adding new features or data sources.

## `evaluation.py`: 
Code to generate graphics for evaluating models. For example, precision/recall plots.
