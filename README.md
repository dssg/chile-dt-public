# Improving Workplace Safety through Proactive Inspections

## Summary: 

#### Partner: Department of Labor, Chile

Many Chilean workers face significant workplace safety issues. Dirección del Trabajo, Chile’s labor department, helps reduce the risks workers face through inspections and remediations, but they spend a lot of time inspecting facilities reactively based on complaints or facilities where no significant hazards exist. Dirección del Trabajo has taken steps to focus its limited resources on the facilities that need it most, including with some machine learning and a field trial. We will use historical violation and safety records, as well as business, geographic, and other data to help improve their inspection targeting.

## Code structure:

* `pipeline/`: Useful, generalizable function declarations to be used across this project and perhaps in future projects
* `utils/`: Non-generalizable code for our pipeline. These files are intended to be run from `run.py` but will not be directly useful to other, different projects
* `raw/`: Code from DT
* `notebooks/`: Jupyter notebooks used for exploratory analysis, generation of graphics, etc.
* `preprocessing/`: Code used for loading data to the database (not included in the pipeline)
* `extras/`: Other non-generalizable code that's not specific to any step of the analysis process
* `run.py`: The main executable. Use this file to run our analysis pipeline.
* Other files: Other code specific to this project, such as example configuration files and `.gitignore`

## How to run 

`run.py` is our main executable. Run it using ```python run.py <optional flags>```

### Requirements to run

* Python 3.6, Python packages specified in `requirements.txt`
* Connection to a PostgreSQL database, with accurate credentials included in `config.yaml`
* Experiment configuration file
* Subdirectories in the specified output directory:
    * `precision/`
    * `features/`
    * `histogram_all/`
    * `histogram_label/`
    * `models/`
    * `matrices/`

These directories can be created by running `mkdir <directory name>` or creating the folder in your machine's file management interface.

### Options for running `run.py`

* `--all`: Run the entire pipeline 
* `--load-data`: Perform ETL process and load data into database. Moves raw flat files into the `raw` schema in the database
* `--clean-data`: Clean data and move tables from `raw` schema to `cleaned` schema
* `--eda`: Print some simple descriptive statistics about the data
* `--stage-data`: Move data from `cleaned` schema to `staging` schema and prepare for modeling
* `--model` or `--models`: Train, test, and calculate metrics on all models specified in the configuration file
* `<configuration-filename.yaml>`: Optionally, pass the name of the configuration file for the experiment (must be a `.yaml`). If no filename is given, will default to the filename `config_experiments.yaml`.

Example (runs all parts of the pipeline with the given configuration): ```python run.py --all config_experiments_all_features.yaml````

### Configuration files

There are two configuration files. `config.yaml` contains personal credentials and other possibly sensitive or personal information, including:

* Credentials for PostgreSQL database
* Directories where raw data files are stored

See an example in `example_config.yaml`.

Another `.yaml` file contains the configuration for model(s) being run. Change this file to:

* Select the label of interest
    * `y`: desired column name for label
    * `label_query`: the query used to create the label
    * `null_cond`: the column to filter on when querying for only labeled data
* Decide whether to undersample the "false" labels
    * `sample`: true to undersample
    * `sample_level`: how much to undersample (.8 removes 80%) of "false" labels
* Add features by adding column names to `X`
* Change parameters for temporal validation (all units are in months)
    * `start_date` and `end_date` are the first possible day of list creation to the last possible day of label creation. Must be the first of the month
    * `window_size`: both the span of the training data for the initial temporal split and the span of the window if using a rolling window
    * `steps_ahead`: delay between creation of list and label creation
    * `pred_freq`: the span of time for which the predictions are generated
    * `rolling_window`: indicator of whether to use a rolling window or all data before
* Change imputation methods
* Indicate whether to drop and recreate the tables used to create the training and test data or use existing tables. If new features have been added to the feature generation process, then new tables must be created.

### Label queries

We've run this pipeline with two different labels. Here is the query for the label indicating whether or not the department of labor has found a violation of a labor law at a facility in that month:

```{sql}
select entity_id, month_year,
    case when s.infracted > 0 then true 
    when s.infracted = 0 then false
    end as infractor
    from staging.train_monthly_split_{table_start}_{table_end}
    left join
    (select entity_id, date_monthyear as month_year,
    sum(infra) as infracted
    from cleaned.inspections_se
    where date_monthyear >= '{table_start}'
    and date_monthyear <= '{table_end}'
    group by entity_id, date_monthyear) s
    using (month_year, entity_id)
```
and the query for whether or not the department of labor inspected a facility in that month:

```{sql}
select entity_id, month_year, 
    case when s.inspected = 1 then true else false end as inspected,
    random() as random_system_not_feature
    from staging.train_monthly_split_{table_start}_{table_end}
    left join 
    (select entity_id, date_monthyear as month_year, 1 as inspected
    from cleaned.inspections_se
    where date_monthyear >= '{table_start}'
    and date_monthyear <= '{table_end}'
    group by entity_id, date_monthyear) s
    using (month_year, entity_id)
```

## What does the pipeline do?

There are four parts to the pipeline.

### Load data (`--load-data` flag)

Our data came from two sources: flat `.xlsx` and `.csv` files, and a `.bak` SQL Server database dump. Most of the process to move data from these raw sources to the `raw` schema of our PostgreSQL database is not included in this pipeline, and therefore the data must be in the `raw` schema before running `-all`. 

For the flat files, we moved them to the database using `csvsql` and `psql`. For some files, some cleaning was needed before they could be moved to Postgres. This cleaning was done using `awk`, `sed`, and other `bash` tools.

The files necessary are in located in the `raw` directory in S3:
* `partner_data/datafinaluchicago_completa_todoeldataset_sineliminaciones.csv` --> `raw.inspections_se`: Inspection-level data. The original CSV required some cleaning before it could be uploaded to the database, including: making the header lowercase, removing the first column, and making NA values consistent across the file (in the raw form, some NAs are empty and some are `.`).
* `partner_date/datasiiproceso.csv`: Company data, with one record for each company each year, from Chile's service tax agency (SII). The raw CSV file had the same mixed NAs as the inspection data in addition to periods in the column names.
* `comunaglosa.csv`, in `partner_data/OneDrive_2_7-11-2018.zip`: Comuna (county) information
* `datafinaluchicago_funasignado.csv`, in `partner_data/OneDrive_2_7-11-2018.zip`: Inspector codes to correspond with the inspections data
* `partner_data/Oficinas_DT.xlsx`: Data about inspection offices. Was converted to a CSV.
* `economic_data/copper.csv`: Copper price data from [this link](https://www.quandl.com/data/COM/COPPER-Copper-COMEX) 
* `economic_data/macroeconomic_monthly.csv`: Other macroeconomic information from [this link](https://research.stlouisfed.org/pdl/1014)

The SQL Server data dump backup file is `partner_data/DT_UChicago.bak`. The process we used to move this data to Postgres is as follows:
* Created a SQL Server RDS instance on AWS (SQL Server Standard Edition 11.00.7462.6.V1)
* Followed [this process](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Procedural.Importing.html) to upload the `.bak` to upload the backup to RDS
* Created a Windows EC2 instance with SQL Server Management Studio
* Used [this tool](https://www.convert-in.com/mss2pgs.htm) to move SQL Server --> PostgreSQL
* Several tables did not transfer using this tool, so we used `pandas`, `pymssql`, and `sqlalchemy`. We also had to install additional software: ```sudo apt-get install unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc``` The file we used to do this is missing, but is documented in [this gist](https://gist.github.com/emmaremy/ba9e96125da03b589f3f810de365fc4e). Notice that using `pymssql` requires an additional `.yaml` file with credentials for the SQL Server database.
* Finally, we moved all tables to the `raw` schema. All tables beginning with `dt_fi` or `dt_mul` are from this data dump.

Notice that, because we received this data very late, we did not end up actually using it in our pipeline. However, we believe that this data contains a great deal more information than the original flat files and therefore ideally future work on this project would use the data dump instead of or in addition to the flat files.

We also used several files for reference that are not necessary to upload the database as they are not used in our pipeline, but are very important for context and information about the data. Full documentation about these files is included in our technical report.

### Clean data (`--clean-data` flag)

This part of the pipeline moves data from the `raw` schema to the `cleaned` schema.

Required tables for this are:
* `raw.inspections_se`: Inspection-level data
* `raw.comuna_glosa`: Comuna (county) code table to allow for matching
* `raw.office_data`: Code table for inspection offices
* `raw.taxes`: Company/year-level tax data
* `raw.inspectors`: Table of inspectors that conducted each inspection

Cleaning done here and assumptions made:
* `99` replaced with `NULL` in columns containing inspected matters (`inspections_se`)
* `9999` replaed with `NULL` in columns containing number of employees affected (`inspections_se`)
* `0`, `-1`, `3` replaced with `NULL` in columns containing comuna codes (`inspections_se`)
* Creation of "long" tables from "wide" columns

### Stage data (`--stage-data` flag)

This part of the pipeline moves data from the `cleaned` schema to the `staging` schema

Required tables for this are:
* `cleaned.inspections_se`
* `cleaned.taxes`

### Modeling (`--model` or `--models` flag)

Here is the main part of our pipeline, the model.

Requirements for this part of the pipeline:
* All tables required for feature and label generation:
    * `cleaned.copper_formatted`
    * `cleaned.macro_month`
    * `cleaned.inspections_se`
    * `cleaned.taxes`
    * `staging.entity_month_year`
    * `staging.entity_id`
    * `staging.industry_month_year`
* Appropriate experiment configuration file
* `models`, `matrices`, `precision`, `histogram_label`, `histogram_all`, `score` directories within the output directory specified in the config file
* Probably more than 32 GB of RAM
* Connection to PostgreSQL database (we pass a `sqlalchemy` engine as a parameter throughout the pipeline)

We begin by generating a list of temporal splits across our data, based on the configuration file. Then, we use the lists of parameters passed from the configuration file to create a list of models with different parameters.

For every temporal split, we:
* Check whether we have created a `staging.train_monthly_split` table
* If not, or config file says to drop tables, create a new table (process includes feature generation)
* Load train data with null values in label removed
* Load test data
* Impute feature values in both train and test

For each model within each temporal split:
* Check whether `model_group` already exists
* Check whether model already exists (if yes, skip to next model)
* Initialize `scikit-learn` model object with appropriate parameters
* Train model
* Test model and generate risk scores
* Write results to `results` schema
* Pickle and HDF5 files written for model and model matrix (saved to `output_dir` given in config file)
* Evaluation graphs created and written to `output_dir`

## Other things to know

### Masking company IDs

In some of the data we received, company IDs were masked, and in other data they were not. As a result, depending on what data is used, it may be necessary to mask company IDs to allow for matching. The script `preprocessing/mask.R` can be used to do this.

### Evaluation functions

There are helpful functions for post-modeling comparison and evaluation of models in `utils/evaluation.py` that are not called from the pipeline.
