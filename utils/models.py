"""
Pipeline code for creating models
""" 
import sys
sys.path.append('../')

import pipeline.models as plmod
import pipeline.sql as plsql
import pipeline.results as plres
import pipeline.imputation as plimp
import utils.tables as utab
import utils.evaluation as pleva
import pandas as pd
import numpy as np
import datetime
import itertools

"""
Runs SQL query to create table for training or test
    Given: sqlalchemy engine, label, 
        list of features,
        a string either "training" or "test" that will indicate whether to include nulls
"""
def get_table(engine, y, X, start_month, end_month, label_query, 
        nulls = True, sample = False, sample_level = .8):
    # Test data: include nulls if nulls == True:
    if nulls == True:
        na = ""
    # Training data: don't include nulls
    else:
        na = " where {} is not null".format(y)

    # If undersampling data to correct imbalance
    if sample == True:
        samp = "random_system_not_feature > {} or {} is true".format(sample_level, y)
        if nulls == False:
            samp = " and " + samp
        else:
            samp = " where " + samp
    else:
        samp = ""

    # Get strings for correct table name
    table_start = start_month.strftime('%Y-%m-%d').replace('-','')
    table_end = end_month.strftime('%Y-%m-%d').replace('-','')

    # Load in the model matrix
    lab_qry = label_query.format(table_start = table_start,
            table_end = table_end)
    qry = """set role direccion_trabajo_inspections_write;
        with 
        label as ({lab_qry})
        select t.entity_id, t.month_year, {features}, {label}
        from staging.train_monthly_split_{table_start}_{table_end} t
        left join label l
        on t.entity_id = l.entity_id
        and t.month_year = l.month_year
        {na}{samp};
        """.format(lab_qry = lab_qry, 
                features = ', '.join(X), label = y, 
                table_start = table_start, 
                table_end = table_end, na = na,
                label_query = label_query,
                samp = samp)
    df = plsql.query(qry, engine)

    return df
    

"""
Checks for existence of time split staging table
    and creates it if that's what you want or it doesn't yet exist
"""
def check_table(engine, start_date, end_date, drop_table = False):

    table_start = start_date.strftime('%Y-%m-%d').replace('-','')
    table_end = end_date.strftime('%Y-%m-%d').replace('-','')

    # Check for existence of staging table for TRAINING time split
    df = plsql.query("""set role direccion_trabajo_inspections_write;
        select table_name from information_schema.tables
        where table_schema = 'staging'
        and table_name = 'train_monthly_split_{}_{}';
        """.format(table_start, table_end), engine)
        
    # If doesn't exist or config says to drop table, create new table (TRAINING)
    if df.shape[0] == 0 or drop_table == True:
        utab.create_table(engine, start_date, end_date)

"""
Create grid over all model parameters
"""
def create_grid(config):
    d = {}
    for model_type in config['models']:
        param_names = list(config['parameters'][model_type].keys())
        i = 0
        for item in itertools.product(*list(config['parameters'][model_type].values())):
            model_str = 'model_' + model_type + '_' + str(i)
            d[model_str] = {}
            d[model_str][model_type] = {}
            for l in range(len(param_names)):
                d[model_str][model_type][param_names[l]] = item[l]
            i = i + 1
    return d


"""
Creates model(s)
"""
def create_models(engine, config, output_dir = './'):

    # Do this ridiculous write query to avoid permission error
    plsql.query_no_return("""
        set role direccion_trabajo_inspections_write;
        drop table if exists staging.permissions;
        create table staging.permissions as select 1;
        drop table if exists staging.permissions;
        """, engine)
    
    # Pull variables out of config file
    y = config['y']
    X = config['X']

    # Create a dictionary of models with different params
    models = create_grid(config)
    
    # If doing temporal validation
    time_splits = plmod.temporal_validation(
            config['validation']['start_date'],
            config['validation']['end_date'],
            window_size = config['validation']['window_size'],
            steps_ahead = config['validation']['steps_ahead'],
            pred_freq = config['validation']['pred_freq'],
            rolling_window = config['validation']['rolling_window'])

    # Save start time of batch
    batch_run_time = datetime.datetime.utcnow()

    # Loop through time folds
    for index, row in time_splits.iterrows():

        # Check for existence of staging tables for this time split
        # and create it if desired/doesn't exist
        check_table(engine, row.training_labels_start, row.training_labels_end, config['drop_table'])
        check_table(engine, row.test_labels_start, row.test_labels_end, config['drop_table'])

        # Load appropriate data from staging tables
        training_data = get_table(engine, config['y'], X, 
                row.training_labels_start, 
                row.training_labels_end,
                config['label_query'],
                nulls = False,
                sample = config['sample'])
        training_X = training_data[X]
        training_y = training_data[y]
        print("Imputing training set features...")
        training_X = plimp.impute(training_X, config)
        print("Imputation complete.")
        test_data = get_table(engine, config['y'], X, 
                row.test_labels_start, 
                row.test_labels_end, config['label_query'])
        test_y = test_data[y]
        test_X_raw = test_data[X]
        print("Imputing test set features...")
        test_X = plimp.impute(test_X_raw, config)
        print("Imputation complete.")


        # Loop through models
        for model in models:
            
            model_type = list(models[model].keys())[0]
            model_params = models[model][model_type]
            print(model_type)
            print(model_params)

            # Initialize model
            model = plmod.initialize_models(model_type = model_type,
                        parameters = model_params, 
                        n_cores = -1)

            # Get model group id
            config_mg = {**config['validation'], **config['imputation']}
            model_group_id = plres.check_model_group(engine = engine, 
                            role = 'direccion_trabajo_inspections_write', 
                            model_type = model_type, 
                            model_params = model_params, 
                            X = config['X'], y = config['y'], 
                            config = config_mg)

            # Calulate hashed id for model
            model_hash = plres.hash_model_id(model_type, model_params,
                    X, 
                    config_mg,
                    row.training_features_end)

            # Check whether we've run this model
            exists, model_id = plres.check_model_row(engine, 
                    "direccion_trabajo_inspections_write", model_group_id,
                    model_hash)
            if exists == True:
                print("Model {} already exists, skipping".format(int(model_id)))
                continue
            
           # Start run time for single model
            print("Running model {} ({})".format(int(model_id), model_type))
            run_time = datetime.datetime.utcnow()
        
            # Train model
            print("Training model.")
            fitted = plmod.train_model(model, model_type, training_X, training_y)

            # Test model
            print("Testing model.")
            y_score = plres.get_scores(fitted, test_X[training_X.columns.values].values)

            # End run time for single model
            end_run_time = datetime.datetime.utcnow()
            
            # Write to results
            plres.write_results(engine, 
                    "direccion_trabajo_inspections_write", config, model, model_id,
                    model_group_id, model_hash, model_type, row.test_features_start, 
                    row.test_features_end,
                    list(training_X.columns.values), y, test_X, 
                    test_X_raw, test_y, test_data, 
                    run_time, end_run_time, 
                    batch_run_time,
                    y_score, model_params,
                    str(config['validation']['pred_freq']) + ' month', output_dir)
            
            # Create graphs
            pleva.model_id_graphs(int(model_id), "direccion_trabajo_inspections_write", engine, output_dir)
