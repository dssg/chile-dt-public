"""
Functions for generating and writing results of models

Some of this code is hijacked and adapted from police-eis and also nyc_peu_inspections """

import numpy as np
import pandas as pd
import hashlib
import pipeline.sql as plsql
import pipeline.metrics as plmet
import json
import pickle

from sklearn.metrics import roc_curve, auc


"""
Generic function to create a hashed ID from a list
    Given: list
    Returns: hashed UUID
"""
def hash_id(ls):
    to_hash = str.encode('-'.join(map(str, ls)))
    return hashlib.md5(to_hash).hexdigest()

"""
Function to get model_id
    Given: run_time,
        model_type,
        model_params,
        config,
        train_end_time
    Returns: hash for model_id
"""
def hash_model_id(model_type, model_params, X, config, train_end_time):
    return hash_id([model_type, model_params, X, config, train_end_time])


"""
Function to hash matrix_id
    Given: features,
        train_end_time,
        config for validation
    Returns: hash for matrix_id
"""
def hash_matrix_id(X, train_start_time, train_end_time, config):
    return hash_id([X, config, train_start_time, train_end_time])

"""
Function to create model row
    Given: model_group_id
    Returns: boolean indicator whether model existed,
        model_id
"""
def check_model_row(engine, role, model_group_id, model_hash):

    # Check whether model has already been run
    df = plsql.query("""set role {};
        select * from results.models
        where model_group_id = '{}' and 
        model_hash = '{}';
        """.format(role, model_group_id, model_hash), engine)

    # If model already exists
    if df.shape[0] == 1:
        
        # Also check other tables, in case there were errors mid-run
        df_pred = plsql.query("""set role {};
            select * from results.predictions
            where model_id = '{}';
            """.format(role, int(df.model_id)), engine)

        df_eval = plsql.query("""set role {};
            select * from results.evaluations
            where model_id = '{}';
            """.format(role, int(df.model_id)), engine)

        if df_pred.shape[0] < 1 or df_eval.shape[0] < 1:

            # On a previous run, predictions/evaluations not written correctly
            plsql.query_no_return("""set role {};
            delete from results.feature_importances
            where model_id = {mod_id};
            delete from results.evaluations
            where model_id = {mod_id};
            delete from results.predictions
            where model_id = {mod_id};
            delete from results.models
            where model_id = {mod_id};
            """.format(role, mod_id = int(df.model_id)), engine)
            return False, int(df.model_id)

        return True, int(df.model_id)

    # If model doesn't exist
    elif df.shape[0] == 0:
        
        # Get next model_id
        df = plsql.query("""set role {};
            select * from results.models
            order by model_id desc limit 1;
            """.format(role), engine)
        if df.shape[0] == 0:
            model_id = 1
        else:
            model_id = df.model_id + 1
        return False, model_id

    else:
        print("This model exists multiple times. Something funky happening.")
        exit()

"""
Write the model row to results.models
"""
def write_model_row(engine, role,
        model_id,
        model_group_id,
        run_time_start,
        run_time_end,
        batch_run_time,
        model_type,
        model_params,
        config,
        test, 
        model_hash,
        train_matrix_uuid,
        train_end_time,
        model_comment = None,
        batch_comment = None):

    # Create new row
    df = pd.DataFrame({'model_id':model_id, 
        'model_group_id':model_group_id, 
        'run_time':run_time_start,
        'run_time_end':run_time_end,
        'batch_run_time':batch_run_time, 
        'model_type':model_type, 
        'model_parameters':json.dumps(model_params), 
        'model_comment':model_comment, 
        'batch_comment':batch_comment, 
        'config':json.dumps(config), 
        'test':test,
        'model_hash':model_hash, 
        'train_matrix_uuid':train_matrix_uuid,
        'train_end_time':train_end_time},
        index=[0])
    
    # Write to database
    df.to_sql("models", engine, schema = "results",
            if_exists = 'append', index = False)


"""
Write the model predictions to results.predictions
"""
def write_predictions(engine, role, 
        model_id,
        as_of_date, 
        entity_id, 
        y_scores, 
        y_true,
        matrix_uuid):

    # Create initial table
    df = pd.DataFrame({ 
        "entity_id": entity_id,
        "score": y_scores,
        "label_value": y_true
        })

    # Add remaining columns
    # TODO: also sort by label
    df['model_id'] = model_id
    df['as_of_date'] = as_of_date
    df['matrix_uuid'] = matrix_uuid

    # Add ranking
    df['rank_abs'] = df['score'].rank(method='dense', ascending=False)
    df['rank_pct'] = df['score'].rank(method='dense', ascending=False, pct=True)

    # Write to database
    df.to_sql("predictions", engine, if_exists="append", schema="results", index=False)



"""
Function to check for and write to initial results tables
    Given: sqlalchemy engine and role,
        algorithm_type,
        model_params,
        feature_list,
        model_config
    Does: checks for the given model group and model,
        writes new model group if not exists
    Returns: model_group_id, model_id if existing
"""
def check_model_group(engine, role, model_type, model_params, X, y, config):
    # Check for existing model group
    df = plsql.query("""set role {};
        select * from results.model_groups
        where model_type = '{}'
        and label = '{}'
        and feature_list = '{{{}}}'
        and model_parameters = '{}'
        and model_config = '{}'
        order by model_group_id desc;
        """.format(role, model_type, y, ','.join(sorted(X)), 
            json.dumps(model_params, sort_keys = True),
            json.dumps(config)), engine)

    # If model group exists, return ID
    if df.shape[0] == 1:
        print("Model group found.")
        return df.model_group_id[0]

    # If model group doesn't exist
    elif df.shape[0] == 0:
        print("No model group found, creating new model group.")
        
        # Get model_group_id
        most_recent = plsql.query("""set role {};
            select model_group_id
            from results.model_groups
            order by model_group_id desc
            limit 1;
            """.format(role), engine)
        if most_recent.shape[0] == 0:
            model_group_id = 1
        else:
            model_group_id = int(most_recent.model_group_id[0]) + 1

        # Create data frame
        df = pd.DataFrame({"model_group_id": model_group_id, 
            "model_type": model_type, 
            "model_parameters": json.dumps(model_params, sort_keys = True),
            "feature_list":'{'+ ','.join(sorted(X)) + '}', 
            "label":y, 
            "model_config":json.dumps(config, sort_keys = True)},
            index = [0])

        print("final query")
        # Write to results.model_groups
        df.to_sql("model_groups", engine, schema = "results", if_exists = "append", index = False)
        return model_group_id

    else:
        print("Several model groups found that match this. Something is wrong?")
        # TODO: more robust error-checking
        return df.model_group_id[0]


"""
Function to get feature importances
    Given: trained model
    Returns: appropriate feature importance array for the model type
    Note: adapted from police-eis
"""
def get_feature_imps(model):

    # If the scikit model has them built in, use them
    try: 
        return model.feature_importances_.copy()
    except:
        pass
    
    # If it doesn't, use coefficient
    try: 
        if len(model.coef_) <= 1:
            return list(model.coef_.tolist()[0])
        else:
            return list(model.coef_)
    except:
        pass
    
    # Or else just using a None object
    return None

"""
Function to get predicted scores
"""
def get_scores(model, test_data):
    """
    model: trained model
    test_data: testing data
    """
    
    try:
        return model.predict_proba(test_data)
    except Exception as e:
        print(e)

    try:
        return model.decision_function(test_data)
    except Exception as e:
        print(e)

    print("No function to get results works for this model type.")

"""
Write the feature importances to results.feature_importances
"""
def write_feature_imps(engine,
	model_id,
	feature,
	feature_importance
	):
    
    # Add new row
    df = pd.DataFrame({
       	"feature": feature, 
        'feature_importance': feature_importance
       	})

    # Add remaining columns
    df['model_id'] = int(model_id) 

    # Add rank columns
    df['rank_abs'] = df['feature_importance'].rank(method='dense', ascending=False)
    df['rank_pct'] = df['feature_importance'].rank(method='dense', ascending=False, pct=True)

    # Write to database
    df.to_sql("feature_importances", engine, schema = "results",
	    if_exists = 'append', index = False)	
 
"""
Function to write metrics to results.evaluations
"""
def write_evaluations(engine, model_id, 
        metrics, values,
        eval_start_time,
        eval_end_time,
        pred_freq,
        comment = None):

    # Create initial table
    df = pd.DataFrame({
        "metric": metrics,
        "value": values})

    # Add columns of same value
    df['model_id'] = int(model_id)
    df['evaluation_start_time'] = eval_start_time
    df['evaluation_end_time'] = eval_end_time
    df['prediction_frequency'] = pred_freq
    df['comment'] = comment

    # Write to database
    df.to_sql("evaluations", engine, if_exists="append", schema="results", index=False)


"""
Function to write results for one model
"""
def write_results(engine, role, config, model, model_id, model_group_id, model_hash, 
        model_type, start, end, X, y, test_X, test_X_notimputed, 
        test_y, test_data, run_time, end_run_time, 
        batch_run_time, 
        y_score, model_params, pred_freq = '1 month',
        output_dir = './'):

    # TODO: pass output directory from config file

    print("Writing results for model {} ({})".format(int(model_id), model_type))

    # Save models to pickle file
    with open(output_dir + '/models/{}'.format(model_hash), 'wb') as f:
        pickle.dump(model, f)
        
    print("Hashing matrix") 
    # Calculate hash for matrix and save to file
    matrix_id = hash_matrix_id(X, start, end, config['validation'])
    
    print("Saving")
    print(matrix_id)
    
    test_data[X + [y]].to_hdf(output_dir + '/matrices/{}.hdf5'.format(matrix_id), key='train')

    print("Hashed matrix, writing model row")
    
    # Write model to results.models
    write_model_row(engine, role, model_id, model_group_id, run_time, end_run_time,
            batch_run_time, model_type, model_params,
            config['validation'], True, model_hash, matrix_id, start)
    
    print("Feature importances")
    
    # Get and save feature importances
    feature_importance = get_feature_imps(model)
    write_feature_imps(engine, model_id, X, feature_importance)

    print("Predictions")
    
    # Write predictions
    write_predictions(engine, role, int(model_id), start, test_data['entity_id'],
            y_score[:,1], test_y, matrix_id)

    print("Metrics")
    
    # Write metrics
    # -- for all data
    metrics = plmet.all_metrics_evaluations(y_score[:,1],
            test_y, config['metrics']['cutoffs'][-1], config['metrics']['cutoffs'], 
            'all_data')
    # -- for subsets of data designated in config
    for subset in config['metrics']['subset_features']:
        index_subset = [test_X_notimputed[subset] == 1]
        if len(test_X.loc[test_X_notimputed[subset] == 1]) == 0:
            print("No true values for subset on {}. Skipping.".format(subset))
            continue
        metrics.update(plmet.all_metrics_evaluations(y_score[:,1][index_subset],
            test_y.loc[test_X_notimputed[subset] == 1], config['metrics']['cutoffs'][-1], config['metrics']['cutoffs'],
            subset))
    write_evaluations(engine, model_id, list(metrics.keys()), list(metrics.values()), 
            start, end, pred_freq)

    print("Done")
