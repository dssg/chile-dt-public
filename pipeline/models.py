"""
Functions for creating models
"""
import sys
sys.path.append('../')

import pipeline.results as plres
import pipeline.metrics as plmet
import pipeline.sql as plsql

from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import pickle
import datetime

from sklearn.metrics import precision_score 
from dateutil.relativedelta import relativedelta
from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)
from triage.component.catwalk.estimators.classifiers import ScaledLogisticRegression

class ConfigError():
    pass             
        
"""
Function to initialize a single model
"""

def initialize_models(model_type, parameters, n_cores = -1):
    """
    Function initializes list of models

    Inputs:
    model_type -- model type
    parameters -- hyperparameters dictionary
    n_cores -- number of cores to us, default 1
                               
    Results:
    Initialized model
    """
    
    if model_type == "RandomForest":
        return ensemble.RandomForestClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            random_state=parameters['random_state'],
            n_jobs=n_cores)

    elif model_type == 'ScaledLogisticRegression':
        return ScaledLogisticRegression(
                C=parameters['C_reg'],
                penalty=parameters['penalty'],
                random_state=parameters['random_state'],
                n_jobs = n_cores)

    elif model_type == "RandomForestBagging":
        #TODO Make Model Bagging
        return ensemble.BaggingClassifier(
                    ensemble.RandomForestClassifier(
                        n_estimators=parameters['n_estimators'],
                        max_features=parameters['max_features'],
                        criterion=parameters['criterion'],
                        max_depth=parameters['max_depth'],
                        min_samples_split=parameters['min_samples_split'],
                        random_state=parameters['random_state'],
                        n_jobs=n_cores),
                    #Bagging parameters
                    n_estimators=parameters['n_estimators_bag'],
                    max_samples=parameters['max_samples'],
                    max_features=parameters['max_features_bag'],
                    bootstrap=parameters['bootstrap'],
                    bootstrap_features=parameters['bootstrap_features'],
                    n_jobs=n_cores)

    elif model_type == "RandomForestBoosting":
        #TODO Make Model Boosting
        return ensemble.AdaBoostClassifier(
            ensemble.RandomForestClassifier(
                n_estimators=parameters['n_estimators'],
                max_features=parameters['max_features'],
                criterion=parameters['criterion'],
                max_depth=parameters['max_depth'],
                min_samples_split=parameters['min_samples_split'],
                random_state=parameters['random_state'],
                n_jobs=n_cores),
            #Boosting parameters
            learning_rate=parameters['learning_rate'],
            algorithm=parameters['algorithm'],
            n_estimators=parameters['n_estimators_boost'])

    elif model_type == 'SVM':
        return svm.SVC(C=parameters['C_reg'],
                       kernel=parameters['kernel'],
                       probability=True,
                       random_state=parameters['random_state'])

    elif model_type == 'LogisticRegression':
        return linear_model.LogisticRegression(
            C=parameters['C_reg'],
            penalty=parameters['penalty'],
            random_state=parameters['random_state'])

    elif model_type == 'AdaBoost':
        return ensemble.AdaBoostClassifier(
            learning_rate=parameters['learning_rate'],
            algorithm=parameters['algorithm'],
            n_estimators=parameters['n_estimators'],
            random_state=parameters['random_state'])

    elif model_type == 'ExtraTrees':
        return ensemble.ExtraTreesClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            random_state=parameters['random_state'],
            n_jobs=n_cores)

    elif model_type == 'GradientBoostingClassifier':
        return ensemble.GradientBoostingClassifier(
            n_estimators=parameters['n_estimators'],
            learning_rate=parameters['learning_rate'],
            subsample=parameters['subsample'],
            max_depth=parameters['max_depth'],
            random_state=parameters['random_state'])

    elif model_type == 'GaussianNB':
        return naive_bayes.GaussianNB()

    elif model_type == 'DecisionTreeClassifier':
        return tree.DecisionTreeClassifier(
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            random_state=parameters['random_state'])

    elif model_type == 'SGDClassifier':
        return linear_model.SGDClassifier(
            loss=parameters['loss'],
            penalty=parameters['penalty'],
            random_state=parameters['random_state'],
            n_jobs=n_cores)

    elif model_type == 'KNeighborsClassifier':
        return neighbors.KNeighborsClassifier(
            n_neighbors=parameters['n_neighbors'],
            weights=parameters['weights'],
            algorithm=parameters['algorithm'],
            n_jobs=n_cores)

    else:
        raise ConfigError("Sorry, unsupported model {}".format(model_type))


        
"""
Function to perform model with model_id
"""

def train_model(model, model_type, training_X, training_y):
    
    # Pull out the model object
    clf = model

    # Train model using function defined
    clf.fit(training_X.values, training_y)

    return clf
    

"""
Function to perform temporal validation
"""

def temporal_validation(beginning, end_date, 
        window_size = 24, steps_ahead = 1, rolling_window = False, pred_freq = 1):
    """
    This function performs monthly temporal validation.
    
    Inputs:
    start_date -- date to consider the beginning of time
    end_date -- date to consider the end of time
    window_size -- the size of the rolling window in months/minimum training size of non-rolling window (integer)
    steps_ahead -- number of months delay in getting labels
    pred_freq -- number of months predicting for
    rolling_window -- whether to validate with a rolling window or not (boolean)
    
    Returns:
    Matrix of dates for temporal validation folds
        Columns: TRAINING_FEATURES_START
                 TRAINING_FEATURES_END
                 TRAINING_LABELS_START
                 TRAINING_LABELS_END
                 TEST_FEATURES_START
                 TEST_FEATURES_END
                 TEST_LABELS_START
                 TEST_LABELS_END

    """

    # Get month-year list
    month_year = pd.date_range(start=beginning, end=end_date, freq='MS')
    beginning = month_year[0]
    
    # Initialize table
    time_splits = pd.DataFrame(columns=['training_features_start',
        'training_features_end',
        'training_labels_start',
        'training_labels_end',
        'test_features_start',
        'test_features_end',
        'test_labels_start',
        'test_labels_end'])

    # Initialize list of rows (efficient way to loop data --> pandas)
    rows_list = []

    # Loop over month-year
    for i in range(0, len(month_year), pred_freq):
        start = month_year[i]
        if ((start >= month_year[window_size - 1] and 
            start >= month_year[steps_ahead + pred_freq]) and
            (start <= month_year[-(steps_ahead+pred_freq)])):

            # Initialize row and other values
            row = {}
            training_features_end = start - relativedelta(months=steps_ahead)
            training_labels_end = start
            test_features_start = start
            test_labels_start = start + relativedelta(months=steps_ahead)
            test_features_end = start + relativedelta(months=pred_freq - steps_ahead)
            test_labels_end = start + relativedelta(months=steps_ahead + pred_freq - 1)

            if rolling_window == True:
                training_features_start = start - relativedelta(months=window_size)
                training_labels_start = start - relativedelta(months=window_size + steps_ahead)

            else:
                training_features_start = beginning
                training_labels_start = beginning + relativedelta(months=steps_ahead)

            # Save row
            row.update({'training_features_start': training_features_start,
                'training_features_end': training_features_end,
                'training_labels_start': training_labels_start,
                'training_labels_end': training_labels_end,
                'test_features_start': test_features_start,
                'test_features_end': test_features_end,
                'test_labels_start': test_labels_start,
                'test_labels_end': test_labels_end})
            rows_list.append(row)

    return pd.DataFrame(rows_list)

