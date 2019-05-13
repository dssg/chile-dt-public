"""
Calculate metrics 

Some code was borrowed froa https://github.com/dssg/police-eis/blob/master/eis/scoring.py
"""

import pandas as pd
import numpy as np

from sklearn import metrics


def generate_cutoff_at_k(len_scores, x_value, unit='abs'):
    """
    This function returns the number of facilities at the cut off 
    
    Inputs:
    y_pred -- list of predicted labels yielded from the model x_value -- number or percentage of facilities included
    unit -- type of cutoff numeric or absolute

    Output:
    predictions and labels ordered and filtered by threshold or num_predictions
    """
    if unit == 'pct':
        cutoff_value = int(len_scores * (x_value))
    else:
        cutoff_value = int(x_value)

    return cutoff_value



def binary_cutoff(scores, threshold):
    """
    Performs the binary cutoff for a give top k.
    
    Parameters
        scores: list of scores
        num_k: number of entities at top k
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        
    Returns:  
    List of predicted values
    """
    # If cutoff is larger than number of obs then use number of obs 
    if threshold > len(scores):
        y_pred = [1]*len(scores)
    else:
        y_pred = [1]*threshold + [0]*(len(scores) - threshold)

    return y_pred



def ordered_df_atk(scores, y_true, threshold, drop_na = False):
    """
    Returns data frame ordered by scores and with new column of predicted labels
    
    Parameters
        scores: list of scores
        y_true: list of true labels
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        class_type: type of threshold, probability (threshold_p) or top k (threshold_k)
        
    Returns:  
    data frame ordered and with labels
    """
    # Ordered scores
    df = (pd.DataFrame(data = {'scores' : scores,
                               'y_true' : y_true}).
          sort_values('scores', ascending=False).
          reset_index(drop = True))

    if drop_na == True:
        df = df.dropna()
        
    # facilities to be inspected at cutoff
    df['y_pred'] = binary_cutoff(df['scores'], threshold)
    df['y_true'] = pd.to_numeric(df.y_true)
    
    # NULL treatment
    # if null no change
    df['y_true_omit'] = df.y_true

    # impute all nulls as 1
    df['y_true_all1'] = np.where( (pd.isnull(df.y_true)), 1, df.y_true)
    
    # impute all nulls as 0
    df['y_true_all0'] = np.where( (pd.isnull(df.y_true)), 0, df.y_true)
    
    return df



# Metrics functions
def precision(df, true_label):
    """
    This function calculates the precision for the top k including null observations in test. 

    Parameters
        scores: list of scores
        y_true: list of true labels
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        class_type: type of threshold, probability (threshold_p) or top k (threshold_k)

    Returns:
        Precision score
    """    

    # Top k entities, NAs removed
    df = df[ (df.y_pred == 1) ].dropna(thresh = 1)
    
    # precision: relevant items recommended at k / recommended items at k
    relev_recomm_items = sum( (df[true_label] == 1) & (df['y_pred'] == 1) )
    recomm_items = sum( (df['y_pred'] == 1) & ( pd.isnull(df['y_true_omit']) == False ) )
    
    if(recomm_items == 0):
        return -1 # something is wrong
    else:
        return relev_recomm_items/recomm_items # tp/(tp + fp)



def recall(df, true_label):
    """
    This function calculates the recall of the k entities investigate.
    Also accepts no cut-off and then requieres a threshold, default .5.

    Parameters
        scores: list of scores
        y_true: list of true labels
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        class_type: type of threshold, probability (threshold_p) or top k (threshold_k)

    Returns:
        Recall score
    """ 

    # recall: recommended items relevant at k / relevant items total
    # tp/(tp + fn)
    relev_recomm_items = sum( (df['y_true_omit'] == 1) & (df['y_pred'] == 1) ) # tp
    relevant_items = sum( (df['y_true_omit'] == 1) ) # (tp + fn)
    
    # recall calculated
    if(relevant_items == 0):
        recall = -1
    elif(relevant_items != 0):
        recall = relev_recomm_items/relevant_items # tp/(tp + fn)
    
    return recall


def fallout(df, true_label):
    """
    This function calculates the recall at zero of the k entities investigate.
    Also accepts no cut-off and then requieres a threshold, default .5.

    Parameters
        y_pred: list of scores
        y_true: list of true labels
        
    Returns:
        Recall of 0's or fallout
    """
    # fallout: non relevant items recommended at k / non relevant items total 
    # fp/(fp + tn)
    nonrelev_recomm_items = sum( ((df['y_true_omit'] == 0) & (df['y_pred'] == 1)) ) # fp
    nonrelev_items = sum( (df['y_true_omit'] == 0) ) # (fp + tn)
    
    # fallout calculated
    if(nonrelev_items == 0):
        fallout = -1
    elif(nonrelev_items != 0):
        fallout = nonrelev_recomm_items/nonrelev_items # fp/(fp + tn)
    
    return fallout



def number_labels(df):
    """
    This function calculates the number of labels at selected k entities.

    Parameters
        scores: list of scores
        y_true: list of true labels
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        class_type: type of threshold, probability (threshold_p) or top k (threshold_k)
        
    Returns:
        Number of true labels at k
    """
    number_labels = sum(((df.y_true_omit == 0) | (df.y_true_omit == 1)) & df.y_pred == 1)
    
    return number_labels


def number_nulls(df, true_label):
    """
    This function calculates the number of true labels at selected k entities.

    Parameters
        scores: list of scores
        y_true: list of true labels
        threshold: cutoff k or minimum probability to label 0-1 predictions 
        class_type: type of threshold, probability (threshold_p) or top k (threshold_k)
        
    Returns:
        Number of true labels at k
    """
    # number of labels equal 1 at top k
    number_labels = sum( (pd.isnull(df[true_label])) )
    
    return number_labels



def auc(df):
    """
    This function calculates the AUC. It requieres a threshold, default .5.

    Parameters
        scores: list of scores
        y_true: list of true labels
        
    Returns:
        AUC score
    """
    # tp and tn given threshold
    fpr, tpr, thresholds = metrics.roc_curve(df['y_true'], df['scores'])
    auc = metrics.auc(fpr, tpr) 
    
    return auc


def brier_score(scores, y_true):
    """
    This function calculates Brier Score. 

    Parameters
        scores: list of scores
        y_true: list of true labels
        
    Returns:
        AUC score
    """
    # tp and tn given threshold
    brier = metrics.brier_score_loss(df['y_true'], df['scores'])
    
    return brier





def all_metrics_evaluations(scores, y_true, threshold, 
        abs_cutoffs = [],
        subset = 'all_data'):
    """
    This function returns the metrics dictionary for several cutoffs_
        -- precision
        -- recall
    
    Inputs:
    scores -- list of predicted probabilities
    y_true -- actual labels
    threshold -- % at cutoff if num_k is None

    Output:
    Dictionary of precision and recall scores
    """
    
    # Cutoff is higher than actual observations
    if(threshold > len(scores)):
        print("Cut off set at {}, only {}".format(threshold, len(scores)))
        threshold = len(scores)

    # percentage range 
    pct_rng = list(np.arange(.01, 1.01, 0.01))    

    # dictionary of types of cutoff
    cuts = { 'pct': pct_rng, 'abs': abs_cutoffs}

    # dictionary of metrics
    all_metrics = dict()


    for x_type, x_values in cuts.items():

        print("Calculating metrics by {} cutoff".format(x_type))
        
        for x_value in x_values:
            
            
            # All cases
            # cutoff to number
            cutoff = generate_cutoff_at_k(len(scores), x_value, x_type)
            # ordered data frame and cutoff
            df_all = ordered_df_atk(scores, y_true, cutoff)   
            
            # Omit case
            # cutoff to number
            cutoff_om = generate_cutoff_at_k(len(y_true.dropna()), x_value, x_type)
            # ordered data frame and cutoff
            df_omit = ordered_df_atk(scores, y_true, cutoff_om, 
                    drop_na = True)
            
            # Labels of true value manipulated
            for label in ['omit', 'all']:
                
                if label == 'omit':
                    df = df_omit
                elif label == 'all':
                    df = df_all

                # Precision
                all_metrics["precision|{}|{}|{}|{}".format(label,
                    str(x_value), x_type, subset)] = precision(df, 'y_true_omit')
                    
                # Recall
                all_metrics["recall|{}|{}|{}|{}".format(label, str(x_value),
                    x_type, subset)] = recall(df, label)

                # Fallout
                all_metrics["fallout|{}|{}|{}|{}".format(label,
                    str(x_value), x_type, subset)] = fallout(df, label)

            # Number labels: only need this once per cutoff
            all_metrics["number_labels||{}|{}|{}".format(
                str(x_value), x_type, subset)] = number_labels(df)
            
    return all_metrics
