
### Functions for model graphic evaluation and comparison

import sys
sys.path.append('../')

import pandas as pd
import numpy as np

import pipeline.sql as plsql

import matplotlib
matplotlib.use('Agg')

from plotnine import *

from mizani.breaks import date_breaks
from mizani.formatters import date_format
from datetime import date


theme_set(theme_bw())


"""
- Results per model id: Some of these functions go in the pipeline
    model_id_score_gghist
    model_id_precsion_recall
    model_id_feature_importance
    model_id_graphs: calls the previous functions, meant to be in the pipeline

- Comparing model group ids:
    group_ids_view_metrics

- Comparing model ids:

"""

def custom_date_format2(breaks):
    """
    Function to format the date
    """
    res = []
    for x in breaks:
        # First day of the year
        if x.month == 1 and x.day == 1:
            fmt = '%Y'
        # Every other month
        elif x.month % 2 != 0:
            fmt = '%b'
        else:
            fmt = ''

        res.append(date.strftime(x, fmt))

    return res


def custom_date_format3(breaks):
    """
    Function to format the date
    """
    res = []
    for x in breaks:
        # First day of the year
        if x.month == 1:
            fmt = '%Y'
        else:
            fmt = '%b'

        res.append(date.strftime(x, fmt))

    return res


def custom_date_breaks(width=None):
    """
    Create a function that calculates date breaks
    It delegates the work to `date_breaks`
    """
    def filter_func(limits):
        breaks = date_breaks(width)(limits)
        # filter
        return [x for x in breaks if x.month % 2]

    return filter_func





"""
These functions create several results per model id
"""    
def model_id_score_gghist(model_id, model_group_id, model_type, role, engine):
    """
    This function creates de scores histogram
    
    Parameters:
    model_id = model id to plot
    group_model_id
    role
    engine    
    
    Return
    histogram
    """
    qry = """set role {}; 
            select * 
            from results.predictions 
            where model_id = {};
            """.format(role, model_id)
    
    df = plsql.query(engine=engine, qry=qry)

    model_type = model_type
    model_id = model_id
    month_start = df.as_of_date[0]
    
    
    gg_hist_label = (ggplot(df, aes(x = 'score')) + 
            geom_histogram(bins = 25) + 
            ggtitle('{model_type}; model_id = {model_id},\n{month_start} '.format(model_type=model_type, 
                                                                                model_id=model_id,
                                                                                month_start= month_start)) + 
            facet_wrap('~label_value', scales = 'free_y') + 
            theme(figure_size = (5, 3))  )
    
    gg_hist_all = (ggplot(df, aes(x = 'score')) + 
            geom_histogram(bins = 25) + 
            ggtitle('{model_type}; model_id = {model_id},\n{month_start} '.format(model_type=model_type, 
                                                                                model_id=model_id,
                                                                                month_start= month_start)) + 
            theme(figure_size = (4, 3))  )
    
    
    return gg_hist_all, gg_hist_label




def model_id_precsion_recall(model_id, 
                             role, engine):
    qry = """set role {}; 
            select * from
                (select model_id, model_type,
                    value, evaluation_start_time, 
                    evaluation_end_time, prediction_frequency,
                    split_part(metric, '|', 1) AS metric_name,
                    split_part(metric, '|', 2) AS null_param,
                    split_part(metric, '|', 3) AS cutoff,
                    split_part(metric, '|', 4) AS type,
                    split_part(metric, '|', 5) AS subset
                from results.evaluations a
                left join 
                    (select model_id, model_group_id, model_type
                    from results.models) b
                using (model_id) 
                where a.model_id = {}) s
            where s.subset = 'all_data' and
                  s.type = 'pct';
                """.format(role, model_id)

    df = plsql.query(engine=engine, qry=qry)
    tab = (df[df['metric_name'].isin(['precision', 'recall', 'fallout'])]).reset_index()
    tab.cutoff = tab.cutoff.astype('float')
    
    
    gg_precrecall = (ggplot(tab,
                    aes(x = 'cutoff', y = 'value', 
                    color = 'metric_name', 
                    group = 'metric_name')) + 
                 geom_line(size = 1) + 
                 xlab('Population (Prop.)') +
                 ylab('Value') +     
                 facet_wrap('~null_param') + 
                 ggtitle('{model_type}; model_id = {model_id},\n{month_start} '.format(model_type = df.model_type[0], 
                                                                                    model_id = df.model_id[0],
                                                                                    month_start= df.evaluation_start_time[0])) +
                 theme(figure_size = (7, 3)) )
    
    return gg_precrecall


    
def model_id_feature_importance(model_id, role, engine):
    
    qry = """set role {}; 
            select * 
            from results.feature_importances  a
            left join 
                (select model_id, model_group_id, model_type
                from results.models) b
            using (model_id)
            where a.model_id = {};
            """.format(role, model_id)
    
    df = plsql.query(engine=engine, qry=qry)
    
    df_sorted = df.sort_values(by = 'feature_importance', ascending = True)
    df_sorted = df_sorted.nlargest(20, 'feature_importance')
    
    df_sorted['feature_clean'] = df_sorted.feature.str.replace("_", " ").str.title()

    ## order the features
    order_features = list(df_sorted.feature_clean)

    ## create categorical version for graph
    df_sorted['feature_categorical'] = df_sorted.feature_clean.astype('category', 
                                    ordered = True,
                                    categories = order_features)
    
    gg_importance = (ggplot(df_sorted, aes(x = 'feature_categorical',
                          y = 'feature_importance')) +
         geom_bar(stat = 'identity', fill = '#1f497d', 
                  alpha = 0.2, color = 'black')  +
         xlab('Feature') +
         ylab('Importance') +
         ggtitle('{model_type}; model_id = {model_id} '.format(model_type=df.model_type[0], 
                                                               model_id=df.model_id[0])) +
         theme(axis_text_x = element_text(size = 7, angle = 90), 
               figure_size = (7, 3)) )
           
    return gg_importance


def model_id_graphs(model_id, role, engine, path):
    """
    Running and save graphs for every model
    """
    print('Creating graphs.......')
    
    qry = """set role {}; 
            select *
            from results.models
            where model_id = {};
    """.format(role, model_id)
    tab_name = plsql.query(qry, engine)
    
    model_group_id = tab_name.model_group_id[0]
    model_type = tab_name.model_type[0]
    
    fig_name = ("group_{group_model_id}_modelid_{model_id}_{model_type}.png".format(
        group_model_id = tab_name.model_group_id[0], 
        model_id = model_id, 
        model_type = tab_name.model_type[0])).lower()
    fig_path = path
    
    
    gg_hist_all, gg_hist_lab = model_id_score_gghist(model_id, 
                                                     model_group_id, 
                                                     model_type, 
                                                     role, engine)
    gg_hist_all.save(filename = fig_name , 
                     path = fig_path + 'histogram_all/')
    gg_hist_lab.save(filename = fig_name , 
                     path = fig_path + 'histogram_label/')

    
    gg_prec = model_id_precsion_recall(model_id, role, engine)
    gg_prec.save(filename = fig_name , 
                 path = fig_path + 'precision/')
    
    gg_impo = model_id_feature_importance(model_id, role, engine)
    gg_impo.save(filename = fig_name , 
                 path = fig_path + 'features/')
    
        
        
"""
These functions compare results between several model group ids
"""    

# Metrics
def group_ids_view_metrics(model_group_ids, role, engine):
    qry_view = """
        SET ROLE {};
        DROP MATERIALIZED VIEW IF EXISTS results.view_modelgroup;
        CREATE MATERIALIZED VIEW IF NOT EXISTS results.view_modelgroup
                AS
            SELECT 
                model_id, model_group_id, model_type,
                value, 
                evaluation_start_time, 
                evaluation_end_time, 
                split_part(metric, '|', 1) AS metric_name,
                split_part(metric, '|', 2) AS null_par,
                split_part(metric, '|', 3) AS cutoff,
                split_part(metric, '|', 4) AS type,
                split_part(metric, '|', 5) AS subset
            FROM results.evaluations a
            LEFT JOIN
                (SELECT model_id, model_group_id, model_type
                FROM results.models) b
            USING (model_id)
            WHERE b.model_group_id in ({model_group_ids}) and 
                value >= 0;
      """.format(role, 
                 model_group_ids = ', '.join(model_group_ids))
    plsql.query_no_return(qry_view, engine)


# Group id average and precision scatter plot    
def groupid_metricat_eval_time(metric_name, cutoff, 
                               role, engine): 
    
    qry = """set role {}; 
            select *,
                cast(model_group_id as varchar) as model_group_id_char,
                case when null_par = 'omit' then 'labeled' 
                    else 'all' end as null_label
            from results.view_modelgroup
            where 
                subset = 'all_data' and 
                cutoff = '{cutoff}' and 
                null_par = 'all' and
                metric_name = '{metric_name}' and
                type = 'abs';
            """.format(role, 
                       cutoff = cutoff, 
                       metric_name = metric_name)
    
    tab = pd.read_sql_query(qry, engine)

    gg = (ggplot(tab, 
            aes(x = 'evaluation_start_time', y = 'value',
               color = 'model_type',
               group = 'model_group_id')) + 
        geom_line() + 
        geom_point() + 
        theme(axis_text_x = element_text(angle = 90)) + 
        ggtitle("{} at {}".format(metric_name.title(), cutoff)) + 
        #facet_wrap('~cutoff') +
        scale_color_brewer('qual', name = 'Model type') + 
        scale_x_datetime(
             breaks=date_breaks('1 months')) + 
        #ylim(0,1) + 
        ylab(metric_name.title()) +
        xlab('Evaluation start time') +
        theme(figure_size = (5, 3)))
    
    return gg


# Group id average and standart precision vs recall scatter plot
def groupid_prerec_avgsd(cutoff_list, role, engine):
    qry = """SET ROLE {}; 
        SELECT metric_name, 
            model_type,
            cutoff,
            cast(model_group_id as varchar) as model_group_id_char,
            case when null_par = 'omit' then 'labeled' 
                        else 'all' end as null_label,
            AVG(value),
            STDDEV(value)
        FROM results.view_modelgroup
        WHERE 
            subset = 'all_data' AND 
            cutoff in ({cutoff_list}) AND
            type = 'abs' AND
            metric_name in ('precision', 'recall')
        GROUP BY model_group_id_char,  
            null_label,
            metric_name, 
            model_type,
            cutoff;
    """.format(role, 
               cutoff_list = "'" + "', '".join(cutoff_list) + "'")
    
    # run query
    tab = pd.read_sql_query(qry, engine)
    
    # just precision and recall
    tab_me = pd.melt(tab, 
            id_vars = ['metric_name', 'cutoff', 'model_type',
                       'model_group_id_char', 'null_label'],
            value_vars = ['avg', 'stdev'])
    
    # prepare for ggplot
    tab_gg = pd.pivot_table(tab_me,
                   index=['variable', 'cutoff', 'model_type', 
                          'model_group_id_char', 'null_label'],
                    columns = 'metric_name',
                   values="value").reset_index()

    gg = (ggplot(tab_gg, aes(x = 'recall', y = 'precision',
                       color = 'model_type') ) + 
         geom_smooth(method = 'lm', se = False,
                     color = 'gray', alpha = .6) + 
         geom_point() + 
        facet_grid('cutoff~variable') + #, scales = 'free') + 
        ggtitle('Precision - Recall') + 
        ylab('Precision') +
        xlab('Recall') +
        theme(figure_size = (7, 6)))
    
    return gg




# Comparison between model ids
def model_group_id_graphs(model_group_list, cutoff_list, role, engine):
    
    group_ids_view_metrics(model_group_list, role, engine)
    
    for cut in cutoff_list:
            gg_prec = groupid_metricat_eval_time('precision', cut, role, engine)
            print(gg_prec)
            
            gg_recl = groupid_metricat_eval_time('recall', cut, role, engine)
            print(gg_recl)
            
            gg_fall = groupid_metricat_eval_time('fallout', cut, role, engine)
            print(gg_fall)
    
    
    gg_comp = groupid_prerec_avgsd(cutoff_list, role, engine)
    print(gg_comp)
    
    
"""
These functions compare results between two model ids
"""    
# jaccard coefficient for two models full join
def compute_jaccard_index_full(label_1, label_2):
    
    # number of entities in both top's
    n = sum((label_1 - label_2) == 0)
    
    # number of entities total
    d = len(label_1)
    coef = n/d
    
    return coef


# Compare two models' scores with jaccard coefficient
def comp_modelid_jaccard_scores(model_id_a, model_id_b, top_k, role, engine):

    qry = """set role {}; 
        SELECT entity_id,
            case when score_b != 0 then 1 else 0 end as score_2,
            case when score_a != 0 then 1 else 0 end as score_1
        FROM 
            (
            SELECT entity_id, 1 as score_b
            FROM results.predictions 
            WHERE model_id = '{model_b}'
            ORDER BY score DESC
            LIMIT {top_k}
            ) b
        FULL JOIN
            (
            SELECT entity_id, 1 as score_a
            FROM results.predictions 
            WHERE model_id = '{model_a}'
            ORDER BY score DESC
            LIMIT {top_k}
            ) a
        USING (entity_id);
        """.format(role, 
                   model_a = model_id_a, 
                   model_b = model_id_b, 
                   top_k = top_k)
    df = pd.read_sql_query(qry, engine)
    coef = compute_jaccard_index_full( df['score_1'], df['score_2'])
    
    return coef
    
    