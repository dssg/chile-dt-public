


"""
Functions to create data story for a specific company
- General information
- Graphs in time
- Matters inspected
"""
import sys
sys.path.append('../')

import pipeline.sql as plsql

# Import libraries
import pandas as pd
import numpy as np

from plotnine import *
from mizani.breaks import date_breaks
from mizani.formatters import date_format
from datetime import date
from wordcloud import WordCloud, STOPWORDS
from sklearn.feature_extraction.text import CountVectorizer

import matplotlib as mpl
import matplotlib.pyplot as plt

# Set theme for plotnine
theme_set(theme_bw())
stopwords = set(STOPWORDS)






def facility_info(engine, id_company):
    """
    Function to print company's general information
        engine: query
        id_company: rutempresamask
    """
    qry = """set role direccion_trabajo_inspections_write;
        select *
        from raw.inspections_complete
        where rutempresamask = {};""".format("'" + id_company + "'")
    tab_emp = plsql.query(qry, engine)
    
    total = tab_emp.idfiscalizacion.count()
    print('Total inspections:', tab_emp.idfiscalizacion.count())
    print('Total inspections with violations:', tab_emp.infractor.sum())
    print('% inspections with violations:', round(100*tab_emp.infractor.sum()/total) )
    
    
    print("\n")
    print('Total proactive inspections:', sum(tab_emp["solicitante"] == 'Por Programa'))
    print('Total reactive inspections:', sum(tab_emp["solicitante"] != 'Por Programa'))
    print('% proactive inspections:', round(100*sum(tab_emp["solicitante"] == 'Por Programa')/total) )
    
    print("\n")
    print('Total urgent inspections:', tab_emp.urgencia.sum())
    print('% urgent inspections:', round(100*tab_emp.urgencia.sum()/total) )
    
    print("\n")
    print('Total matters inspected:', tab_emp.num_materias.sum().astype('int'))
    print('Total matters infractioned:', tab_emp.infra.sum().astype('int'))
    print('% matters infractioned:', round(100*tab_emp.infra.sum().astype('int')/tab_emp.num_materias.sum().astype('int')) )
    

    
def economicactivity_info(engine, id_company):
    """
    Function to print economic activity from taxes and inpsections tables
        engine: query
        id_company: rutempresamask
    """
    qry = """set role direccion_trabajo_inspections_write;
        select gcae, ccae, count(*)
        from raw.inspections_complete
        where rutempresamask = '{}'
        group by gcae, ccae
        order by ccae;""".format(id_company)
    tab_ins = plsql.query(qry, engine)
    print('Economic activity from Inpsections data: \n')
    print(tab_ins)
    print('\n')

    qry = """set role direccion_trabajo_inspections_write;
        select actividadeconomica, count(*)
        from raw.taxes
        where rutmask = '{}'
        group by actividadeconomica
        order by count desc;""".format(id_company)
    tab_tax = plsql.query(qry, engine)
    print('Economic activity from Taxes data:\n')
    print(tab_tax)
    
    
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


def time_graphs(engine, id_company):

    """
    Function to create inspections, type of inspections and infractions over time
        engine: to query
        id_company: rutempresamask
    """

    #  Query with new variables
    qry = """set role direccion_trabajo_inspections_write;
        select rutempresamask,  
        date(agno || '-' || mesreg || '-01') as date,    
        count(rutempresamask) as inspections, 
        sum(infra)  as infractions,
        sum(num_materias) as matters,
        sum(case when solicitante = 'Por Programa' then 1 else 0 end) as proactive,
        sum(case when solicitante = 'Por Programa' then 0 else 1 end) as reactive
        from raw.inspections_complete
        where rutempresamask = {} 
        group by rutempresamask, date
        order by date;""".format("'" + id_company + "'")

    tab_summ = pd.read_sql_query(qry, engine)

    # Date format for graph
    tab_summ['date'] = pd.to_datetime(tab_summ.date)

    # New features
    tab_summ['infractions'] = tab_summ['infractions'].astype('int')
    tab_summ['matters'] = tab_summ['matters'].astype('int')
    tab_summ['prop_raw'] = tab_summ['infractions']/tab_summ['matters']
    tab_summ['prop'] = round(100*tab_summ['infractions']/tab_summ['matters'])
    tab_summ['prop'] = tab_summ['prop'].astype('int')
    tab_summ['prop'] = tab_summ['prop'].map(str) + "%"
    

    # Number of inspections
    gg1 = (ggplot(tab_summ, aes ( x = 'date', y = 'inspections')) +
        geom_hline(yintercept = 0, color = 'gray') +
        geom_bar(stat = 'identity', fill = 'purple', alpha = .5) +
        geom_text(aes(y = 'inspections + 1', label = 'inspections'),
                  color = 'black', size = 10) +
        ylab('Count') +
        xlab('Month of inspection')  +
        scale_x_datetime(
             breaks=date_breaks('1 months'),
             labels=custom_date_format2) +
        theme(axis_text_x = element_text(angle = 90, size= 8),
             figure_size = (15, 3)))

    # Number of inspections by type of inspections
    tab_gg = pd.melt(tab_summ[['rutempresamask', "date", "proactive","reactive", "inspections"]],
            id_vars = ['rutempresamask', 'date', 'inspections'])

    gg2 = (ggplot(tab_gg, aes ( x = 'date', y = 'value', fill = 'variable')) +
        geom_hline(yintercept = 0, color = 'gray') +
        geom_bar(stat = 'identity', alpha = .5) +
        #geom_text(aes(y = 'inspections + 1', label = 'inspections'),
        #          color = 'black', size = 10) +
        ylab('Count') +
        xlab('Month of inspection')  +
        scale_x_datetime(
             breaks=date_breaks('1 months'),
             labels=custom_date_format2) +
        theme(axis_text_x = element_text(angle = 90, size= 8),
             figure_size = (15, 3)))

    # Matters inspected vs matters with infractions
    gg3 = (ggplot(tab_summ, aes ( x = 'date', y = -1)) +
        geom_hline(yintercept = -1, color = 'gray') +
        geom_linerange(aes(ymin =0, ymax = 'matters'),
                color = 'blue') +
        geom_linerange(aes(ymin =0, ymax = 'infractions'),
                color = 'red') +
        geom_point(aes(y = 'infractions'),
                color = 'red', size = 4,
                   alpha = .3, shape  = 4) +
        geom_text(aes(y = 'matters + 6', label = 'prop'),
                size = 4, color = 'gray') +
        ylab('Count') +
        xlab('Month of infractions') +
        scale_x_datetime(
             breaks=date_breaks('1 months'),
             labels=custom_date_format2) +
        theme(axis_text_x = element_text(angle = 90, size= 8),
             figure_size = (15, 3)))

    return gg1, gg2, gg3


def first_row(x):
    
    """
    Function to keep the first row of a data frame
        x: data frame
    """
    
    return x.iloc[0]




def freq_matters_bookupdated(engine, role, id_company):
    
    """
    Function to create frequency table of matters infracted
        engine: to query
        role: role for query
        id_company: rutempresamask
    """
    
    qry = """set role {};
    select case when trim(matter_code) = '-' then '' else trim(matter_code) end as matter, count(*)
    from cleaned.infracted_matters_updatedbook
    where rutempresamask = {}
    group by matter
    order by matter;""".format(role, "'" + id_company + "'")
    tab = plsql.query(qry, engine)

    qry = """set role {};
        select case when trim(codigo) = '-' then '' else trim(codigo) end as matter, 
        trim(lower(regexp_replace(statement , '\.', ''))) as state,
        count(*)
        from raw.tipificador
        group by matter, state;""".format(role)
    tab_tipi = plsql.query(qry, engine)
    tab_join = tab.merge(tab_tipi, left_on='matter', right_on='matter', how='inner')
    tab_result = tab_join.groupby('matter').apply(first_row).sort_values('count_x', ascending = False)
    
    return tab_result


def text_wordcloud_matters(tab_result, count_frame = 1):
    """
    Function to create frequency table of matters infracted
        tab_result: frequency table by matters
        count_frame: count limit
    """
    
    text = ' '.join(tab_result.state[(tab_result['count_x']> count_frame)])
    
    strip = CountVectorizer().build_tokenizer()(text)
    strip2 = [w for w in strip if not w.isdigit()]
    filtered = [w for w in strip2 if not w in stopwords]
    text = ' '.join(filtered) 

    # Display the generated image:
    # the matplotlib way:
    wordcloud = WordCloud(background_color = 'white').generate(text)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    
    return text