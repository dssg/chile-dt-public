
import pipeline.sql as plsql
import pipeline.eda as pleda

import pandas as pd

from plotnine import *
from mizani.breaks import date_breaks
from mizani.formatters import date_format
from datetime import date

theme_set(theme_bw())

def description_colname(engine, role, schema, table, colname):
    """
    This function prints the number of nulls by column and returns 
    a dataframe with the counts per label
    
    Parameters: 
    - engine: for sql connection
    - role: role for query
    - schema: table schema
    - table: table name
    - colname: colname
    
    Return:
    Table with frequencies per label of selected columns
    """
    
    print(colname)
    print('\nNulls:')
    print(pleda.count_nulls(engine, role, schema, table, colname))
    tab = pleda.rows_by_group(engine, role, schema, table, colname)
    print('\nDescription:')
    print(tab[colname].describe())
    print('\n')
    return tab


def custom_date_format2(breaks):
    """
    Function to format the date by first day of the year 
        
    Parameters: 
    - breaks: time serie
    
    Return:
    List of dates with new format
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
    Function to format the date by month
        
    Parameters: 
    - breaks: time serie
    
    Return:
    List of dates with new format
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
    
    Parameters: 
    - width: breaks of date
    
    Return:
    Function with new limits
    """
    def filter_func(limits):
        breaks = date_breaks(width)(limits)
        # filter
        return [x for x in breaks if x.month % 2]

    return filter_func