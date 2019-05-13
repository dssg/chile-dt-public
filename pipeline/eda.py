"""
Functions for use in exploratory data analysis
"""

import pipeline.sql as plsql

# TODO: more descriptive stats
# TODO: function to run a bunch of descriptive stats for a table

"""
Runs SQL query to get total number of rows in a table
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table
    Does: prints the numbers of rows in the table
    Returns: data frame that contains number of rows in table
"""
def total_rows(engine, role, schema, table):
    qry = """set role {};
        select count(*) from {}.{};
        """
    result = plsql.query(qry.format(role, schema, table), engine)
    return result


"""
Runs SQL query to get total number of rows in a table by group
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table,
        string name of column to group by
    Does: prints the numbers of rows in the table by group
    Returns: data frame that contains number of rows in table by group
"""
def rows_by_group(engine, role, schema, table, group):
    qry = """set role {};
        select {}, count(*) from {}.{}
        group by {}
        order by {};
        """
    result = plsql.query(qry.format(role, group, 
        schema, table, group, group), engine)
    return result


"""
Runs SQL query to get total number of unique values in one column
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table,
        string name of column
    Does: prints the numbers of unique values for one column
    Returns: data frame that contains number of unique values for one col
"""
def count_distinct(engine, role, schema, table, column):
    qry = """set role {};
        select count(distinct {}) from {}.{};
        """
    result = plsql.query(qry.format(role, column, schema, table), 
            engine)
    return result


"""
Runs SQL query to get total number of NAs in a column
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table,
        string name of column,
        string value of NA column (defaults to NULL),
        indicator if NA value is an int (assumes string otherwise)
    Does: prints the numbers of NAs in the column
    Returns: data frame that contains number of NAs in col
"""
def count_nulls(engine, role, schema, table, column, 
        na = '__null__', ind = False):

    qry = """set role {};
        select sum(case when {} {} then 1 else 0 end) from {}.{};
        """

    ##### Create NA condition for query
    # If NA value is NULL, do not pass any value for "na" parameter
    # (this is the default case)
    if na == '__null__':
        na_qry = 'is null'
    # If NA value is something other than NULL, pass value 
    # with an indicator for whether this value should be treated as a string
    else:
        if ind == False:
            na = "'{}'".format(na)
        na_qry = '= ' + na

    result = plsql.query(qry.format(role, column, na_qry, 
        schema, table), engine)
    return result


"""
Runs SQL query to get proportion of null values in one column
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table,
        string name of column,
        na, ind from count_nulls function
    Does: prints the numbers of unique values for one column
    Returns: data frame that contains number of unique values for one col
"""
def proportion_nulls(engine, role, schema, table, column, na = '__null__', ind = False):
    nulls = count_nulls(engine, role, schema, table, column, na, ind).values
    length = total_rows(engine, role, schema, table).values
    result = float(nulls/length)
    return result



"""
Runs SQL query to get proportion and number of null values in all columns
    Given: sql engine,
        role in database,
        string name of schema,
        string name of table, 
        na, ind from count_nulls function
    Does: prints the numbers of unique values for one column
    Returns: data frame that contains number of unique values for one col
"""
def proportion_nulls_all_columns(engine, role, schema, table, na = '__null__', ind = False):
    props = []
    counts = []
    qry = """
        set role {};
        select column_name 
        from information_schema.columns 
        where table_name = '{}' 
        and table_schema = '{}';""".format(role, table, schema)
    result = plsql.query(qry, engine)

    for i, row in result.iterrows():
        nulls = count_nulls(engine, role, schema, table, row.column_name, na, ind).values
        length = total_rows(engine, role, schema, table).values
        counts.append(float(nulls))
        props.append(float(nulls/length))

    result['count'] = counts
    result['proportion'] = props

    return result

