"""
Functions for making data nice and usable in our database
Includes:
    - Cleaning flat files for database
    - Moving flat files to database
    - Restructuring of files in database to be useful
"""

import pipeline.sql as plsql
import psycopg2
from subprocess import call


"""
Function to convert xlsx file to a csv
    Given: file name for xlsx, new file name, sep (defaults to comma)
    Does: creates filename.csv
"""
def xlsx_to_csv(filename, newfile, sep = ','):
    call("in2csv -d'{}' {} > {}".format(sep, filename, newfile), shell = True)
    print("\nConverted {} to CSV.".format(filename))


"""
Function to make CSV header with mixed capitalization all lowercase
    Given: filename
    Does: rewrites first line to be all lowercase
"""
def header_to_lower(filename):
    call("awk 'NR==1{{$0=tolower($0)}} 1' {} > {}".format(filename, filename + '.mod'), shell = True)
    call("mv {}.mod {}".format(filename, filename), shell = True)
    print("\nConverted header of {} to lowercase.".format(filename))


"""
Function to replace gross values throughout a file
    Given: filename, old char, new char
    Does: rewrites file with the replaced character
"""
def replace_char(filename, old, new):
    call("sed 's/{}/{}/g {} > {}".format(old, new, filename, filename + '_mod'), shell = True)
    call("mv {}_mod {}".format(filename, filename), shell = True)
    print("Replaced {} with {} in {}.".format(old, new, filename))


"""
Function to remove first column from csv
    Given: filename, sep (defaults to comma)
    Does: Rewrites file to not have that column
"""
def remove_first_column(filename, sep = ','):
    call("cut -d'{}' -f1 --complement {} > {}_mod".format(sep, filename, filename), shell = True)
    call("mv {}_mod {}".format(filename, filename), shell = True)
    print("Removed first column of {}.".format(filename))


"""
Runs SQL query to create a long table from a wide table
where there is a column containing a list of values
    Given: sql engine,
            role in database,
            string name of original schema,
            string name of original table,
            string name of new table,
            string name of column containing list of values,
            list of strings for desired columns selected,
            string name of new schema,
            string name for new column (defaults to 'new'),
            string for separator between values in array column (defaults to comma)
    Does: creates new table in long format instead of wide
"""
def create_long_table(engine, role, 
        old_schema, old_table, new_table, 
        array_col, selected_cols, new_schema,
        new_col='new', sep=','):

    # Base query
    qry = """set role {};
        create schema if not exists {};
        drop table if exists {}.{};
        create table {}.{} as
        select {},
        unnest(string_to_array({}, '{}'))
        as {}
        from {}.{};
        """.format(role, new_schema, 
        new_schema, new_table, new_schema, new_table,
        ', '.join(selected_cols), array_col, sep,
        new_col, old_schema, old_table)

    print("\nQuerying...\n")
    print(qry)

    # Sqlalchemy stuff
    conn = engine.connect()
    trans = conn.begin()

    # Actual query
    conn.execute(qry, engine)

    # More sqlalchemy stuff
    trans.commit()
    conn.close()
    print("\nQuery complete.\n")
