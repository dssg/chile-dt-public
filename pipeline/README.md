# Pipeline 

These files contain functions intended for generalized use across this project and other future data science projects.

## `sql.py`
Functions for very general use of SQL scripts within Python (using `pandas` and `psycopg2`).  
Recommended usage for setting up SQL in Python:

```{python}
import pipeline.sql as plsql
plsql.create_engine('example.yaml')
plsql.query('select * from schema.table;', engine)
```

* `read_yaml(filename)`: Reads in `.yaml` file and returns a dictionary.
* `create_engine(filename)`: Reads in a `.yaml` file and then creates a `sqlalchemy` engine. Returns this engine.
* `query(query, engine)`: Takes a query and an engine, performs the query, and returns the result of the query. Note that currently this is equivalent to `pandas.read_to_sql(query, engine)`.
* `query_no_return(query, engine)`: This is similar to `query`, but does not require a table to be returned. Should be used for writing to tables.
* `append_table(pandas_dataframe, schema, table, engine)`: Append the contents of a pandas data frame to an existing table

## `preprocessing.py`

This file contains functions for processing data before it reaches its (mostly) final, cleaned state. 

Some of the functions are for converting raw flat files into a format that's movable into PostgreSQL:
* `xlsx_to_csv(old_filename, new_filename, sep = ',')`: Converts an XSLX file to CSV
* `header_to_lower(filename)`: Converts the header of a CSV file to all lowercase
* `replace_char(filename, old_string, new_string)`: Replaces all instances of the old string with the new string. Use regular expressions.
* `remove_first_column(filename, sep = ',')`: Removes the first column of a CSV (typically an index)

And a function for transforming tables:
* `create_long_table(engine, role, old_schema, new_schema, old_table, new_table, array_col, selected cols, new_col, sep)`: Performs a SQL query to create a new "long" table from a "wide" table with a column that is a list of values. See code for more detailed description of parameters.

## `eda.py`

This file contains functions for descriptive statistics and other more exploratory data analysis.

* `total_rows(engine, role, schema, table)`: Performs a SQL query to get the total number of rows in a given table.
* `rows_by_group(engine, role, schema, table, group)`: Performs a SQL query to get the number of rows in a given table, grouped by a given column.
* `count_distinct(engine, role, schema, table, column)`: Performs a SQL query to get the number of distinct values of a given column in a given table.
* `count_nulls(engine, role, schema, table, column, na = '__null__', ind = 'false')`: Performs a SQL query to get the total number of NAs in a given column in a given table. The `na` parameter default assumes the NA value is an actual null value. Pass in a non-default value if the NA value is something other than a null, such as `99` or `.`, passed in as a string. If this value is an integer, such as in the case of `99`, change the `ind` parameter to `'true'`.
* `proportion_nulls(engine, role, schema, table, column, na = '__null__', ind = False)`: The same as `count_null`, but returns a proportion rather than a count
* `proportion_nulls_all_columns(engine, role, schema, na = '__null__', ind = False)`: Calls both `count_nulls` and `proportion_nulls` on each column of a given table. Returns a table with two columns: `count` and `proportion`.

## `imputation.py`

This file contains functions for imputation in pandas using various methods.

* `simple_imputation(df, features, method='mean')`: Imputes specified features within a given data frame with simple methods. This includes mean, mode, max, min, median, zeros, and ones.

* `complex_imputation(df, method='mice', neighbors = 3)`: Performs matrix completion imputation for an incomplete matrix using more complex methods than simple imputation. This includes K Nearest Neighbors, Soft Impute, Multiple Imputation by Chained Equations, Nuclear Norm Minimization, Matrix Factorization, and Iterative Singular Value Decomposition. 

## `results.py`

This file contains functions for writing to the results schema.

* `hash_id(list)`: Creates a unique hash based on a list of values
* `hash_model_id(model_type, model_params, features, config, train_end_time)`: Creates a unique hash for a given model
* `hash_matrix_id(features, train_start_time, train_end_time, config)`: Creates a unique hash for a given model matrix
* `check_model_row(engine, role, model_group_id, model_hash)`: Checks whether a model exists, returns model ID
* `write_model_row(engine, role, model_id, model_group_id, run_time_start, run_time_end, batch_run_time, model_type, model_params, config, test, model_hash, train_matrix_uuid, train_end_time, model_comment = None, batch_commend = None)`: Writes the given model to `results.models`
* `write_predictions(engine, role, model_id, as_of_date, entity_id, y_scores, y_true, matrix_uuid)`: Writes predictions to `results.predictions`
* `check_model_group(engine, role, model_type, model_params, features, label, config)`: Checks whether the given model group already exists
* `get_feature_imps(model)`: Given a trained model, get feature importances
* `get_scores(model, test_data)`: Given a trained model, get risk scores
* `write_feature_imps(engine, model_id, features, feature_importances)`: Writes feature importances to `results.feature_importances`
* `write_evaluations(engine, model_id, metrics, values, eval_start_time, eval_end_time, pred_freq, comment = None)`: Writes metrics with values to `results.evaluations`
* `write_results(engine, role, config, model, model_id, model_group_id, model_hash, model_type, start, end, features, labels, test_X, test_X_not_imputed, test_y, test_data, run_time, end_run_time, batch_run_time, y_score, model_params, pred_freq = '1 month', output_dir = './')`: Writes all results to results schema

## `metrics.py`

This file contains functions for calculating metrics. In particular, our metrics are all at a threshold _k_ due to the nature of our problem.

* `generate_cutoff_at_k(len_scores, x_value, unit = 'abs')`: Given a cutoff value or percentage, returns the number of facilities above the cutoff to mark as recommended for inspection
* `binary_cutoff(scores, threshold)`: Given a _k_ cutoff, produces a list of predictions 
* `ordered_df_atk(scores, y_true, threshold, drop_na = False)`: Creates a data frame ordered by scores with several new columns of predicted labels with different "imputation" methods: no change (`y_true_omit`), fill with 1s (`y_true_all1`), fill with 0s (`y_true_all1`)
* `precision(df, true_label)`: Calculates precision 
* `recall(df, true_label)`: Calculates recall
* `fallout(df, true_label)`: Calculates fallout, aka recall on 0's
* `number_labels(df)`: Calculates the number of labels that appear above the cutoff
* `number_nulls(df, true_label)`: Calculates the number of null labels that appear above the cutoff
* `auc(df)`: Calculates the AUC
* `brier_score(scores, y_true)`: Calculates the Brier scores
* `all_metrics_evaluations(scores, y_true, threshold, abs_cutoffs = [], subset = 'all_data')`: Creates a dictionary of all metrics for a variety of cutoffs
