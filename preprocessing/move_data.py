import pymssql
import pandas as pd
import yaml
import sys
sys.path.append('../')
import pipeline.sql as plsql

def main():
    with open('sql_server.yaml', 'r') as f:
        config_mssql = yaml.load(f)
    engine_mssql = pymssql.connect(server=config_mssql['db']['host'],
            user=config_mssql['db']['username'],
            password=config_mssql['db']['password'],
            database=config_mssql['db']['database'])
    engine_psql = plsql.create_engine('config.yaml')
    print("about to query")
    df = pd.read_sql("select * from DT_mul_CapituloNormasan;", engine_mssql)
    print(df.head())
    df.columns = df.columns.str.lower()
    plsql.query_no_return("""set role direccion_trabajo_inspections_write;
    create table staging.fake as select 1;
    drop table staging.fake;""", engine_psql)
    print(len(df))
    start = 0
    table = 'dt_mul_capitulonormasan'
    for i in range(100, len(df), 100):
        df.iloc[start:i, :].to_sql(table, engine_psql, schema = 'raw', if_exists = 'append')
        start = i
        print(start)
    remaining = len(df) - start
    df.tail(remaining).to_sql(table, engine_psql, schema = 'raw', if_exists = 'append')
    print(len(df))
    print("done")


main()
