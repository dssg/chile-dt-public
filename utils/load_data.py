"""
Non-generalizable code for loading data into database
(Intended to be called from run.py as part of the pipeline)
"""

import sys
sys.path.append('../')
import pipeline.preprocessing as plpre
import pipeline.sql as plsql
from subprocess import call

"""
Function to make raw files usable
    Given: data dirs
    Does: creates usable csv files
"""
def clean_csv(data_dirs):
   
    # Service tax --> taxes.csv
    # TODO: does this actually work? probably not lol
    call("cp --no-preserve=mode {} {}taxes.csv".format(
        data_dirs['taxes'],
        data_dirs['output']), shell = True)
    data_dirs['taxes'] = data_dirs['output'] + 'taxes.csv'
    call("awk 'sub(/\./,"")+1' {} > {}_mod".format(filename, filename), shell = True)
    call("mv {}_mod {}".format(filename, filename), shell = True)
    plpre.replace_char(data_dirs['taxes'], ';;', ';\.;')

    # Inspections_se --> inspections_se.csv
    # TODO: test
    call("cp --no-preserve=mode {} {}inspections_se.csv".format(
        data_dirs['inspections'], data_dirs['output']), shell = True)
    data_dirs['inspections'] = data_dirs['output'] + 'inspections_se.csv'
    plpre.remove_first_column(data_dirs['inspections'], sep = ';')
    plpre.header_to_lower(data_dirs['inspections'])
    plpre.replace_char(data_dirs['inspections'], ';;', ';\.;')

    # TODO: Inspection offices --> offices.csv

    # TODO: Inspector data?

    # TODO: Comuna glosa

    # Copper and macroeconomic_monthly are file as the original files

    return data_dirs



"""
Function to load data into database
    Given: sqlalchemy engine
    Does: 
    loads raw.inspections_complete
        raw.social
        raw.taxes
        raw.inspections_se
        cleaned.violations, 
        cleaned.inspected_matters,
        cleaned.complaints
        cleaned.inspected_matters_updated_book
        cleaned.social
"""
def load_data(engine, data_dirs):

    data_dirs = clean_csv(data_dirs)

    # Load raw.taxes
    plsql.query_no_return(""" SET ROLE direccion_trabajo_inspections_write;

        CREATE SCHEMA IF NOT EXISTS raw;

        DROP TABLE IF EXISTS raw.taxes;

        CREATE TABLE raw.taxes (
            agno DECIMAL, 
            dv VARCHAR, 
            tramoventas VARCHAR, 
            ntrabajadores DECIMAL, 
            rubro VARCHAR, 
            subrubro VARCHAR, 
            actividadeconomica VARCHAR, 
            calle VARCHAR, 
            numero VARCHAR, 
            bloque VARCHAR, 
            depto VARCHAR, 
            villapoblacion VARCHAR, 
            comuna VARCHAR, 
            region VARCHAR, 
            fechainicio VARCHAR, 
            fechatermino VARCHAR, 
            tipotermino VARCHAR, 
            tipocontribuyente VARCHAR, 
            subtipocontribuyente VARCHAR, 
            f22c645 VARCHAR, 
            f22c646 VARCHAR, 
            rutmask VARCHAR, 
            razonsocialmask VARCHAR 
            );

        \COPY raw.taxes from '{}' WITH DELIMITER ';' NULL AS '.' CSV HEADER;
        """.format(data_dirs['taxes']), engine)

    # Load inspections + originally eliminated data
    plsql.query_no_return("""
        SET ROLE direccion_trabajo_inspections_write;

        CREATE SCHEMA IF NOT EXISTS raw;

        DROP TABLE IF EXISTS raw.inspections_se;

        CREATE TABLE raw.inspections_se (
            idfiscalizacion VARCHAR,
            codoficina VARCHAR,
            agno DECIMAL,
            nrocomision VARCHAR,
            totalafectados DECIMAL,
            urgencia BOOLEAN,
            solesafectado BOOLEAN,
            codtiposol VARCHAR,
            solicitante VARCHAR,
            codunidadorigen VARCHAR,
            unidadorigen VARCHAR,
            codtipotermino VARCHAR,
            tipotermino VARCHAR,
            egresoconmulta VARCHAR,
            empdfcodcomuna VARCHAR,
            empdmcodcomuna VARCHAR,
            emptrabhombres VARCHAR,
            codcae VARCHAR,
            codtipoempresa VARCHAR,
            grupocodtipomaterias VARCHAR,
            grupoglosatipomaterias VARCHAR,
            grupoglosainfra VARCHAR,
            grupocodigoinfra VARCHAR,
            grupoglosainfra_det VARCHAR,
            grupocodigoinfra_det VARCHAR,
            grupocodtipomaterias2 VARCHAR,
            grupoglosatipomaterias2 VARCHAR,
            grupoglosainfra2 VARCHAR,
            grupoglosainfra2_det VARCHAR,
            grupocodigoinfra2 VARCHAR,
            grupocodigoinfra2_det VARCHAR,
            grupocodigonormainfra2_det VARCHAR,
            ntrabajadoressii DECIMAL,
            tramoventassii DECIMAL,
            ccae VARCHAR,
            gcae VARCHAR,
            crae VARCHAR,
            grae VARCHAR,
            noinfra DECIMAL,
            infra DECIMAL,
            derechofund DECIMAL,
            num_materias DECIMAL,
            num_sind DECIMAL,
            region VARCHAR,
            infractor BOOLEAN,
            exsind BOOLEAN,
            datereg DATE,
            mesreg DECIMAL,
            inrangeofdates VARCHAR,
            rareruts VARCHAR,
            rutisna VARCHAR,
            rutempresamask VARCHAR
            );

        \COPY raw.inspections_se from '{}' WITH DELIMITER ';' NULL '.' CSV HEADER;
        """.format(data_dirs['inspections_se']), engine)

    # Load raw.copper
    plsql.query_no_return("""
    SET ROLE direccion_trabajo_inspections_write;

    CREATE SCHEMA IF NOT EXISTS raw;

    DROP TABLE IF EXISTS raw.copper;

    CREATE TABLE raw.copper (
            date DATE NOT NULL, 
            value DECIMAL NOT NULL
            );

    \COPY raw.copper from '{}' WITH CSV HEADER;
    """.format(data_dirs['copper']), engine)

    # Load raw.macroeconomic_monthly
    plsql.query_no_return("""
    SET ROLE direccion_trabajo_inspections_write;

    CREATE SCHEMA IF NOT EXISTS raw;

    DROP TABLE IF EXISTS raw.macroeconomic_monthly;

    CREATE TABLE raw.macroeconomic_monthly (
            date DATE NOT NULL, 
            exchange_rate DECIMAL NOT NULL, 
            cpi DECIMAL NOT NULL, 
            civilian_labor_force DECIMAL, 
            total_retail_trade DECIMAL NOT NULL, 
            active_population DECIMAL NOT NULL, 
            employed_population DECIMAL NOT NULL, 
            inactive_population DECIMAL NOT NULL, 
            population DECIMAL NOT NULL, 
            unemployed_population DECIMAL NOT NULL, 
            working_age_population DECIMAL NOT NULL, 
            employment_rate DECIMAL NOT NULL, 
            inactivity_rate DECIMAL NOT NULL, 
            unemployment_rate DECIMAL NOT NULL
            );

    \COPY raw.macroeconomic_monthly from '{}' WITH DELIMITER E'\t' CSV HEADER;
    """.format(data_dirs['macroeconomic_monthly']), engine)
    

    # TODO: load office data

    # TODO: load comunaglosa

    # TODO: some others?

