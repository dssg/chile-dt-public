"""
Non-generalizable code for staging data
(moving from cleaned to staging schema)
(Intended to be called from run.py as part of the pipeline)
"""

import sys
sys.path.append('../')

import pipeline.sql as plsql

import pandas as pd
import sqlalchemy

from subprocess import call


"""
Function to load data into database
    Given: sqlalchemy engine
    Does:  creates
        staging.entityid
"""


def stage_data(engine):

    # Create entity id table
    qry = """SET ROLE direccion_trabajo_inspections_write;
        CREATE SCHEMA IF NOT EXISTS staging;
        DROP TABLE IF EXISTS staging.temp_entityid_ins;
        CREATE TABLE staging.temp_entityid_ins as
        select distinct (entity_id), rutempresamask as rutmask_ins, comuna_ins from
            (select rutmask as rutempresamask, comuna_entity as comuna_ins, CONCAT(rutmask, '_', comuna_entity) as entity_id
            from cleaned.inspections_se) as t1;
        DROP TABLE IF EXISTS staging.temp_entityid_tax;
        CREATE TABLE staging.temp_entityid_tax as
            select distinct (entity_id), rutmask as rutmask_tax, comuna_entity as comuna_tax from
            (select rutmask, comuna_entity, CONCAT(rutmask, '_', comuna_entity) as entity_id
            from cleaned.taxes) as t2;
        DROP TABLE IF EXISTS staging.entity_id;
        CREATE TABLE staging.entity_id as
            select * from staging.temp_entityid_tax tab1
            full join  staging.temp_entityid_ins tab2
            using (entity_id);
        DROP TABLE IF EXISTS staging.temp_entityid_ins;
        DROP TABLE IF EXISTS staging.temp_entityid_tax;"""
    plsql.query_no_return(qry, engine)

    # Create table month year 
    qry = """SET ROLE direccion_trabajo_inspections_write;
        CREATE SCHEMA IF NOT EXISTS staging;
        DROP TABLE IF EXISTS  staging.month_year;
        CREATE TABLE staging.month_year as 
           SELECT DATE '2006-01-01' +  
                    (interval '1' month * generate_series(0,131)) as month_year, 
                cast( date_part('month', DATE '2006-01-01' +  
                    (interval '1' month * generate_series(0,131))) as int) as month,
                cast( date_part('year', DATE '2006-01-01' +  
                    (interval '1' month * generate_series(0,131))) as int) as year;"""
    plsql.query_no_return(qry, engine)

    # Create unique entity, month_year table from inspections
    qry = """set role direccion_trabajo_inspections_write;
        create schema if not exists staging;
        drop table if exists staging.entity_unique_inspections;
        create table staging.entity_unique_inspections as
        select distinct
            comuna_entity as comuna_ins,
            rutempresamask as rutmask_ins,
            entity_id,
            date_monthyear
            from cleaned.inspections_se;
        create index if not exists 
        month_year_unique_ins_index on
        staging.entity_unique_inspections (date_monthyear);
        """
    plsql.query_no_return(qry, engine)

    # Create table month-year-entity-id for taxes
    qry = """Set role direccion_trabajo_inspections_write;
        create schema if not exists staging;
        drop table if exists staging.entity_unique_taxes
        CREATE TABLE staging.entity_unique_taxes as
           SELECT 
           m.month_year,
           e.entity_id
           from cleaned.taxes t
           right join staging.month_year m
           on t.agno = m.year
           right join staging.entity_id e
           on e.rutmask_tax = t.rutmask
           and e.comuna_tax = t.comuna_entity;
        create unique index emy_tax_index on staging.entity_month_year
        (month_year, entity_id);
        select month_year, entity_id from staging.entity_unique_taxes
        where month_year is not null
        union
        select date_monthyear as month_year, 
        concat(rutmask_ins, '_', comuna_ins) as entity_id
        from staging.entity_unique_inspections
        where date_monthyear is not null;
        """
     
    # Creates table with industry matching and translation between taxes and insepctions
    # (Translated and matched by Sonia)
    # inspections: column used 'ccrae_rec'
    codes_ins = [111,100,0,112,101,107,6,5,106,4,113,8,2,103,
                 116,108,104,3,110,117,115,102,9,114,105,109,7]
    labels_ins = ['real_estate','others','others','public_administration_and_defense',
                  'agriculture_livestock_hunting_and_forestry','commerce','commerce',
                  'construction','construction','electricity_gas_ and_water','education',
                  'financial_establishments_insurance,_real estate_and_services',
                  'exploitation_mines_and_quarries','exploitation_mines_and_quarries',
                  'private_homes_with_domestic_service','hotels_and_restaurants',
                  'manufacturing_industries','manufacturing_industries','financial_intermediation',
                  'organizations_and_extraterritorial_bodies','other_community_service_activities',
                  'fishing','state_social_services','other_social_and_health_services',
                  'electricity_gas_and_water_supply','transportation_storage_and_communications',
                  'transportation_storage_and_communications']
    source_ins = ['ins']*len(labels_ins)

    # taxes: column used 'rubro'
    codes_tax = ['L','M','A','H','G','N','C','I','K','R','P',
                 'B','F','J','SIN INFORMACION','E','D','Q','O']
    labels_tax = ['real_state_business_and_rental','public_administration_and_defense',
                  'agriculture_livestock_hunting_and_forestry','commerce','construction',
                  'education','exploitation_mines_and_quarries','hotels_and_restaurants',
                  'financial_intermediation','organizations_and_extraterritorial_bodies',
                  'other_community_service_activities','fishing','electricity_gas_and_water_supply',
                  'transportation_storage_and_communications','none','manufacturing_industries',
                  'manufacturing_industries','administration_council_of_buildings','social_and_health_services']
    source_tax = ['tax']*len(labels_tax)

    d = {'code': codes_ins + codes_tax, 
         'industry': labels_ins + labels_tax, 
         'source': source_ins + source_tax}
    df = pd.DataFrame(data=d)

    plsql.query_no_return("set role direccion_trabajo_inspections_write;", engine)
    df.to_sql("industry_codes", engine, "cleaned", "replace")
        
        
        
     # Create table industry by entity month year deleting duplicates
    qry = """SET ROLE direccion_trabajo_inspections_write;
        SET ROLE direccion_trabajo_inspections_write;
        
        DROP TABLE IF EXISTX staging.tmp_ins_ind;
        CREATE TABLE staging.tmp_ins_ind as
            SELECT entity_id as entity_id, 
                            date_monthyear as month_year, 
                            industry as industry
                        FROM (
                            SELECT entity_id, date_monthyear, grae_rec, crae_rec as code
                            FROM cleaned.inspections_se
                            GROUP BY entity_id, date_monthyear, grae_rec, crae_rec) s
                    LEFT JOIN cleaned.industry_codes c
                    ON c.code = s.code;

        DROP TABLE IF EXISTS staging.tmp_taxes_ind;
        CREATE TABLE staging.tmp_taxes_ind AS
            SELECT entity_id as entity_id, 
                        month_year as month_year, 
                        industry as industry
                    FROM (
                         SELECT entity_id, 
                            agno as year, 
                            industry
                            FROM 
                                (
                                SELECT agno, 
                                    CONCAT(rutmask, '_', comuna_entity) AS entity_id,
                                    TRIM(split_part(rubro, '-', 1)) AS code
                                FROM cleaned.taxes
                                GROUP BY code, agno, entity_id
                                ) s
                        LEFT JOIN cleaned.industry_codes c
                        ON c.code = s.code
                        ) sc
                    LEFT JOIN staging.month_year m
                    ON sc.year = m.year;

        DROP TABLE IF EXISTS staging.industry_month_year;
        CREATE TABLE staging.industry_month_year as
            SELECT * FROM staging.tmp_taxes_ind
            UNION 
            SELECT * FROM staging.tmp_ins_ind;

        DROP TABLE IF EXISTS staging.tmp_taxes_ind;
        DROP TABLE IF EXISTS staging.tmp_ins_ind;
     """
