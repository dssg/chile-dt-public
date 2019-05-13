"""
Code to move data from raw schema to cleaned schema
"""


import sys
sys.path.append('../')
import pipeline.preprocessing as plpre
import pipeline.sql as plsql
from subprocess import call


"""
Function to load data into database
    Given: sqlalchemy engine
"""
def clean_data(engine):
    # Create cleaned table inspections se
    # Joins inspector id from raw inspectors
    qry = """SET ROLE direccion_trabajo_inspections_write;
        CREATE SCHEMA IF NOT EXISTS cleaned;
        DROP TABLE IF EXISTS cleaned.inspections_se;
        CREATE TABLE cleaned.inspections_se AS
            SELECT idfiscalizacion, 
                codoficina, 
                agno, 
                datereg, 
                mesreg, 
                grupocodtipomaterias, 
                grupocodtipomaterias2,
                grupoglosatipomaterias,
                grupoglosatipomaterias2, 
                grupocodigoinfra, 
                grupocodigoinfra2, 
                grupoglosainfra, 
                grupoglosainfra2, 
                grupocodigoinfra_det,
                grupocodigoinfra2_det,
                grupoglosainfra_det, 
                grupoglosainfra2_det,  
                grupocodigonormainfra2_det,
                CASE WHEN grupocodtipomaterias = '99' THEN 
                    ( CASE WHEN grupocodtipomaterias2 = '99' THEN 
                        NULL ELSE 
                        regexp_replace(grupocodtipomaterias2, ';', '|', 'g') 
                        END) ELSE 
                    ( CASE WHEN grupocodtipomaterias = '99' THEN 
                        NULL ELSE 
                        regexp_replace(grupocodtipomaterias, ',', '|', 'g') 
                        END) END 
                        AS grupocodtipomaterias_union,
                CASE WHEN grupoglosatipomaterias = '99' THEN 
                    ( CASE WHEN grupoglosatipomaterias2 = '99' THEN 
                        NULL ELSE 
                        grupoglosatipomaterias2 
                        END) ELSE 
                    ( CASE WHEN grupoglosatipomaterias = '99' THEN 
                        NULL ELSE 
                        grupoglosatipomaterias END) 
                        END 
                        AS grupoglosatipomaterias_union,
                CASE WHEN grupocodigoinfra = '99' THEN 
                    ( CASE WHEN grupocodigoinfra2 = '99' 
                        THEN NULL ELSE 
                        regexp_replace(grupocodigoinfra2 , ';', '|', 'g') 
                        END) ELSE 
                    ( CASE WHEN grupocodigoinfra = '99' 
                        THEN NULL ELSE 
                        regexp_replace(grupocodigoinfra , ',', '|', 'g') 
                        END) END 
                        AS grupocodigoinfra_union,        
                CASE WHEN grupoglosainfra = '99' THEN 
                    ( CASE WHEN grupoglosainfra2 = '99' THEN 
                        NULL ELSE 
                        grupoglosainfra2 
                        END) ELSE 
                    ( CASE WHEN grupoglosainfra = '99' 
                        THEN NULL ELSE 
                        grupoglosainfra 
                        END) END 
                        AS grupoglosainfra_union,
                CASE WHEN grupocodigoinfra_det = '99' THEN 
                    ( CASE WHEN grupocodigoinfra2_det = '99' 
                        THEN NULL ELSE 
                        grupocodigoinfra2_det
                        END) ELSE 
                    ( CASE WHEN grupocodigoinfra_det = '99' 
                        THEN NULL ELSE 
                        grupocodigoinfra_det
                        END) END 
                        AS grupocodigoinfra_det_union,
                CASE WHEN grupoglosainfra_det = '99' THEN 
                    ( CASE WHEN grupoglosainfra2_det = '99' 
                        THEN NULL ELSE 
                        grupoglosainfra2_det 
                        END) ELSE 
                    ( CASE WHEN grupoglosainfra_det = '99' 
                        THEN NULL ELSE 
                        grupoglosainfra_det 
                        END) END 
                        AS grupoglosainfra_det_union,
                CASE WHEN grupocodigonormainfra2_det = '99'
                    THEN NULL ELSE
                    grupocodigonormainfra2_det
                    END AS grupocodigonormainfra2_det_union,
                date(agno || '-' || mesreg || '-01') as date_monthyear,
                nrocomision, 
                CASE WHEN totalafectados in (9999, 'NaN') THEN NULL ELSE totalafectados END as totalafectados_rec,
                urgencia, 
                solesafectado, 
                codtiposol,
                solicitante,
                codunidadorigen,
                codtipotermino,
                lower(tipotermino) as tipotermino,             
                egresoconmulta,   
                emptrabhombres,      
                ntrabajadoressii,
                tramoventassii,     
                codtipoempresa,  
                CASE WHEN empdfcodcomuna in ('0', '-1', '3') 
                    THEN NULL 
                    ELSE trim(empdfcodcomuna) END as empdfcodcomuna_rec,
                CASE WHEN empdmcodcomuna in ('0', '-1', 'None') 
                    THEN NULL 
                    ELSE trim(empdmcodcomuna) END as empdmcodcomuna_rec,
                CASE WHEN codcae in ('0', '-1', 'None', '1') 
                    THEN NULL 
                    ELSE codcae END AS codcae_rec,
                ccae, 
                gcae,
                CASE WHEN crae in ('1') THEN '101' ELSE crae END as crae_rec,
                CASE WHEN grae in ('AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                         ') 
                    THEN 'agricultura, ganadería, caza y silvicultura' 
                    ELSE lower(grae) END AS grae_rec,
                infra,
                noinfra,
                derechofund,
                num_materias,
                num_sind,   
                region,
                infractor,
                exsind,
                rareruts,
                rutisna,
                rutempresamask,
                funasignadomask
            FROM raw.inspections_se A 
            JOIN raw.inspectors B
            USING (idfiscalizacion);"""
    plsql.query_no_return(qry, engine)


    # Merge inspections_se with comunaglosa
    qry = """alter TABLE cleaned.inspections_se alter COLUMN empdfcodcomuna_rec TYPE numeric USING empdfcodcomuna_rec::numeric;
        alter TABLE cleaned.inspections_se add COLUMN codigotgr NUMERIC;
        alter TABLE cleaned.inspections_se add COLUMN glosatgr VARCHAR;
        alter TABLE cleaned.inspections_se add COLUMN activo BOOL;
        alter TABLE cleaned.inspections_se add COLUMN codigo_sf2000 NUMERIC;
        alter TABLE cleaned.inspections_se add COLUMN codprovincia NUMERIC;
        alter TABLE cleaned.inspections_se add COLUMN comuna_entity VARCHAR;
        alter TABLE cleaned.inspections_se add COLUMN glosa VARCHAR;
       UPDATE cleaned.inspections_se t2
            SET 
                codigotgr = t1.codigotgr,
                glosatgr = trim( t1.glosatgr),
                comuna_entity = replace( replace( replace ( 
                                case when trim(t1.glosatgr) = 'Sin Comuna' 
                                then NULL 
                                else trim(t1.glosatgr) end,
                            'CAMARONES.', 'CAMARONES'),
                            'GRAL LAGOS', 'GENERAL LAGOS'),
                            'O''HIGGINS', 'OHIGGINS'),
                activo = t1.activo,
                codigo_sf2000 = t1.codigo_sf2000,
                codprovincia = t1.codprovincia,
                glosa = t1.glosa
            FROM raw.comunaglosa t1
            WHERE t2.empdfcodcomuna_rec = t1.codigo;"""
    plsql.query_no_return(qry, engine)

    qry = """
        alter table cleaned.inspections_se add COLUMN entity_id VARCHAR;
        update cleaned.inspections_se
        set entity_id = concat(rutempresamask, '_', comuna_entity);
        """
    plsql.query_no_return(qry, engine)

    # Merge inspections_se with office data
    qry = """
	alter table cleaned.inspections_se add COLUMN inspection_office_name VARCHAR;
	alter table cleaned.inspections_se add COLUMN inspection_office_address VARCHAR;
	alter table cleaned.inspections_se add COLUMN no_inspectors_office NUMERIC;
	alter table cleaned.inspections_se add COLUMN inspection_office_region NUMERIC;
	alter table cleaned.inspections_se add COLUMN inspection_office_comuna VARCHAR;    
       UPDATE cleaned.inspections_se t2
	    SET
		inspection_office_name = t1.name, 
		inspection_office_address = t1.adress, 
		no_inspectors_office = t1.number_of_inspectors, 
		inspection_office_region = t1.region, 
		inspection_office_comuna = t1.comuna
	    FROM raw.office_data t1
	    WHERE t2.CodOficina = t1.codeoffice;"""
    plsql.query_no_return(qry, engine)

    # Long tables
    # Create long table of inspected matters
    plpre.create_long_table(engine,
            'direccion_trabajo_inspections_write',
            'cleaned',
            'inspections_se',
            'inspected_matters',
            'grupocodtipomaterias_union',
            ['idfiscalizacion', 'datereg', 'rutempresamask', 'date_monthyear'],
            'cleaned',
            'matter_code', '|')

    # Create long table of matters inspected
    plpre.create_long_table(engine,
            'direccion_trabajo_inspections_write',
            'cleaned',
            'inspections_se',
            'infracted_matters',
            'grupocodigoinfra_union',
            ['idfiscalizacion', 'datereg', 'rutempresamask', 'date_monthyear'],
            'cleaned',
            'matter_code', '|')

    # Create long table of detailed matters inspected
    plpre.create_long_table(engine,
            'direccion_trabajo_inspections_write',
            'cleaned',
            'inspections_se',
            'infracted_matters_detailed',
            'grupocodigoinfra_det_union',
            ['idfiscalizacion', 'datereg', 'rutempresamask', 'date_monthyear'],
            'cleaned',
            'matter_code', '||')

    # Create long table of infracted matters from the updated infraction book
    plpre.create_long_table(engine,
            'direccion_trabajo_inspections_write',
            'cleaned',
            'inspections_se',
            'infracted_matters_updatedbook',
            'grupocodigonormainfra2_det_union',
            ['idfiscalizacion', 'datereg', 'rutempresamask', 'date_monthyear'],
            'cleaned',
            'matter_code', '||')

    # Create cleaned table of taxes
    qry = """SET ROLE direccion_trabajo_inspections_write;
        CREATE SCHEMA IF NOT EXISTS cleaned;
        DROP TABLE IF EXISTS cleaned.taxes;
        CREATE TABLE cleaned.taxes AS
            SELECT *, 
                replace( replace( replace( replace( 
                replace( replace( replace( replace( replace( 
                replace( replace( replace( replace( replace ( 
                        case when trim(comuna) = 'Sin Comuna' then NULL else trim(comuna) end, 
                        'LLAY-LLAY', 'LLAY LLAY'),
                        'SAN VICENTE T-T', 'SAN VICENTE'),
                        'SAN FCO DE MOSTAZAL', 'MOSTAZAL'),
                        'EST CENTRAL', 'ESTACION CENTRAL'),
                        'CON CON', 'CONCON'),
                        'ANTARTIDA', 'ANTARTICA'),
                        'PUERTO NATALES', 'NATALES'),
                        'P AGUIRRE CERDA', 'PEDRO AGUIRRE CERDA'),
                        'SAAVEDRA', 'PUERTO SAAVEDRA'), 
                        'QUINTA TILCOCO', 'QUINTA DE TILCOCO'),
                        'SAN JOSE MAIPO', 'SAN JOSE DE MAIPO'),
                        'SAN PEDRO DE MELIPILLA', 'SAN PEDRO'),
                        'TORRES DE PAINE', 'TORRES DEL PAINE'),
                        'TREHUACO', 'TREGUACO') as comuna_entity
            FROM raw.taxes;"""
    plsql.query_no_return(qry, engine)
