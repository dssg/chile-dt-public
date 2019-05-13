"""
File for the Big Query that we'll use to create our time-split staging tables
Includes: feature generation, joining many data sources
"""

import sys
sys.path.append('../')
import pipeline.sql as plsql

def create_table(engine, date_start, date_end):

    date_start_string = date_start.strftime('%Y-%m-%d').replace('-','')
    date_end_string = date_end.strftime('%Y-%m-%d').replace('-','')
    
    create_query_train = """
        -- Set seed
        SELECT setseed(0.5);

        -- Creating materialized view by month
        CREATE MATERIALIZED VIEW IF NOT EXISTS staging.month_features
        AS
            SELECT 
            m.*,
            avg_copper_price
            from cleaned.copper_formatted c
            FULL JOIN 
            cleaned.macro_month m
            ON m.month_plus_one = DATE_TRUNC('month', c.month_plus_one);
            

        -- Creating materialized view by entity
        CREATE MATERIALIZED VIEW IF NOT EXISTS staging.geographic
        AS 
            SELECT DISTINCT
            entity_id AS entity_id_geographic, 
            CASE WHEN comuna_entity = 'ALGARROBO' then true else false end AS comuna_algarrobo, 
            CASE WHEN comuna_entity = 'ALHUE' then true else false end AS comuna_alhue, 
            CASE WHEN comuna_entity = 'ALTO BIO BIO' then true else false end AS comuna_altobiobio, 
            CASE WHEN comuna_entity = 'ALTO DEL CARMEN' then true else false end AS comuna_altodelcarmen, 
            CASE WHEN comuna_entity = 'ALTO HOSPICIO' then true else false end AS comuna_altohospicio, 
            CASE WHEN comuna_entity = 'ANCUD' then true else false end AS comuna_ancud, 
            CASE WHEN comuna_entity = 'ANDACOLLO' then true else false end AS comuna_andacollo, 
            CASE WHEN comuna_entity = 'ANGOL' then true else false end AS comuna_angol, 
            CASE WHEN comuna_entity = 'ANTARTICA' then true else false end AS comuna_antartica, 
            CASE WHEN comuna_entity = 'ANTOFAGASTA' then true else false end AS comuna_antofagasta, 
            CASE WHEN comuna_entity = 'ANTUCO' then true else false end AS comuna_antuco, 
            CASE WHEN comuna_entity = 'ARAUCO' then true else false end AS comuna_arauco, 
            CASE WHEN comuna_entity = 'ARICA' then true else false end AS comuna_arica, 
            CASE WHEN comuna_entity = 'AYSEN' then true else false end AS comuna_aysen, 
            CASE WHEN comuna_entity = 'BUIN' then true else false end AS comuna_buin, 
            CASE WHEN comuna_entity = 'BULNES' then true else false end AS comuna_bulnes, 
            CASE WHEN comuna_entity = 'CABILDO' then true else false end AS comuna_cabildo, 
            CASE WHEN comuna_entity = 'CABO DE HORNOS' then true else false end AS comuna_cabodehornos, 
            CASE WHEN comuna_entity = 'CABRERO' then true else false end AS comuna_cabrero, 
            CASE WHEN comuna_entity = 'CALAMA' then true else false end AS comuna_calama, 
            CASE WHEN comuna_entity = 'CALBUCO' then true else false end AS comuna_calbuco, 
            CASE WHEN comuna_entity = 'CALDERA' then true else false end AS comuna_caldera, 
            CASE WHEN comuna_entity = 'CALERA DE TANGO' then true else false end AS comuna_caleradetango, 
            CASE WHEN comuna_entity = 'CALLE LARGA' then true else false end AS comuna_callelarga, 
            CASE WHEN comuna_entity = 'CAMARONES' then true else false end AS comuna_camarones, 
            CASE WHEN comuna_entity = 'CAMINA' then true else false end AS comuna_camina, 
            CASE WHEN comuna_entity = 'CANELA' then true else false end AS comuna_canela, 
            CASE WHEN comuna_entity = 'CANETE' then true else false end AS comuna_canete, 
            CASE WHEN comuna_entity = 'CARAHUE' then true else false end AS comuna_carahue, 
            CASE WHEN comuna_entity = 'CARTAGENA' then true else false end AS comuna_cartagena, 
            CASE WHEN comuna_entity = 'CASABLANCA' then true else false end AS comuna_casablanca, 
            CASE WHEN comuna_entity = 'CASTRO' then true else false end AS comuna_castro, 
            CASE WHEN comuna_entity = 'CATEMU' then true else false end AS comuna_catemu, 
            CASE WHEN comuna_entity = 'CAUQUENES' then true else false end AS comuna_cauquenes, 
            CASE WHEN comuna_entity = 'CERRILLOS' then true else false end AS comuna_cerrillos, 
            CASE WHEN comuna_entity = 'CERRO NAVIA' then true else false end AS comuna_cerronavia, 
            CASE WHEN comuna_entity = 'CHAITEN' then true else false end AS comuna_chaiten, 
            CASE WHEN comuna_entity = 'CHANARAL' then true else false end AS comuna_chanaral, 
            CASE WHEN comuna_entity = 'CHANCO' then true else false end AS comuna_chanco, 
            CASE WHEN comuna_entity = 'CHEPICA' then true else false end AS comuna_chepica, 
            CASE WHEN comuna_entity = 'CHIGUAYANTE' then true else false end AS comuna_chiguayante, 
            CASE WHEN comuna_entity = 'CHILE CHICO' then true else false end AS comuna_chilechico, 
            CASE WHEN comuna_entity = 'CHILLAN' then true else false end AS comuna_chillan, 
            CASE WHEN comuna_entity = 'CHILLAN VIEJO' then true else false end AS comuna_chillanviejo, 
            CASE WHEN comuna_entity = 'CHIMBARONGO' then true else false end AS comuna_chimbarongo, 
            CASE WHEN comuna_entity = 'CHOLCHOL' then true else false end AS comuna_cholchol, 
            CASE WHEN comuna_entity = 'CHONCHI' then true else false end AS comuna_chonchi, 
            CASE WHEN comuna_entity = 'CISNES' then true else false end AS comuna_cisnes, 
            CASE WHEN comuna_entity = 'COBQUECURA' then true else false end AS comuna_cobquecura, 
            CASE WHEN comuna_entity = 'COCHAMO' then true else false end AS comuna_cochamo, 
            CASE WHEN comuna_entity = 'COCHRANE' then true else false end AS comuna_cochrane, 
            CASE WHEN comuna_entity = 'CODEGUA' then true else false end AS comuna_codegua, 
            CASE WHEN comuna_entity = 'COELEMU' then true else false end AS comuna_coelemu, 
            CASE WHEN comuna_entity = 'COIHUECO' then true else false end AS comuna_coihueco, 
            CASE WHEN comuna_entity = 'COINCO' then true else false end AS comuna_coinco, 
            CASE WHEN comuna_entity = 'COLBUN' then true else false end AS comuna_colbun, 
            CASE WHEN comuna_entity = 'COLCHANE' then true else false end AS comuna_colchane, 
            CASE WHEN comuna_entity = 'COLINA' then true else false end AS comuna_colina, 
            CASE WHEN comuna_entity = 'COLLIPULLI' then true else false end AS comuna_collipulli, 
            CASE WHEN comuna_entity = 'COLTAUCO' then true else false end AS comuna_coltauco, 
            CASE WHEN comuna_entity = 'COMBARBALA' then true else false end AS comuna_combarbala, 
            CASE WHEN comuna_entity = 'CONCEPCION' then true else false end AS comuna_concepcion, 
            CASE WHEN comuna_entity = 'CONCHALI' then true else false end AS comuna_conchali, 
            CASE WHEN comuna_entity = 'CONCON' then true else false end AS comuna_concon, 
            CASE WHEN comuna_entity = 'CONSTITUCION' then true else false end AS comuna_constitucion, 
            CASE WHEN comuna_entity = 'CONTULMO' then true else false end AS comuna_contulmo, 
            CASE WHEN comuna_entity = 'COPIAPO' then true else false end AS comuna_copiapo, 
            CASE WHEN comuna_entity = 'COQUIMBO' then true else false end AS comuna_coquimbo, 
            CASE WHEN comuna_entity = 'CORONEL' then true else false end AS comuna_coronel, 
            CASE WHEN comuna_entity = 'CORRAL' then true else false end AS comuna_corral, 
            CASE WHEN comuna_entity = 'COYHAIQUE' then true else false end AS comuna_coyhaique, 
            CASE WHEN comuna_entity = 'CUNCO' then true else false end AS comuna_cunco, 
            CASE WHEN comuna_entity = 'CURACAUTIN' then true else false end AS comuna_curacautin, 
            CASE WHEN comuna_entity = 'CURACAVI' then true else false end AS comuna_curacavi, 
            CASE WHEN comuna_entity = 'CURACO DE VELEZ' then true else false end AS comuna_curacodevelez, 
            CASE WHEN comuna_entity = 'CURANILAHUE' then true else false end AS comuna_curanilahue, 
            CASE WHEN comuna_entity = 'CURARREHUE' then true else false end AS comuna_curarrehue, 
            CASE WHEN comuna_entity = 'CUREPTO' then true else false end AS comuna_curepto, 
            CASE WHEN comuna_entity = 'CURICO' then true else false end AS comuna_curico, 
            CASE WHEN comuna_entity = 'DALCAHUE' then true else false end AS comuna_dalcahue, 
            CASE WHEN comuna_entity = 'DIEGO DE ALMAGRO' then true else false end AS comuna_diegodealmagro, 
            CASE WHEN comuna_entity = 'DONIHUE' then true else false end AS comuna_donihue, 
            CASE WHEN comuna_entity = 'EL BOSQUE' then true else false end AS comuna_elbosque, 
            CASE WHEN comuna_entity = 'EL CARMEN' then true else false end AS comuna_elcarmen, 
            CASE WHEN comuna_entity = 'EL MONTE' then true else false end AS comuna_elmonte, 
            CASE WHEN comuna_entity = 'EL QUISCO' then true else false end AS comuna_elquisco, 
            CASE WHEN comuna_entity = 'EL TABO' then true else false end AS comuna_eltabo, 
            CASE WHEN comuna_entity = 'EMPEDRADO' then true else false end AS comuna_empedrado, 
            CASE WHEN comuna_entity = 'ERCILLA' then true else false end AS comuna_ercilla, 
            CASE WHEN comuna_entity = 'ESTACION CENTRAL' then true else false end AS comuna_estacioncentral, 
            CASE WHEN comuna_entity = 'FLORIDA' then true else false end AS comuna_florida, 
            CASE WHEN comuna_entity = 'FREIRE' then true else false end AS comuna_freire, 
            CASE WHEN comuna_entity = 'FREIRINA' then true else false end AS comuna_freirina, 
            CASE WHEN comuna_entity = 'FRESIA' then true else false end AS comuna_fresia, 
            CASE WHEN comuna_entity = 'FRUTILLAR' then true else false end AS comuna_frutillar, 
            CASE WHEN comuna_entity = 'FUTALEUFU' then true else false end AS comuna_futaleufu, 
            CASE WHEN comuna_entity = 'FUTRONO' then true else false end AS comuna_futrono, 
            CASE WHEN comuna_entity = 'GALVARINO' then true else false end AS comuna_galvarino, 
            CASE WHEN comuna_entity = 'GENERAL LAGOS' then true else false end AS comuna_generallagos, 
            CASE WHEN comuna_entity = 'GORBEA' then true else false end AS comuna_gorbea, 
            CASE WHEN comuna_entity = 'GRANEROS' then true else false end AS comuna_graneros, 
            CASE WHEN comuna_entity = 'GUAITECAS' then true else false end AS comuna_guaitecas, 
            CASE WHEN comuna_entity = 'HIJUELAS' then true else false end AS comuna_hijuelas, 
            CASE WHEN comuna_entity = 'HUALAIHUE' then true else false end AS comuna_hualaihue, 
            CASE WHEN comuna_entity = 'HUALANE' then true else false end AS comuna_hualane, 
            CASE WHEN comuna_entity = 'HUALPEN' then true else false end AS comuna_hualpen, 
            CASE WHEN comuna_entity = 'HUALQUI' then true else false end AS comuna_hualqui, 
            CASE WHEN comuna_entity = 'HUARA' then true else false end AS comuna_huara, 
            CASE WHEN comuna_entity = 'HUASCO' then true else false end AS comuna_huasco, 
            CASE WHEN comuna_entity = 'HUECHURABA' then true else false end AS comuna_huechuraba, 
            CASE WHEN comuna_entity = 'ILLAPEL' then true else false end AS comuna_illapel, 
            CASE WHEN comuna_entity = 'INDEPENDENCIA' then true else false end AS comuna_independencia, 
            CASE WHEN comuna_entity = 'IQUIQUE' then true else false end AS comuna_iquique, 
            CASE WHEN comuna_entity = 'ISLA DE MAIPO' then true else false end AS comuna_islademaipo, 
            CASE WHEN comuna_entity = 'ISLA DE PASCUA' then true else false end AS comuna_isladepascua, 
            CASE WHEN comuna_entity = 'JUAN FERNANDEZ' then true else false end AS comuna_juanfernandez, 
            CASE WHEN comuna_entity = 'LA CALERA' then true else false end AS comuna_lacalera, 
            CASE WHEN comuna_entity = 'LA CISTERNA' then true else false end AS comuna_lacisterna, 
            CASE WHEN comuna_entity = 'LA CRUZ' then true else false end AS comuna_lacruz, 
            CASE WHEN comuna_entity = 'LA ESTRELLA' then true else false end AS comuna_laestrella, 
            CASE WHEN comuna_entity = 'LA FLORIDA' then true else false end AS comuna_laflorida, 
            CASE WHEN comuna_entity = 'LAGO RANCO' then true else false end AS comuna_lagoranco, 
            CASE WHEN comuna_entity = 'LAGO VERDE' then true else false end AS comuna_lagoverde, 
            CASE WHEN comuna_entity = 'LA GRANJA' then true else false end AS comuna_lagranja, 
            CASE WHEN comuna_entity = 'LAGUNA BLANCA' then true else false end AS comuna_lagunablanca, 
            CASE WHEN comuna_entity = 'LA HIGUERA' then true else false end AS comuna_lahiguera, 
            CASE WHEN comuna_entity = 'LAJA' then true else false end AS comuna_laja, 
            CASE WHEN comuna_entity = 'LA LIGUA' then true else false end AS comuna_laligua, 
            CASE WHEN comuna_entity = 'LAMPA' then true else false end AS comuna_lampa, 
            CASE WHEN comuna_entity = 'LANCO' then true else false end AS comuna_lanco, 
            CASE WHEN comuna_entity = 'LA PINTANA' then true else false end AS comuna_lapintana, 
            CASE WHEN comuna_entity = 'LA REINA' then true else false end AS comuna_lareina, 
            CASE WHEN comuna_entity = 'LAS CABRAS' then true else false end AS comuna_lascabras, 
            CASE WHEN comuna_entity = 'LAS CONDES' then true else false end AS comuna_lascondes, 
            CASE WHEN comuna_entity = 'LA SERENA' then true else false end AS comuna_laserena, 
            CASE WHEN comuna_entity = 'LA UNION' then true else false end AS comuna_launion, 
            CASE WHEN comuna_entity = 'LAUTARO' then true else false end AS comuna_lautaro, 
            CASE WHEN comuna_entity = 'LEBU' then true else false end AS comuna_lebu, 
            CASE WHEN comuna_entity = 'LICANTEN' then true else false end AS comuna_licanten, 
            CASE WHEN comuna_entity = 'LIMACHE' then true else false end AS comuna_limache, 
            CASE WHEN comuna_entity = 'LINARES' then true else false end AS comuna_linares, 
            CASE WHEN comuna_entity = 'LITUECHE' then true else false end AS comuna_litueche, 
            CASE WHEN comuna_entity = 'LLANQUIHUE' then true else false end AS comuna_llanquihue, 
            CASE WHEN comuna_entity = 'LLAY LLAY' then true else false end AS comuna_llayllay, 
            CASE WHEN comuna_entity = 'LO BARNECHEA' then true else false end AS comuna_lobarnechea, 
            CASE WHEN comuna_entity = 'LO ESPEJO' then true else false end AS comuna_loespejo, 
            CASE WHEN comuna_entity = 'LOLOL' then true else false end AS comuna_lolol, 
            CASE WHEN comuna_entity = 'LONCOCHE' then true else false end AS comuna_loncoche, 
            CASE WHEN comuna_entity = 'LONGAVI' then true else false end AS comuna_longavi, 
            CASE WHEN comuna_entity = 'LONQUIMAY' then true else false end AS comuna_lonquimay, 
            CASE WHEN comuna_entity = 'LO PRADO' then true else false end AS comuna_loprado, 
            CASE WHEN comuna_entity = 'LOS ALAMOS' then true else false end AS comuna_losalamos, 
            CASE WHEN comuna_entity = 'LOS ANDES' then true else false end AS comuna_losandes, 
            CASE WHEN comuna_entity = 'LOS ANGELES' then true else false end AS comuna_losangeles, 
            CASE WHEN comuna_entity = 'LOS LAGOS' then true else false end AS comuna_loslagos, 
            CASE WHEN comuna_entity = 'LOS MUERMOS' then true else false end AS comuna_losmuermos, 
            CASE WHEN comuna_entity = 'LOS SAUCES' then true else false end AS comuna_lossauces, 
            CASE WHEN comuna_entity = 'LOS VILOS' then true else false end AS comuna_losvilos, 
            CASE WHEN comuna_entity = 'LOTA' then true else false end AS comuna_lota, 
            CASE WHEN comuna_entity = 'LUMACO' then true else false end AS comuna_lumaco, 
            CASE WHEN comuna_entity = 'MACHALI' then true else false end AS comuna_machali, 
            CASE WHEN comuna_entity = 'MACUL' then true else false end AS comuna_macul, 
            CASE WHEN comuna_entity = 'MAFIL' then true else false end AS comuna_mafil, 
            CASE WHEN comuna_entity = 'MAIPU' then true else false end AS comuna_maipu, 
            CASE WHEN comuna_entity = 'MALLOA' then true else false end AS comuna_malloa, 
            CASE WHEN comuna_entity = 'MARCHIGUE' then true else false end AS comuna_marchigue, 
            CASE WHEN comuna_entity = 'MARIA ELENA' then true else false end AS comuna_mariaelena, 
            CASE WHEN comuna_entity = 'MARIA PINTO' then true else false end AS comuna_mariapinto, 
            CASE WHEN comuna_entity = 'MARIQUINA' then true else false end AS comuna_mariquina, 
            CASE WHEN comuna_entity = 'MAULE' then true else false end AS comuna_maule, 
            CASE WHEN comuna_entity = 'MAULLIN' then true else false end AS comuna_maullin, 
            CASE WHEN comuna_entity = 'MEJILLONES' then true else false end AS comuna_mejillones, 
            CASE WHEN comuna_entity = 'MELIPEUCO' then true else false end AS comuna_melipeuco, 
            CASE WHEN comuna_entity = 'MELIPILLA' then true else false end AS comuna_melipilla, 
            CASE WHEN comuna_entity = 'MOLINA' then true else false end AS comuna_molina, 
            CASE WHEN comuna_entity = 'MONTE PATRIA' then true else false end AS comuna_montepatria, 
            CASE WHEN comuna_entity = 'MOSTAZAL' then true else false end AS comuna_mostazal, 
            CASE WHEN comuna_entity = 'MULCHEN' then true else false end AS comuna_mulchen, 
            CASE WHEN comuna_entity = 'NACIMIENTO' then true else false end AS comuna_nacimiento, 
            CASE WHEN comuna_entity = 'NANCAGUA' then true else false end AS comuna_nancagua, 
            CASE WHEN comuna_entity = 'NATALES' then true else false end AS comuna_natales, 
            CASE WHEN comuna_entity = 'NAVIDAD' then true else false end AS comuna_navidad, 
            CASE WHEN comuna_entity = 'NEGRETE' then true else false end AS comuna_negrete, 
            CASE WHEN comuna_entity = 'NINHUE' then true else false end AS comuna_ninhue, 
            CASE WHEN comuna_entity = 'NIQUEN' then true else false end AS comuna_niquen, 
            CASE WHEN comuna_entity = 'NOGALES' then true else false end AS comuna_nogales, 
            CASE WHEN comuna_entity = 'NUEVA IMPERIAL' then true else false end AS comuna_nuevaimperial, 
            CASE WHEN comuna_entity = 'NUNOA' then true else false end AS comuna_nunoa, 
            CASE WHEN comuna_entity = 'OHIGGINS' then true else false end AS comuna_ohiggins, 
            CASE WHEN comuna_entity = 'OLIVAR' then true else false end AS comuna_olivar, 
            CASE WHEN comuna_entity = 'OLLAGUE' then true else false end AS comuna_ollague, 
            CASE WHEN comuna_entity = 'OLMUE' then true else false end AS comuna_olmue, 
            CASE WHEN comuna_entity = 'OSORNO' then true else false end AS comuna_osorno, 
            CASE WHEN comuna_entity = 'OVALLE' then true else false end AS comuna_ovalle, 
            CASE WHEN comuna_entity = 'PADRE HURTADO' then true else false end AS comuna_padrehurtado, 
            CASE WHEN comuna_entity = 'PADRE LAS CASAS' then true else false end AS comuna_padrelascasas, 
            CASE WHEN comuna_entity = 'PAIHUANO' then true else false end AS comuna_paihuano, 
            CASE WHEN comuna_entity = 'PAILLACO' then true else false end AS comuna_paillaco, 
            CASE WHEN comuna_entity = 'PAINE' then true else false end AS comuna_paine, 
            CASE WHEN comuna_entity = 'PALENA' then true else false end AS comuna_palena, 
            CASE WHEN comuna_entity = 'PALMILLA' then true else false end AS comuna_palmilla, 
            CASE WHEN comuna_entity = 'PANGUIPULLI' then true else false end AS comuna_panguipulli, 
            CASE WHEN comuna_entity = 'PANQUEHUE' then true else false end AS comuna_panquehue, 
            CASE WHEN comuna_entity = 'PAPUDO' then true else false end AS comuna_papudo, 
            CASE WHEN comuna_entity = 'PAREDONES' then true else false end AS comuna_paredones, 
            CASE WHEN comuna_entity = 'PARRAL' then true else false end AS comuna_parral, 
            CASE WHEN comuna_entity = 'PEDRO AGUIRRE CERDA' then true else false end AS comuna_pedroaguirrecerda, 
            CASE WHEN comuna_entity = 'PELARCO' then true else false end AS comuna_pelarco, 
            CASE WHEN comuna_entity = 'PELLUHUE' then true else false end AS comuna_pelluhue, 
            CASE WHEN comuna_entity = 'PEMUCO' then true else false end AS comuna_pemuco, 
            CASE WHEN comuna_entity = 'PENAFLOR' then true else false end AS comuna_penaflor, 
            CASE WHEN comuna_entity = 'PENALOLEN' then true else false end AS comuna_penalolen, 
            CASE WHEN comuna_entity = 'PENCAHUE' then true else false end AS comuna_pencahue, 
            CASE WHEN comuna_entity = 'PENCO' then true else false end AS comuna_penco, 
            CASE WHEN comuna_entity = 'PERALILLO' then true else false end AS comuna_peralillo, 
            CASE WHEN comuna_entity = 'PERQUENCO' then true else false end AS comuna_perquenco, 
            CASE WHEN comuna_entity = 'PETORCA' then true else false end AS comuna_petorca, 
            CASE WHEN comuna_entity = 'PEUMO' then true else false end AS comuna_peumo, 
            CASE WHEN comuna_entity = 'PICA' then true else false end AS comuna_pica, 
            CASE WHEN comuna_entity = 'PICHIDEGUA' then true else false end AS comuna_pichidegua, 
            CASE WHEN comuna_entity = 'PICHILEMU' then true else false end AS comuna_pichilemu, 
            CASE WHEN comuna_entity = 'PINTO' then true else false end AS comuna_pinto, 
            CASE WHEN comuna_entity = 'PIRQUE' then true else false end AS comuna_pirque, 
            CASE WHEN comuna_entity = 'PITRUFQUEN' then true else false end AS comuna_pitrufquen, 
            CASE WHEN comuna_entity = 'PLACILLA' then true else false end AS comuna_placilla, 
            CASE WHEN comuna_entity = 'PORTEZUELO' then true else false end AS comuna_portezuelo, 
            CASE WHEN comuna_entity = 'PORVENIR' then true else false end AS comuna_porvenir, 
            CASE WHEN comuna_entity = 'POZO ALMONTE' then true else false end AS comuna_pozoalmonte, 
            CASE WHEN comuna_entity = 'PRIMAVERA' then true else false end AS comuna_primavera, 
            CASE WHEN comuna_entity = 'PROVIDENCIA' then true else false end AS comuna_providencia, 
            CASE WHEN comuna_entity = 'PUCHUNCAVI' then true else false end AS comuna_puchuncavi, 
            CASE WHEN comuna_entity = 'PUCON' then true else false end AS comuna_pucon, 
            CASE WHEN comuna_entity = 'PUDAHUEL' then true else false end AS comuna_pudahuel, 
            CASE WHEN comuna_entity = 'PUENTE ALTO' then true else false end AS comuna_puentealto, 
            CASE WHEN comuna_entity = 'PUERTO MONTT' then true else false end AS comuna_puertomontt, 
            CASE WHEN comuna_entity = 'PUERTO OCTAY' then true else false end AS comuna_puertooctay, 
            CASE WHEN comuna_entity = 'PUERTO SAAVEDRA' then true else false end AS comuna_puertosaavedra, 
            CASE WHEN comuna_entity = 'PUERTO VARAS' then true else false end AS comuna_puertovaras, 
            CASE WHEN comuna_entity = 'PUMANQUE' then true else false end AS comuna_pumanque, 
            CASE WHEN comuna_entity = 'PUNITAQUI' then true else false end AS comuna_punitaqui, 
            CASE WHEN comuna_entity = 'PUNTA ARENAS' then true else false end AS comuna_puntaarenas, 
            CASE WHEN comuna_entity = 'PUQUELDON' then true else false end AS comuna_puqueldon, 
            CASE WHEN comuna_entity = 'PUREN' then true else false end AS comuna_puren, 
            CASE WHEN comuna_entity = 'PURRANQUE' then true else false end AS comuna_purranque, 
            CASE WHEN comuna_entity = 'PUTAENDO' then true else false end AS comuna_putaendo, 
            CASE WHEN comuna_entity = 'PUTRE' then true else false end AS comuna_putre, 
            CASE WHEN comuna_entity = 'PUYEHUE' then true else false end AS comuna_puyehue, 
            CASE WHEN comuna_entity = 'QUEILEN' then true else false end AS comuna_queilen, 
            CASE WHEN comuna_entity = 'QUELLON' then true else false end AS comuna_quellon, 
            CASE WHEN comuna_entity = 'QUEMCHI' then true else false end AS comuna_quemchi, 
            CASE WHEN comuna_entity = 'QUILACO' then true else false end AS comuna_quilaco, 
            CASE WHEN comuna_entity = 'QUILICURA' then true else false end AS comuna_quilicura, 
            CASE WHEN comuna_entity = 'QUILLECO' then true else false end AS comuna_quilleco, 
            CASE WHEN comuna_entity = 'QUILLON' then true else false end AS comuna_quillon, 
            CASE WHEN comuna_entity = 'QUILLOTA' then true else false end AS comuna_quillota, 
            CASE WHEN comuna_entity = 'QUILPUE' then true else false end AS comuna_quilpue, 
            CASE WHEN comuna_entity = 'QUINCHAO' then true else false end AS comuna_quinchao, 
            CASE WHEN comuna_entity = 'QUINTA DE TILCOCO' then true else false end AS comuna_quintadetilcoco, 
            CASE WHEN comuna_entity = 'QUINTA NORMAL' then true else false end AS comuna_quintanormal, 
            CASE WHEN comuna_entity = 'QUINTERO' then true else false end AS comuna_quintero, 
            CASE WHEN comuna_entity = 'QUIRIHUE' then true else false end AS comuna_quirihue, 
            CASE WHEN comuna_entity = 'RANCAGUA' then true else false end AS comuna_rancagua, 
            CASE WHEN comuna_entity = 'RANQUIL' then true else false end AS comuna_ranquil, 
            CASE WHEN comuna_entity = 'RAUCO' then true else false end AS comuna_rauco, 
            CASE WHEN comuna_entity = 'RECOLETA' then true else false end AS comuna_recoleta, 
            CASE WHEN comuna_entity = 'RENAICO' then true else false end AS comuna_renaico, 
            CASE WHEN comuna_entity = 'RENCA' then true else false end AS comuna_renca, 
            CASE WHEN comuna_entity = 'RENGO' then true else false end AS comuna_rengo, 
            CASE WHEN comuna_entity = 'REQUINOA' then true else false end AS comuna_requinoa, 
            CASE WHEN comuna_entity = 'RETIRO' then true else false end AS comuna_retiro, 
            CASE WHEN comuna_entity = 'RINCONADA' then true else false end AS comuna_rinconada, 
            CASE WHEN comuna_entity = 'RIO BUENO' then true else false end AS comuna_riobueno, 
            CASE WHEN comuna_entity = 'RIO CLARO' then true else false end AS comuna_rioclaro, 
            CASE WHEN comuna_entity = 'RIO HURTADO' then true else false end AS comuna_riohurtado, 
            CASE WHEN comuna_entity = 'RIO IBANEZ' then true else false end AS comuna_rioibanez, 
            CASE WHEN comuna_entity = 'RIO NEGRO' then true else false end AS comuna_rionegro, 
            CASE WHEN comuna_entity = 'RIO VERDE' then true else false end AS comuna_rioverde, 
            CASE WHEN comuna_entity = 'ROMERAL' then true else false end AS comuna_romeral, 
            CASE WHEN comuna_entity = 'SAGRADA FAMILIA' then true else false end AS comuna_sagradafamilia, 
            CASE WHEN comuna_entity = 'SALAMANCA' then true else false end AS comuna_salamanca, 
            CASE WHEN comuna_entity = 'SAN ANTONIO' then true else false end AS comuna_sanantonio, 
            CASE WHEN comuna_entity = 'SAN BERNARDO' then true else false end AS comuna_sanbernardo, 
            CASE WHEN comuna_entity = 'SAN CARLOS' then true else false end AS comuna_sancarlos, 
            CASE WHEN comuna_entity = 'SAN CLEMENTE' then true else false end AS comuna_sanclemente, 
            CASE WHEN comuna_entity = 'SAN ESTEBAN' then true else false end AS comuna_sanesteban, 
            CASE WHEN comuna_entity = 'SAN FABIAN' then true else false end AS comuna_sanfabian, 
            CASE WHEN comuna_entity = 'SAN FELIPE' then true else false end AS comuna_sanfelipe, 
            CASE WHEN comuna_entity = 'SAN FERNANDO' then true else false end AS comuna_sanfernando, 
            CASE WHEN comuna_entity = 'SAN GREGORIO' then true else false end AS comuna_sangregorio, 
            CASE WHEN comuna_entity = 'SAN IGNACIO' then true else false end AS comuna_sanignacio, 
            CASE WHEN comuna_entity = 'SAN JAVIER' then true else false end AS comuna_sanjavier, 
            CASE WHEN comuna_entity = 'SAN JOAQUIN' then true else false end AS comuna_sanjoaquin, 
            CASE WHEN comuna_entity = 'SAN JOSE DE MAIPO' then true else false end AS comuna_sanjosedemaipo, 
            CASE WHEN comuna_entity = 'SAN JUAN DE LA COSTA' then true else false end AS comuna_sanjuandelacosta, 
            CASE WHEN comuna_entity = 'SAN MIGUEL' then true else false end AS comuna_sanmiguel, 
            CASE WHEN comuna_entity = 'SAN NICOLAS' then true else false end AS comuna_sannicolas, 
            CASE WHEN comuna_entity = 'SAN PABLO' then true else false end AS comuna_sanpablo, 
            CASE WHEN comuna_entity = 'SAN PEDRO' then true else false end AS comuna_sanpedro, 
            CASE WHEN comuna_entity = 'SAN PEDRO DE ATACAMA' then true else false end AS comuna_sanpedrodeatacama, 
            CASE WHEN comuna_entity = 'SAN PEDRO DE LA PAZ' then true else false end AS comuna_sanpedrodelapaz, 
            CASE WHEN comuna_entity = 'SAN RAFAEL' then true else false end AS comuna_sanrafael, 
            CASE WHEN comuna_entity = 'SAN RAMON' then true else false end AS comuna_sanramon, 
            CASE WHEN comuna_entity = 'SAN ROSENDO' then true else false end AS comuna_sanrosendo, 
            CASE WHEN comuna_entity = 'SANTA BARBARA' then true else false end AS comuna_santabarbara, 
            CASE WHEN comuna_entity = 'SANTA CRUZ' then true else false end AS comuna_santacruz, 
            CASE WHEN comuna_entity = 'SANTA JUANA' then true else false end AS comuna_santajuana, 
            CASE WHEN comuna_entity = 'SANTA MARIA' then true else false end AS comuna_santamaria, 
            CASE WHEN comuna_entity = 'SANTIAGO' then true else false end AS comuna_santiago, 
            CASE WHEN comuna_entity = 'SANTO DOMINGO' then true else false end AS comuna_santodomingo, 
            CASE WHEN comuna_entity = 'SAN VICENTE' then true else false end AS comuna_sanvicente, 
            CASE WHEN comuna_entity = 'SIERRA GORDA' then true else false end AS comuna_sierragorda, 
            CASE WHEN comuna_entity = 'TALAGANTE' then true else false end AS comuna_talagante, 
            CASE WHEN comuna_entity = 'TALCA' then true else false end AS comuna_talca, 
            CASE WHEN comuna_entity = 'TALCAHUANO' then true else false end AS comuna_talcahuano, 
            CASE WHEN comuna_entity = 'TALTAL' then true else false end AS comuna_taltal, 
            CASE WHEN comuna_entity = 'TEMUCO' then true else false end AS comuna_temuco, 
            CASE WHEN comuna_entity = 'TENO' then true else false end AS comuna_teno, 
            CASE WHEN comuna_entity = 'TEODORO SCHMIDT' then true else false end AS comuna_teodoroschmidt, 
            CASE WHEN comuna_entity = 'TIERRA AMARILLA' then true else false end AS comuna_tierraamarilla, 
            CASE WHEN comuna_entity = 'TIL-TIL' then true else false end AS comuna_til_til, 
            CASE WHEN comuna_entity = 'TIMAUKEL' then true else false end AS comuna_timaukel, 
            CASE WHEN comuna_entity = 'TIRUA' then true else false end AS comuna_tirua, 
            CASE WHEN comuna_entity = 'TOCOPILLA' then true else false end AS comuna_tocopilla, 
            CASE WHEN comuna_entity = 'TOLTEN' then true else false end AS comuna_tolten, 
            CASE WHEN comuna_entity = 'TOME' then true else false end AS comuna_tome, 
            CASE WHEN comuna_entity = 'TORRES DEL PAINE' then true else false end AS comuna_torresdelpaine, 
            CASE WHEN comuna_entity = 'TORTEL' then true else false end AS comuna_tortel, 
            CASE WHEN comuna_entity = 'TRAIGUEN' then true else false end AS comuna_traiguen, 
            CASE WHEN comuna_entity = 'TREGUACO' then true else false end AS comuna_treguaco, 
            CASE WHEN comuna_entity = 'TUCAPEL' then true else false end AS comuna_tucapel, 
            CASE WHEN comuna_entity = 'VALDIVIA' then true else false end AS comuna_valdivia, 
            CASE WHEN comuna_entity = 'VALLENAR' then true else false end AS comuna_vallenar, 
            CASE WHEN comuna_entity = 'VALPARAISO' then true else false end AS comuna_valparaiso, 
            CASE WHEN comuna_entity = 'VICHUQUEN' then true else false end AS comuna_vichuquen, 
            CASE WHEN comuna_entity = 'VICTORIA' then true else false end AS comuna_victoria, 
            CASE WHEN comuna_entity = 'VICUNA' then true else false end AS comuna_vicuna, 
            CASE WHEN comuna_entity = 'VILCUN' then true else false end AS comuna_vilcun, 
            CASE WHEN comuna_entity = 'VILLA ALEGRE' then true else false end AS comuna_villaalegre, 
            CASE WHEN comuna_entity = 'VILLA ALEMANA' then true else false end AS comuna_villaalemana, 
            CASE WHEN comuna_entity = 'VILLARRICA' then true else false end AS comuna_villarrica, 
            CASE WHEN comuna_entity = 'VINA DEL MAR' then true else false end AS comuna_vinadelmar, 
            CASE WHEN comuna_entity = 'VITACURA' then true else false end AS comuna_vitacura, 
            CASE WHEN comuna_entity = 'YERBAS BUENAS' then true else false end AS comuna_yerbasbuenas, 
            CASE WHEN comuna_entity = 'YUMBEL' then true else false end AS comuna_yumbel, 
            CASE WHEN comuna_entity = 'YUNGAY' then true else false end AS comuna_yungay, 
            CASE WHEN comuna_entity = 'ZAPALLAR' then true else false end AS comuna_zapallar, 
            CASE WHEN comuna_entity IS NULL then true else false end AS comuna_none     
        FROM cleaned.inspections_se;
        
        CREATE INDEX IF NOT EXISTS entity_id_geographic_index on staging.geographic (entity_id_geographic);


        -- creating train table
        DROP TABLE IF EXISTS staging.train_monthly_split_{date_start_string}_{date_end_string};
        CREATE TABLE staging.train_monthly_split_{date_start_string}_{date_end_string}
        AS 
        WITH        

        categoricals  AS                                                                                  	 
            (
                -- create temp table for some categorical variables
                SELECT
                    entity_id AS entity_id_categoricals,
                    date_monthyear AS date_monthyear_categoricals,
                    month_plus_one AS month_plus_one_categoricals,
                    
                    -- offices
                    ( SUM( CASE WHEN codoficina = '1001' then 1 else 0 end ) > 0) AS codoficina_1001,
                    ( SUM( CASE WHEN codoficina = '1002' then 1 else 0 end ) > 0) AS codoficina_1002,
                    ( SUM( CASE WHEN codoficina = '1003' then 1 else 0 end ) > 0) AS codoficina_1003,
                    ( SUM( CASE WHEN codoficina = '1004' then 1 else 0 end ) > 0) AS codoficina_1004,
                    ( SUM( CASE WHEN codoficina = '1005' then 1 else 0 end ) > 0) AS codoficina_1005,
                    ( SUM( CASE WHEN codoficina = '1006' then 1 else 0 end ) > 0) AS codoficina_1006,
                    ( SUM( CASE WHEN codoficina = '1007' then 1 else 0 end ) > 0) AS codoficina_1007,
                    ( SUM( CASE WHEN codoficina = '1008' then 1 else 0 end ) > 0) AS codoficina_1008,
                    ( SUM( CASE WHEN codoficina = '1009' then 1 else 0 end ) > 0) AS codoficina_1009,
                    ( SUM( CASE WHEN codoficina = '101' then 1 else 0 end ) > 0) AS codoficina_101,
                    ( SUM( CASE WHEN codoficina = '1011' then 1  else 0 end ) > 0) AS codoficina_1011,
                    ( SUM( CASE WHEN codoficina = '1012' then 1  else 0 end ) > 0) AS codoficina_1012,
                    ( SUM( CASE WHEN codoficina = '1013' then 1  else 0 end ) > 0) AS codoficina_1013,
                    ( SUM( CASE WHEN codoficina = '1014' then 1  else 0 end ) > 0) AS codoficina_1014,
                    ( SUM( CASE WHEN codoficina = '1015' then 1  else 0 end ) > 0) AS codoficina_1015,
                    ( SUM( CASE WHEN codoficina = '1016' then 1  else 0 end ) > 0) AS codoficina_1016,
                    ( SUM( CASE WHEN codoficina = '1017' then 1  else 0 end ) > 0) AS codoficina_1017,
                    ( sum( case when codoficina = '102' then 1  else 0 end ) > 0) AS codoficina_102,
                    ( sum( case when codoficina = '103' then 1  else 0 end ) > 0) AS codoficina_103,
                    ( sum( case when codoficina = '104' then 1  else 0 end ) > 0) AS codoficina_104,
                    ( SUM( CASE WHEN codoficina = '1101' then 1 else 0 end ) > 0) AS codoficina_1101,
                    ( SUM( CASE WHEN codoficina = '1102' then 1 else 0 end ) > 0) AS codoficina_1102,
                    ( SUM( CASE WHEN codoficina = '1103' then 1 else 0 end ) > 0) AS codoficina_1103,
                    ( SUM( CASE WHEN codoficina = '1104' then 1 else 0 end ) > 0) AS codoficina_1104,
                    ( SUM( CASE WHEN codoficina = '1105' then 1 else 0 end ) > 0) AS codoficina_1105,
                    ( SUM( CASE WHEN codoficina = '1106' then 1 else 0 end ) > 0) AS codoficina_1106,
                    ( SUM( CASE WHEN codoficina = '1201' then 1 else 0 end ) > 0) AS codoficina_1201,
                    ( SUM( CASE WHEN codoficina = '1202' then 1 else 0 end ) > 0) AS codoficina_1202,
                    ( SUM( CASE WHEN codoficina = '1203' then 1 else 0 end ) > 0) AS codoficina_1203,
                    ( SUM( CASE WHEN codoficina = '1300' then 1 else 0 end ) > 0) AS codoficina_1300,
                    ( SUM( CASE WHEN codoficina = '1301' then 1 else 0 end ) > 0) AS codoficina_1301,
                    ( SUM( CASE WHEN codoficina = '1302' then 1 else 0 end ) > 0) AS codoficina_1302,
                    ( SUM( CASE WHEN codoficina = '1303' then 1 else 0 end ) > 0) AS codoficina_1303,
                    ( SUM( CASE WHEN codoficina = '1304' then 1 else 0 end ) > 0) AS codoficina_1304,
                    ( SUM( CASE WHEN codoficina = '1305' then 1 else 0 end ) > 0) AS codoficina_1305,
                    ( SUM( CASE WHEN codoficina = '1306' then 1 else 0 end ) > 0) AS codoficina_1306,
                    ( SUM( CASE WHEN codoficina = '1307' then 1 else 0 end ) > 0) AS codoficina_1307,
                    ( SUM( CASE WHEN codoficina = '1308' then 1 else 0 end ) > 0) AS codoficina_1308,
                    ( SUM( CASE WHEN codoficina = '1309' then 1 else 0 end ) > 0) AS codoficina_1309,
                    ( SUM( CASE WHEN codoficina = '1310' then 1 else 0 end ) > 0) AS codoficina_1310,
                    ( SUM( CASE WHEN codoficina = '1311' then 1 else 0 end ) > 0) AS codoficina_1311,
                    ( SUM( CASE WHEN codoficina = '1312' then 1 else 0 end ) > 0) AS codoficina_1312,
                    ( SUM( CASE WHEN codoficina = '1313' then 1 else 0 end ) > 0) AS codoficina_1313,
                    ( SUM( CASE WHEN codoficina = '1314' then 1 else 0 end ) > 0) AS codoficina_1314,
                    ( SUM( CASE WHEN codoficina = '1315' then 1 else 0 end ) > 0) AS codoficina_1315,
                    ( SUM( CASE WHEN codoficina = '1316' then 1 else 0 end ) > 0) AS codoficina_1316,
                    ( SUM( CASE WHEN codoficina = '1317' then 1 else 0 end ) > 0) AS codoficina_1317,
                    ( SUM( CASE WHEN codoficina = '1319' then 1 else 0 end ) > 0) AS codoficina_1319,
                    ( SUM( CASE WHEN codoficina = '1320' then 1 else 0 end ) > 0) AS codoficina_1320,
                    ( SUM( CASE WHEN codoficina = '1321' then 1 else 0 end ) > 0) AS codoficina_1321,
                    ( SUM( CASE WHEN codoficina = '1322' then 1 else 0 end ) > 0) AS codoficina_1322,
                    ( SUM( CASE WHEN codoficina = '1323' then 1 else 0 end ) > 0) AS codoficina_1323,
                    ( SUM( CASE WHEN codoficina = '1350' then 1 else 0 end ) > 0) AS codoficina_1350,
                    ( SUM( CASE WHEN codoficina = '1360' then 1 else 0 end ) > 0) AS codoficina_1360,
                    ( SUM( CASE WHEN codoficina = '1388' then 1 else 0 end ) > 0) AS codoficina_1388,
                    ( SUM( CASE WHEN codoficina = '1401' then 1 else 0 end ) > 0) AS codoficina_1401,
                    ( SUM( CASE WHEN codoficina = '1402' then 1 else 0 end ) > 0) AS codoficina_1402,
                    ( SUM( CASE WHEN codoficina = '1403' then 1 else 0 end ) > 0) AS codoficina_1403,
                    ( SUM( CASE WHEN codoficina = '1404' then 1 else 0 end ) > 0) AS codoficina_1404,
                    ( SUM( CASE WHEN codoficina = '1405' then 1 else 0 end ) > 0) AS codoficina_1405,
                    ( SUM( CASE WHEN codoficina = '1406' then 1 else 0 end ) > 0) AS codoficina_1406,
                    ( SUM( CASE WHEN codoficina = '1501' then 1 else 0 end ) > 0) AS codoficina_1501,
                    ( SUM( CASE WHEN codoficina = '200' then 1 else 0 end ) > 0) AS codoficina_200,
                    ( SUM( CASE WHEN codoficina = '201' then 1 else 0 end ) > 0) AS codoficina_201,
                    ( SUM( CASE WHEN codoficina = '202' then 1 else 0 end ) > 0) AS codoficina_202,
                    ( SUM( CASE WHEN codoficina = '203' then 1 else 0 end ) > 0) AS codoficina_203,
                    ( SUM( CASE WHEN codoficina = '204' then 1 else 0 end ) > 0) AS codoficina_204,
                    ( SUM( CASE WHEN codoficina = '205' then 1 else 0 end ) > 0) AS codoficina_205,
                    ( SUM( CASE WHEN codoficina = '206' then 1 else 0 end ) > 0) AS codoficina_206,
                    ( SUM( CASE WHEN codoficina = '301' then 1 else 0 end ) > 0) AS codoficina_301,
                    ( SUM( CASE WHEN codoficina = '302' then 1 else 0 end ) > 0) AS codoficina_302,
                    ( SUM( CASE WHEN codoficina = '303' then 1 else 0 end ) > 0) AS codoficina_303,
                    ( SUM( CASE WHEN codoficina = '305' then 1 else 0 end ) > 0) AS codoficina_305,
                    ( SUM( CASE WHEN codoficina = '306' then 1 else 0 end ) > 0) AS codoficina_306,
                    ( SUM( CASE WHEN codoficina = '308' then 1 else 0 end ) > 0) AS codoficina_308,
                    ( SUM( CASE WHEN codoficina = '401' then 1 else 0 end ) > 0) AS codoficina_401,
                    ( SUM( CASE WHEN codoficina = '402' then 1 else 0 end ) > 0) AS codoficina_402,
                    ( SUM( CASE WHEN codoficina = '403' then 1 else 0 end ) > 0) AS codoficina_403,
                    ( SUM( CASE WHEN codoficina = '404' then 1 else 0 end ) > 0) AS codoficina_404,
                    ( SUM( CASE WHEN codoficina = '406' then 1 else 0 end ) > 0) AS codoficina_406,
                    ( SUM( CASE WHEN codoficina = '407' then 1 else 0 end ) > 0) AS codoficina_407,
                    ( SUM( CASE WHEN codoficina = '408' then 1 else 0 end ) > 0) AS codoficina_408,
                    ( SUM( CASE WHEN codoficina = '409' then 1 else 0 end ) > 0) AS codoficina_409,
                    ( SUM( CASE WHEN codoficina = '410' then 1 else 0 end ) > 0) AS codoficina_410,
                    ( SUM( CASE WHEN codoficina = '411' then 1 else 0 end ) > 0) AS codoficina_411,
                    ( SUM( CASE WHEN codoficina = '500' then 1 else 0 end ) > 0) AS codoficina_500,
                    ( SUM( CASE WHEN codoficina = '501' then 1 else 0 end ) > 0) AS codoficina_501,
                    ( SUM( CASE WHEN codoficina = '502' then 1 else 0 end ) > 0) AS codoficina_502,
                    ( SUM( CASE WHEN codoficina = '503' then 1 else 0 end ) > 0) AS codoficina_503,
                    ( SUM( CASE WHEN codoficina = '504' then 1 else 0 end ) > 0) AS codoficina_504,
                    ( SUM( CASE WHEN codoficina = '505' then 1 else 0 end ) > 0) AS codoficina_505,
                    ( SUM( CASE WHEN codoficina = '506' then 1 else 0 end ) > 0) AS codoficina_506,
                    ( SUM( CASE WHEN codoficina = '507' then 1 else 0 end ) > 0) AS codoficina_507,
                    ( SUM( CASE WHEN codoficina = '508' then 1 else 0 end ) > 0) AS codoficina_508,
                    ( SUM( CASE WHEN codoficina = '510' then 1 else 0 end ) > 0) AS codoficina_510,
                    ( SUM( CASE WHEN codoficina = '511' then 1 else 0 end ) > 0) AS codoficina_511,
                    ( SUM( CASE WHEN codoficina = '513' then 1 else 0 end ) > 0) AS codoficina_513,
                    ( SUM( CASE WHEN codoficina = '516' then 1 else 0 end ) > 0) AS codoficina_516,
                    ( SUM( CASE WHEN codoficina = '601' then 1 else 0 end ) > 0) AS codoficina_601,
                    ( SUM( CASE WHEN codoficina = '602' then 1 else 0 end ) > 0) AS codoficina_602,
                    ( SUM( CASE WHEN codoficina = '603' then 1 else 0 end ) > 0) AS codoficina_603,
                    ( SUM( CASE WHEN codoficina = '604' then 1 else 0 end ) > 0) AS codoficina_604,
                    ( SUM( CASE WHEN codoficina = '606' then 1 else 0 end ) > 0) AS codoficina_606,
                    ( SUM( CASE WHEN codoficina = '607' then 1 else 0 end ) > 0) AS codoficina_607,
                    ( SUM( CASE WHEN codoficina = '608' then 1 else 0 end ) > 0) AS codoficina_608,
                    ( SUM( CASE WHEN codoficina = '609' then 1 else 0 end ) > 0) AS codoficina_609,
                    ( SUM( CASE WHEN codoficina = '610' then 1 else 0 end ) > 0) AS codoficina_610,
                    ( SUM( CASE WHEN codoficina = '701' then 1 else 0 end ) > 0) AS codoficina_701,
                    ( SUM( CASE WHEN codoficina = '702' then 1 else 0 end ) > 0) AS codoficina_702,
                    ( SUM( CASE WHEN codoficina = '703' then 1 else 0 end ) > 0) AS codoficina_703,
                    ( SUM( CASE WHEN codoficina = '704' then 1 else 0 end ) > 0) AS codoficina_704,
                    ( SUM( CASE WHEN codoficina = '705' then 1 else 0 end ) > 0) AS codoficina_705,
                    ( SUM( CASE WHEN codoficina = '706' then 1 else 0 end ) > 0) AS codoficina_706,
                    ( SUM( CASE WHEN codoficina = '707' then 1 else 0 end ) > 0) AS codoficina_707,
                    ( SUM( CASE WHEN codoficina = '708' then 1 else 0 end ) > 0) AS codoficina_708,
                    ( SUM( CASE WHEN codoficina = '709' then 1 else 0 end ) > 0) AS codoficina_709,
                    ( SUM( CASE WHEN codoficina = '710' then 1 else 0 end ) > 0) AS codoficina_710,
                    ( SUM( CASE WHEN codoficina = '712' then 1 else 0 end ) > 0) AS codoficina_712,
                    ( SUM( CASE WHEN codoficina = '715' then 1 else 0 end ) > 0) AS codoficina_715,
                    ( SUM( CASE WHEN codoficina = '801' then 1 else 0 end ) > 0) AS codoficina_801,
                    ( SUM( CASE WHEN codoficina = '802' then 1 else 0 end ) > 0) AS codoficina_802,
                    ( SUM( CASE WHEN codoficina = '803' then 1 else 0 end ) > 0) AS codoficina_803,
                    ( SUM( CASE WHEN codoficina = '804' then 1 else 0 end ) > 0) AS codoficina_804,
                    ( SUM( CASE WHEN codoficina = '805' then 1 else 0 end ) > 0) AS codoficina_805,
                    ( SUM( CASE WHEN codoficina = '806' then 1 else 0 end ) > 0) AS codoficina_806,
                    ( SUM( CASE WHEN codoficina = '807' then 1 else 0 end ) > 0) AS codoficina_807,
                    ( SUM( CASE WHEN codoficina = '808' then 1 else 0 end ) > 0) AS codoficina_808,
                    ( SUM( CASE WHEN codoficina = '809' then 1 else 0 end ) > 0) AS codoficina_809,
                    ( SUM( CASE WHEN codoficina = '810' then 1 else 0 end ) > 0) AS codoficina_810,
                    ( SUM( CASE WHEN codoficina = '811' then 1 else 0 end ) > 0) AS codoficina_811,
                    ( SUM( CASE WHEN codoficina = '812' then 1 else 0 end ) > 0) AS codoficina_812,
                    ( SUM( CASE WHEN codoficina = '813' then 1 else 0 end ) > 0) AS codoficina_813,
                    ( SUM( CASE WHEN codoficina = '814' then 1 else 0 end ) > 0) AS codoficina_814,
                    ( SUM( CASE WHEN codoficina = '816' then 1 else 0 end ) > 0) AS codoficina_816,
                    ( SUM( CASE WHEN codoficina = '817' then 1 else 0 end ) > 0) AS codoficina_817,
                    ( SUM( CASE WHEN codoficina = '818' then 1 else 0 end ) > 0) AS codoficina_818,
                    ( SUM( CASE WHEN codoficina = '819' then 1 else 0 end ) > 0) AS codoficina_819,
                    ( SUM( CASE WHEN codoficina = '820' then 1 else 0 end ) > 0) AS codoficina_820,
                    ( SUM( CASE WHEN codoficina = '821' then 1 else 0 end ) > 0) AS codoficina_821,
                    ( SUM( CASE WHEN codoficina = '901' then 1 else 0 end ) > 0) AS codoficina_901,
                    ( SUM( CASE WHEN codoficina = '902' then 1 else 0 end ) > 0) AS codoficina_902,
                    ( SUM( CASE WHEN codoficina = '903' then 1 else 0 end ) > 0) AS codoficina_903,
                    ( SUM( CASE WHEN codoficina = '904' then 1 else 0 end ) > 0) AS codoficina_904,
                    ( SUM( CASE WHEN codoficina = '905' then 1 else 0 end ) > 0) AS codoficina_905,
                    ( SUM( CASE WHEN codoficina = '906' then 1 else 0 end ) > 0) AS codoficina_906,
                    ( SUM( CASE WHEN codoficina = '907' then 1 else 0 end ) > 0) AS codoficina_907,
                    ( SUM( CASE WHEN codoficina = '909' then 1 else 0 end ) > 0) AS codoficina_909,
                    ( SUM( CASE WHEN codoficina = '910' then 1 else 0 end ) > 0) AS codoficina_910,
                    ( SUM( CASE WHEN codoficina = '911' then 1 else 0 end ) > 0) AS codoficina_911

                FROM cleaned.inspections_se
                WHERE date_monthyear < '{date_end}'
                GROUP BY entity_id_categoricals, date_monthyear_categoricals, month_plus_one_categoricals
             )
        ,

	 categoricals_taxes  AS                                                                                  	 
            (
                -- create temp table for some categorical variables from taxes
                SELECT
                    entity_id AS entity_id_taxes,
                    month_year AS month_year_taxes,
                    year_plus_one AS year_plus_one_taxes,
            
                    -- taxes 
                   (sum( CASE WHEN tramoventas = '1' then 1 else 0 end) > 0) AS company_income_level_1,
                   (sum( CASE WHEN tramoventas = '10' then 1 else 0 end) > 0) AS company_income_level_10,
                   (sum( CASE WHEN tramoventas = '11' then 1 else 0 end) > 0) AS company_income_level_11,
                   (sum( CASE WHEN tramoventas = '12' then 1 else 0 end) > 0) AS company_income_level_12,
                   (sum( CASE WHEN tramoventas = '13' then 1 else 0 end) > 0) AS company_income_level_13,
                   (sum( CASE WHEN tramoventas = '2' then 1 else 0 end) > 0) AS company_income_level_2,
                   (sum( CASE WHEN tramoventas = '3' then 1 else 0 end) > 0) AS company_income_level_3,
                   (sum( CASE WHEN tramoventas = '4' then 1 else 0 end) > 0) AS company_income_level_4,
                   (sum( CASE WHEN tramoventas = '5' then 1 else 0 end) > 0) AS company_income_level_5,
                   (sum( CASE WHEN tramoventas = '6' then 1 else 0 end) > 0) AS company_income_level_6,
                   (sum( CASE WHEN tramoventas = '7' then 1 else 0 end) > 0) AS company_income_level_7,
                   (sum( CASE WHEN tramoventas = '8' then 1 else 0 end) > 0) AS company_income_level_8,
                   (sum( CASE WHEN tramoventas = '9' then 1 else 0 end) > 0) AS company_income_level_9
                
                FROM staging.income_level_tax
                WHERE month_year < '{date_end}'
                GROUP BY entity_id, month_year, year_plus_one_taxes
            )
	,
        
        geographic AS 
            (
                -- create temp table geographic to call on later
                SELECT *
                FROM staging.geographic 
            )
        ,

        month_feats AS
            (

                SELECT * 
                FROM staging.month_features
                WHERE month_plus_one <= '{date_end}'
            )
        ,
        
        entities AS
            (
                SELECT 
                   *
                FROM staging.entity_month_year
                where month_year >= '{date_start}' and 
                month_year <= '{date_end}'
             )
        ,

	region AS
	    (
            SELECT
               entity_id as entity_id_region,
                   CASE WHEN region = '1' then true else false end AS region_1,          
                   CASE WHEN region = '2' THEN true else false END AS region_2,                                                 
                   CASE WHEN region = '3' THEN true else false END AS region_3,
                   CASE WHEN region = '4' then true else false end AS region_4,
                   CASE WHEN region = '5' then true else false end AS region_5,                                                 
                   CASE WHEN region = '6' THEN true else false END AS region_6,                                                 
                   CASE WHEN region = '7' THEN true else false END AS region_7,
                   CASE WHEN region = '8' then true else false end AS region_8,
                   CASE WHEN region = '9' then true else false end AS region_9,                                                 
                   CASE WHEN region = '10' THEN true else false END AS region_10,                                               
                   CASE WHEN region = '11' THEN true else false END AS region_11,
                   CASE WHEN region = '12' then true else false end AS region_12,
                   CASE WHEN region = '13' then true else false end AS region_13,                                               
                   CASE WHEN region = '14' THEN true else false END AS region_14,                                               
                   CASE WHEN region = '15' THEN true else false END AS region_15
		
            FROM staging.entity_id
	    )
        ,
        
    inspections AS
        (
        	SELECT 
                distinct date_monthyear::date + cast('1 month' as interval) as month_plus_one_ins, 
                entity_id as entity_id_ins,
   	           	sum(infra) over w as infra_sum, 
                sum(noinfra) over w as noinfra_sum, 
                sum(num_materias) over w as total_matters, 
                count(cast(urgencia as int)) over w as num_urgent, 
    		   	count(idfiscalizacion) over w as num_inspections,
    		   	sum(totalafectados_rec) over w as totalafectados,
    		   	avg(ntrabajadoressii) over w as num_workers_sii,
               	case when sum(cast(urgencia as int)) over w > 0 then 1 else 0 end as urgent
			FROM cleaned.inspections_se 
			WHERE date_monthyear < '{date_end}'
			WINDOW w AS (partition by entity_id, month_plus_one order by date_monthyear)
        ),
        
        
    industries AS 
        (
            SELECT  
                entity_id as entity_id_industries,
                month_year + cast('1 month' as interval) as year_plus_one_industries,
                
                SUM( CASE WHEN industry = 'real_estate' THEN 1 ELSE 0 END ) >0 AS industry_real_estate, 
                SUM( CASE WHEN industry = 'others' THEN 1 ELSE 0 END ) >0 AS industry_others,  
                SUM( CASE WHEN industry = 'public_administration_and_defense' THEN 1 ELSE 0 END ) >0 AS industry_public_administration_and_defense,  
                SUM( CASE WHEN industry = 'agriculture_livestock_hunting_and_forestry' THEN 1 ELSE 0 END ) >0 AS industry_agriculture_livestock_hunting_and_forestry,  
                SUM( CASE WHEN industry = 'commerce' THEN 1 ELSE 0 END ) >0 AS industry_commerce,  
                SUM( CASE WHEN industry = 'construction' THEN 1 ELSE 0 END ) >0 AS industry_construction,  
                SUM( CASE WHEN industry = 'electricity_gas_ and_water' THEN 1 ELSE 0 END ) >0 AS industry_electricity_gas_and_water,  
                SUM( CASE WHEN industry = 'education' THEN 1 ELSE 0 END ) >0 AS industry_education, 
                SUM( CASE WHEN industry = 'financial_establishments_insurance,_real estate_and_services' THEN 1 ELSE 0 END ) >0 AS industry_financial_establishments_insurance_realestate_and_serv, 
                SUM( CASE WHEN industry = 'exploitation_mines_and_quarries' THEN 1 ELSE 0 END ) >0 AS industry_exploitation_mines_and_quarries,  
                SUM( CASE WHEN industry = 'private_homes_with_domestic_service' THEN 1 ELSE 0 END ) >0 AS industry_private_homes_with_domestic_service,  
                SUM( CASE WHEN industry = 'hotels_and_restaurants' THEN 1 ELSE 0 END ) >0 AS industry_hotels_and_restaurants,  
                SUM( CASE WHEN industry = 'manufacturing_industries' THEN 1 ELSE 0 END ) >0 AS industry_manufacturing_industries,  
                SUM( CASE WHEN industry = 'financial_intermediation' THEN 1 ELSE 0 END ) >0 AS industry_financial_intermediation,  
                SUM( CASE WHEN industry = 'organizations_and_extraterritorial_bodies' THEN 1 ELSE 0 END ) >0 AS industry_organizations_and_extraterritorial_bodies,  
                SUM( CASE WHEN industry = 'other_community_service_activities' THEN 1 ELSE 0 END ) >0 AS industry_other_community_service_activities,  
                SUM( CASE WHEN industry = 'fishing' THEN 1 ELSE 0 END ) >0 AS industry_fishing,  
                SUM( CASE WHEN industry = 'state_social_services' THEN 1 ELSE 0 END ) >0 AS industry_state_social_services,  
                SUM( CASE WHEN industry = 'other_social_and_health_services' THEN 1 ELSE 0 END ) >0 AS industry_other_social_and_health_services,  
                SUM( CASE WHEN industry = 'electricity_gas_and_water_supply' THEN 1 ELSE 0 END ) >0 AS industry_electricity_gas_and_water_supply,  
                SUM( CASE WHEN industry = 'transportation_storage_and_communications' THEN 1 ELSE 0 END ) >0 AS industry_transportation_storage_and_communications,  
                SUM( CASE WHEN industry = 'real_state_business_and_rental' THEN 1 ELSE 0 END ) >0 AS industry_real_state_business_and_rental,  
                SUM( CASE WHEN industry = 'none' THEN 1 ELSE 0 END ) >0 AS industry_none,  
                SUM( CASE WHEN industry = 'administration_council_of_buildings' THEN 1 ELSE 0 END ) >0 AS industry_administration_council_of_buildings,  
                SUM( CASE WHEN industry = 'social_and_health_services' THEN 1 ELSE 0 END ) >0 AS industry_social_and_health_services,
                 SUM( CASE WHEN industry IS NULL THEN 1 ELSE 0 END ) >0 AS industry_noinfo
             
			FROM staging.industry_month_year
            WHERE month_year < '{date_end}'
            GROUP BY entity_id, month_year, year_plus_one_industries
        
        
        )
        
        
        --- joining tables
        SELECT 
            a.*,
            f.*,
            g.*,
            c.*,
            r.*,
            t.*,
            i.*,
            n.*,

            -- holidays
            CASE WHEN extract(month from a.month_year) IN (12, 1) then true else false end AS winter_holiday,
            CASE WHEN extract(month from a.month_year) IN (7, 8) then true else false end AS summer_holiday,
            
            -- random feature
            round(random()) AS random_feature
        
        FROM entities a 
            
        LEFT JOIN month_feats f
        ON a.month_year = f.month_plus_one
        
        LEFT JOIN categoricals c
        ON a.month_year = c.month_plus_one_categoricals
        AND a.entity_id = c.entity_id_categoricals
        
        LEFT JOIN geographic g
        ON a.entity_id = g.entity_id_geographic
        
        LEFT JOIN categoricals_taxes t
        ON DATE_TRUNC('year', a.month_year) = t.year_plus_one_taxes
        AND a.entity_id = t.entity_id_taxes 
	
        LEFT JOIN region r
        ON a.entity_id = r.entity_id_region
        
        LEFT JOIN inspections i
        ON a.entity_id = i.entity_id_ins
        AND a.month_year = i.month_plus_one_ins
        
        LEFT JOIN industries n
        ON a.entity_id = n.entity_id_industries
        AND a.month_year = n.year_plus_one_industries;

    CREATE INDEX IF NOT EXISTS month_year_{date_start_string}_{date_end_string}_index on staging.train_monthly_split_{date_start_string}_{date_end_string} (month_year);

    CREATE INDEX IF NOT EXISTS month_year_entity_{date_start_string}_{date_end_string}_index on staging.train_monthly_split_{date_start_string}_{date_end_string} (month_year, entity_id);

     """.format(date_start = date_start, 
                  date_end = date_end, 
                  date_start_string = date_start_string, 
                  date_end_string = date_end_string)
    plsql.query_no_return(create_query_train, engine)
