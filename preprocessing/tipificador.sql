SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.tipificador;

CREATE TABLE raw.tipificador (
        normasan DECIMAL, 
        codigo VARCHAR, 
        nl_infringida VARCHAR, 
        nl_sancionada VARCHAR, 
        cuerpolegal VARCHAR, 
        desde DECIMAL, 
        hasta DECIMAL, 
        codgravedad DECIMAL, 
        tipogravedad VARCHAR, 
        statement VARCHAR, 
        hecho VARCHAR, 
        codtipomoneda DECIMAL, 
        tipomoneda VARCHAR, 
        junji DECIMAL, 
        capitulo DECIMAL, 
        codconcepto VARCHAR, 
        fechainicio DECIMAL, 
        fechatermino DECIMAL, 
        enunciadohecho BOOLEAN, 
        caracteresenunciadomax300 VARCHAR, 
        caractereshechomax2000 VARCHAR
);

\COPY raw.tipificador from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/tipificador_mod.csv' WITH DELIMITER ',' CSV HEADER;

