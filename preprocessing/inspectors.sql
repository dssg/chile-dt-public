SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.inspectors;

CREATE TABLE raw.inspectors (
	idfiscalizacion VARCHAR,
	funasignadomask VARCHAR
);

\COPY raw.inspectors from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/inspectors.csv' WITH DELIMITER ';' NULL '.' CSV HEADER;
