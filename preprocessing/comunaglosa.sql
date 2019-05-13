SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.comunaglosa;

CREATE TABLE raw.comunaglosa (
	codigo DECIMAL NOT NULL, 
	glosa VARCHAR NOT NULL, 
	codigotgr DECIMAL, 
	glosatgr VARCHAR, 
	codigo_subdere DECIMAL, 
	glosa_subdere VARCHAR, 
	activo BOOLEAN, 
	codigo_sf2000 DECIMAL, 
	codprovincia DECIMAL, 
	region DECIMAL
);

\COPY raw.comunaglosa from 'comunaglosa.csv' WITH CSV HEADER;
