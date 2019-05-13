SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.office_data;

CREATE TABLE raw.office_data (
	codeoffice VARCHAR NOT NULL, 
	name VARCHAR, 
	adress VARCHAR, 
	comuna VARCHAR, 
	region DECIMAL, 
	number_of_inspectors DECIMAL NOT NULL
);

\COPY raw.office_data from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/Oficinas_DT.csv' WITH CSV HEADER
