SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.metadata;

CREATE TABLE raw.metadata (
	id DECIMAL, 
	variable_name VARCHAR, 
	description_spanish VARCHAR, 
	r_data_type VARCHAR, 
	description_english VARCHAR
);

\COPY raw.metadata from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/metadatos.csv' WITH CSV HEADER
