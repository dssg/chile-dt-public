SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.copper;

CREATE TABLE raw.copper (
	date DATE NOT NULL, 
	value DECIMAL NOT NULL
);

\COPY raw.copper from '/mnt/data/projects/direccion_trabajo_inspections/copper_price.csv' WITH CSV HEADER;
