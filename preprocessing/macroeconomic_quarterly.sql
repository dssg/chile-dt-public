SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.macroeconomic_quarterly;

CREATE TABLE raw.macroeconomic_quarterly (
	date DATE NOT NULL, 
	gdp_current_price DECIMAL NOT NULL, 
	gdp_constant_price DECIMAL NOT NULL, 
	total_mining_production DECIMAL NOT NULL
);

\COPY raw.macroeconomic_quarterly from '/mnt/data/projects/direccion_trabajo_inspections/macroeconomic_data_quarterly.csv' WITH DELIMITER E'\t' CSV HEADER;


