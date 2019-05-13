SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.macroeconomic_monthly;

CREATE TABLE raw.macroeconomic_monthly (
	date DATE NOT NULL, 
	exchange_rate DECIMAL NOT NULL, 
	cpi DECIMAL NOT NULL, 
	civilian_labor_force DECIMAL, 
	total_retail_trade DECIMAL NOT NULL, 
	active_population DECIMAL NOT NULL, 
	employed_population DECIMAL NOT NULL, 
	inactive_population DECIMAL NOT NULL, 
	population DECIMAL NOT NULL, 
	unemployed_population DECIMAL NOT NULL, 
	working_age_population DECIMAL NOT NULL, 
	employment_rate DECIMAL NOT NULL, 
	inactivity_rate DECIMAL NOT NULL, 
	unemployment_rate DECIMAL NOT NULL
);

\COPY raw.macroeconomic_monthly from '/mnt/data/projects/direccion_trabajo_inspections/macroeconomic_data_monthly.csv' WITH DELIMITER E'\t' CSV HEADER;
