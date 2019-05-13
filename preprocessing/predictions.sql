SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.predictions;

CREATE TABLE raw.predictions (
	n DECIMAL NOT NULL,
	crae DECIMAL NOT NULL,
	region DECIMAL NOT NULL,
	agno_01 DECIMAL NOT NULL,
	inf_06 DECIMAL NOT NULL,
	agno_02 DECIMAL NOT NULL,
	inf_07 DECIMAL NOT NULL,
	agno_03 DECIMAL NOT NULL,
	inf_08 DECIMAL NOT NULL,
	agno_04 DECIMAL NOT NULL,
	inf_09 DECIMAL NOT NULL,
        agno_05 DECIMAL NOT NULL,
        inf_10 DECIMAL NOT NULL,
        agno_06 DECIMAL NOT NULL,
        inf_11 DECIMAL NOT NULL,
        agno_07 DECIMAL NOT NULL,
        inf_12 DECIMAL NOT NULL,
        agno_08 DECIMAL NOT NULL,
        inf_13 DECIMAL NOT NULL,
        agno_09 DECIMAL NOT NULL,
        inf_14 DECIMAL NOT NULL,
        fiscmean VARCHAR NOT NULL,
        infsd VARCHAR NOT NULL,
        recency DECIMAL NOT NULL,
        infrac VARCHAR NOT NULL,
        infractor BOOLEAN NOT NULL,
        resrandomforest BOOLEAN NOT NULL,
        rutempresamask VARCHAR NOT NULL
);

\COPY raw.predictions from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/OneDrive_1_7-16-2018/datafinaluchicago_baseconpredicciones.csv' WITH DELIMITER E';' CSV HEADER;
