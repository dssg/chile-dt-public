SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.cae;

CREATE TABLE raw.cae (
        codigo DECIMAL NOT NULL,
        glosa VARCHAR NOT NULL,
        rae_cod DECIMAL NOT NULL
);

\COPY raw.cae from '/mnt/data/projects/direccion_trabajo_inspections/onedrive_6-26-2018/CAE.csv' WITH CSV HEADER;
