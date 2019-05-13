SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.violations;

CREATE TABLE raw.violations AS
SELECT idfiscalizacion, datereg, 
unnest(string_to_array(grupocodigoinfra2, ',')) as violation_code
FROM raw.inspections_complete;
