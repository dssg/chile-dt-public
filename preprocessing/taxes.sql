SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.taxes;

CREATE TABLE raw.taxes (
        agno DECIMAL, 
        dv VARCHAR, 
        tramoventas VARCHAR, 
        ntrabajadores DECIMAL, 
        rubro VARCHAR, 
        subrubro VARCHAR, 
        actividadeconomica VARCHAR, 
        calle VARCHAR, 
        numero VARCHAR, 
        bloque VARCHAR, 
        depto VARCHAR, 
        villapoblacion VARCHAR, 
        comuna VARCHAR, 
        region VARCHAR, 
        fechainicio VARCHAR, 
        fechatermino VARCHAR, 
        tipotermino VARCHAR, 
        tipocontribuyente VARCHAR, 
        subtipocontribuyente VARCHAR, 
        f22c645 VARCHAR, 
        f22c646 VARCHAR, 
        rutmask VARCHAR, 
        razonsocialmask VARCHAR	
);

\COPY raw.taxes from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/taxes_mod.csv' WITH DELIMITER ';' NULL AS '.' CSV HEADER;
