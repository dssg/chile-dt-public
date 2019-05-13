SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.social_new;

CREATE TABLE raw.social_new (
	administradora DECIMAL, 
	dvempleador VARCHAR, 
	domicilioempleador VARCHAR, 
	comunaempleador DECIMAL, 
	dvafiliado VARCHAR, 
	cotizacion DECIMAL, 
	periodo_devengado DECIMAL, 
	mtocotizacion DECIMAL, 
	fechapago DECIMAL, 
	causalaclaracion DECIMAL, 
	fechainicio DATE, 
	fechatermino DATE, 
	afppago DECIMAL, 
	teleoperador DECIMAL, 
	mca_rebajadoafp VARCHAR, 
	fecharebajadoafp DATE, 
	mca_regularizado VARCHAR, 
	fecharegularizado DATE, 
	rutempleadormask VARCHAR, 
	razonsocialmask VARCHAR, 
	emailempleadormask VARCHAR, 
	rutafiliadomask VARCHAR, 
	nombredelafiliadomask VARCHAR
);

\COPY raw.social_new from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/OneDrive_1_7-9-2018/social_new.csv' WITH DELIMITER ';' NULL '.' CSV HEADER;
