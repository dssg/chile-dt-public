SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.social;

CREATE TABLE raw.social (
	registro DECIMAL, 
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

\COPY raw.social from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/OneDrive_1_6-28-2018/social_mod2.csv' WITH DELIMITER ';' NULL '.' CSV HEADER;
