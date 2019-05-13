SET ROLE direccion_trabajo_inspections_write;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.inspections_sample;

CREATE TABLE raw.inspections_sample (
	idfiscalizacion VARCHAR, 
	codoficina VARCHAR, 
	agno DECIMAL, 
	nrocomision VARCHAR, 
	totalafectados DECIMAL, 
	urgencia BOOLEAN, 
	solesafectado BOOLEAN, 
	codtiposol VARCHAR, 
	solicitante VARCHAR, 
	codunidadorigen VARCHAR, 
	unidadorigen VARCHAR, 
	codtipotermino VARCHAR, 
	tipotermino VARCHAR, 
	egresoconmulta BOOLEAN, 
	empdfcodcomuna VARCHAR, 
	empdmcodcomuna VARCHAR, 
	emptrabhombres DECIMAL, 
	codcae VARCHAR, 
	codtipoempresa VARCHAR, 
	grupocodtipomaterias VARCHAR, 
	grupoglosatipomaterias VARCHAR, 
	grupoglosainfra VARCHAR, 
	grupocodigoinfra VARCHAR, 
	grupoglosainfra_det VARCHAR, 
	grupocodigoinfra_det VARCHAR, 
	grupocodtipomaterias2 VARCHAR, 
	grupoglosatipomaterias2 VARCHAR, 
	grupoglosainfra2 VARCHAR, 
	grupoglosainfra2_det VARCHAR, 
	grupocodigoinfra2 VARCHAR, 
	grupocodigoinfra2_det VARCHAR, 
	grupocodigonormainfra2_det VARCHAR, 
	ccae VARCHAR, 
	gcae VARCHAR, 
	crae VARCHAR, 
	grae VARCHAR, 
	noinfra DECIMAL, 
	infra DECIMAL, 
	derechofund BOOLEAN, 
	num_materias DECIMAL, 
	num_sind DECIMAL, 
	region VARCHAR, 
	infractor BOOLEAN, 
	exsind BOOLEAN, 
	datereg DATE, 
	mesreg DECIMAL, 
	rutempresamask VARCHAR
);

\COPY raw.inspections_sample from '/mnt/data/projects/direccion_trabajo_inspections/preprocessing/inspections_sample.csv' WITH DELIMITER ';' NULL '.' CSV HEADER;
