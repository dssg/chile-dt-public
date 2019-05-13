set role direccion_trabajo_inspections_write;

set role direccion_trabajo_inspections_write;

create schema if not exists semantic;

drop table if exists semantic.companies;

create table semantic.companies as (
    select
        "año" as year,
        rut as company,
        dv,
        fecha_inicio as start_date,
        fecha_termino_giro as end_date,
        tipo_termino_giro as reason_for_termination,
        razon_social as name,
        tramo_ventas,
        actividad_economica,
        actividad_economica_description,
        rubro,
        rubro_description,
        subrubro,
        subrubro_description,
        tipo_contribuyente,
        subtipo_contribuyente,
        bloque,
        calle,
        numero,
        depto,
        villa_poblacion,
        comuna,
        region,
        ciudad,
        num_trabajadores,
        f22_c_645,
        f22_c_646
    from taxes.companies
);

create index concurrently companies_company_ix on semantic.companies(company desc nulls last) ;
create index concurrently companies_year_ix on semantic.companies(year asc);
create index concurrently companies_company_year_ix on semantic.companies(company, year);
--create index concurrently companies_life_period_ix on semantic.companies using gist (daterange(start_date, end_date, '[]'));
--create index concurrently companies_company_life_period_ix on semantic.companies using gist (company, daterange(start_date, end_date, '[]'));

set role direccion_trabajo_inspections_write;

drop table if exists semantic.facilities cascade;

create table semantic.facilities (
        facility serial,
        address text,
        comuna smallint,
        company integer,
        company_dv varchar(1),
        company_name text,
        company_address text,
        company_comuna integer,
        company_legal_contact integer,
        company_legal_contact_dv varchar(1),
        company_legal_contact_name text
);

insert into semantic.facilities
    (address, comuna,
        company, company_dv, company_name, company_address, company_comuna,
        company_legal_contact, company_legal_contact_dv, company_legal_contact_name)
select distinct on (rutempresa, empdfcodcomuna, empdfdireccion) -- We assume that this combination defines a facility
    btrim(lower(empdfdireccion)) as address,
    empdfcodcomuna::integer as comuna,
    rutempresa::integer as company,
    nullif(btrim(lower(dvempresa)), '') as company_dv,
    nullif(btrim(lower(razonsocialempresa)), '') as company_name,
    btrim(lower(empdmdireccion)) as company_address,
    empdmcodcomuna::integer as company_comuna,
    nullif(replrut::integer,0) as company_legal_contact,
    nullif(btrim(lower(repldv)), '') as company_legal_contact_dv,
    nullif(btrim(lower(replnombres)),'') as company_legal_contact_name
from
    raw.dt_fi_informefiscalizacion as informe
order by
    rutempresa,
    empdfcodcomuna,
    empdfdireccion,
    fechainforme asc;

create index concurrently facilities_company_ix on semantic.facilities(company);

set role direccion_trabajo_inspections_write;

drop table if exists semantic.workers;

create table semantic.workers as (
    select
        solrut::integer as worker,
        lower(btrim(soldv))::varchar(1) as dv,
        lower(btrim(solnombres)) as name,
        lower(format('%s %s', btrim(solpaterno), btrim(solmaterno))) as last_name,
        lower(btrim(solsexo))::varchar as sex,
        case when is_valid_date(solfechanac::varchar) then solfechanac::date end as bod,
        lower(btrim(solfuncion))::varchar as funcion,
        solcodsalud::integer as codsalud,
        solcodafp::integer as codafp,
        btrim(solrsu)::varchar as rsu,
        btrim(solnomorgsindical)::varchar as nomorgsindical,
        btrim(soldireccion) as address,
        btrim(solfono) as phone,
        nullif(btrim(solcodcomuna),'')::integer as comuna,
        codnacionalidad::smallint,
        lower(btrim(nacionalidades.glosa)) as nacionalidad
    from
        raw.dt_fi_ingreso_fiscalizacion as ingreso
        left join
        raw.dt_fi_tiponacionalidades as nacionalidades on ingreso.codnacionalidad::smallint = nacionalidades.codigo::smallint

);

comment on table semantic.workers is 'some inspections are triggered by a worker';
create index concurrently workers_worker_ix on semantic.workers(worker);
create index concurrently workers_worker_name_ix on semantic.workers(worker, name, last_name);

set role direccion_trabajo_inspections_write;

drop table if exists semantic.inspectors;

create table semantic.inspectors as (
    select
        split_part("rut",'-',1) as rut,
        split_part("rut",'-',2) as div,
        "fecha nacimiento" as dob,
        lower(substring(btrim("sexo") from 1 for 1)) as gender,
        lower(btrim("región")) as region,
        "cod. dependencia" as dt_office,
        "cod. departamento" as department_code,
        "cod. unidad" as working_unit_code,
        lower(btrim("unidad")) as working_unit,
        lower(btrim("jefe directo")) as supervisor,
        "fec.en el grado" as last_promotion ,
        lower(btrim("area relacionada con la funcion principal")) as work_description,
        lower(btrim("función principal")) as function_description,
        lower(btrim("profesión")) as major ,
        lower(btrim("estudios"))  as title,
        case when lower(btrim("profesional 8 semestres")) = 'si' then True else False end as university_studies,
        "fec.ing. servicio acumulada" as start_date_service,
        "fec.ing. adm.púb." as  start_date_public_administration,
        "fec.en la planta (estamento)" as start_date_estamento,
        "fec.en el grado (escalafón)" as start_date_grado,
        "fec.en el cargo (escalafón)" as start_date_cargo ,
        "fecha ingreso servicio (ultimo ingreso)" as last_starting_date ,
        lower(btrim("estamento gt")) as estamento_gt,
        "grado gt" as grado_gt ,
        "calidad juridica ejece" as hire_status ,
        "lm totales" as medical_licenses
    from raw.inspectors

);

set role direccion_trabajo_inspections_write;

drop table if exists semantic.dt_offices;

create table semantic.dt_offices as (

    select
    codeoffice::integer as dt_office,
    lower(btrim(name)) as name,
    lower(btrim(adress)) as address,
    lower(btrim(comuna)) as comuna,
    lower(btrim(region::varchar)) as region,
    number_of_inspectors::smallint
    from raw.office_data
    where is_valid_integer(codeoffice) is true
);

 comment on table semantic.dt_offices is 'DT offices';
 create index concurrently dt_offices_dt_office_ix on semantic.dt_offices(dt_office);

drop table if exists semantic.events;

create table semantic.events as (
     select
         idfiscalizacion::integer as event,
         codestadofis::smallint as status,
         nullif(lower(btrim(estado.glosa)), '') as status_description,
         ingreso.funasignado::integer as inspector,
         informe.rutempresa::integer as company,
         facilities.facility as facility,
         informe.fechainiciovisita::date as inspection_start_date, -- , regexp_replace(informe.horainiciovisita, '\.|-', ':'))), '') as inspection_start_date, -- need to fix this to timestamp
         informe.fechaterminovisita::date as inspection_end_date, -- regexp_replace(informe.horaterminovisita, '\.|-', ':'))), '') as inspection_end_date,
         ingreso.codorigenfis::smallint as tipo_origen,
         nullif(lower(btrim(tipo_origen.glosa)),'') as tipo_origen_description,
         ingreso.codunidadorigen::smallint as unidad_origen,
         nullif(lower(btrim(unidad_origen.glosa)), '') as unidad_origen_description,
         ingreso.codtiposol::smallint as solicitada_por,
         nullif(lower(btrim(solicitada_por.glosa)), '')  as solicitada_por_description,

         informe.codtipotermino::smallint as tipo_termino, -- DT: They recommend remove 12 (sin trámite), 13 (desisted), 14 (no located), 19 (check of fundamental rights) ,20 (subsumida en) ,21 (deleted by mistake) ,22 (derivad a mediacion)  and 23 (eliminada por error en el ingreso)
         nullif(lower(btrim(tipo_termino.glosa)), '') as tipo_termino_description,
         informe.egresoconmulta::boolean,

         case when informe.codcae::smallint not in (0,1,-1)
         then informe.codcae::smallint
         end as inspected_cae,
         nullif(lower(btrim(cae.glosa)),'') as inspected_cae_description,
         informe.codtipoempresa::smallint as inspected_tipoempresa,

         informe.emptrabhombres::integer as inspected_estimated_number_of_workers,
         informe.insphombresinv::integer,
         informe.inspmujeresinv::integer,
         informe.inspmenohominv::integer,
         informe.inspmenmujinv::integer,
         informe.insphombresext::smallint,
         informe.inspmujeresext::smallint,
         informe.nrotrablugarfisc::integer,

         nrocomision::integer,   -- !!!
         case when is_valid_date(fechaorigen::varchar) then fechaorigen::date end as fechaorigen,
         ponderacion::real as difficulty,
         nullif(btrim(kardex), 'NA')::varchar as kardex,
         ingreso.codoficina::integer as dt_office,
         dt_offices.address as dt_office_address,
         dt_offices.comuna as dt_office_comuna,
         dt_offices.region as dt_office_region,

         -- Data from the requester
         ingreso.solrut::integer as requester,
         ingreso.emprut::integer as company_according_requester,
         btrim(ingreso.empdv)::varchar(1) as dv_according_requester,
         ingreso.empcodcomuna::integer as comuna_according_requester,
         ingreso.emprae::integer as rae_according_requester,
         nullif(btrim(lower(ingreso.empfono::varchar)), '') as phone_according_requester,
         nullif(btrim(lower(ingreso.empemail::varchar)), '') as email_according_requester,
         nullif(lower(btrim(ingreso.empacercam::varchar)), '') as references_according_requester,
         codtipodoc::smallint as cod_tipo_doc,
         nullif(lower(btrim(tipo_documento.glosa)),'') as tipo_doc,
         btrim(iddocumento)::varchar as iddocumento,

         urgencia::boolean,

         nullif(lower(btrim(descdenuncia)),'')::text as descdenuncia,

         ingreso.diavisita1::smallint,
         to_char(to_timestamp(nullif(nullif(btrim(ingreso.horavisita1), ''), ':'), 'HH24:MI'), 'HH24:MI') as horavisita1,
         ingreso.diavisita2::smallint,
         to_char(to_timestamp(nullif(nullif(btrim(ingreso.horavisita2), ''), ':'), 'HH24:MI'), 'HH24:MI') as horavisita2,

         ingreso.nrotrabempresa::integer as number_of_workers_according_requester,   -- estimated by the requester
         ingreso.solesafectado::boolean,

         btrim(ingreso.hayotros)::boolean as hayotros,
         nullif(ingreso.totalafectados, 9999)::smallint as totalafectados,  -- estimated by the requester

         -- Data in the assignation
         case when is_valid_date(fechaasignacion::varchar) then fechaasignacion::date end as assignment_date,
         refiscalizacom::integer,
         btrim(comespecial)::varchar as comespecial,
         btrim(comordinaria)::varchar as comordinaria,
         btrim(comextraord)::varchar as comextraord,
         mesasignacion::smallint, -- Some time is not the same that extract(year from fechaasignacion)
         agnoasignacion::smallint,


         diasjornada::smallint,
         btrim(horasjornadas)::smallint as horasjornadas,

         idconfigregional::integer,

         ingreso.rutfunregistro::integer as request_registry_official,
         case when is_valid_date(fecharegistro::varchar) then fecharegistro::date end as request_registry_date,
         ingreso.ictfunregistro::integer,
         ingreso.empcodtipoemptam::smallint as company_estimated_size, --

         case when is_valid_date(informe.fechainforme::varchar) then fechainforme::date end as report_date,

         -- Removing newlines (including the new unicode definitions)
         regexp_replace(nullif(lower(btrim(informe.obsevinspeccion)), ''), E'[\n\r\f\u000B\u0085\u2028\u2029]+', ' ', 'g') as inspector_observations,

         informe.oficiootrasinst::boolean,
         nullif(lower(btrim(informe.institucionainformar)), '') as institucion_a_informar,

         informe.periodorevdesde::date,
         informe.periodorevhasta::date,
         informe.codorgadmin16744::smallint,

         informe.funrutreg::integer as report_registry_official,
         informe.fechareg::date as report_registry_date,
         informe.codinfdentroplazo::smallint,
         informe.codtpoprocedimiento::smallint
     from
         raw.dt_fi_informefiscalizacion as informe
         left join
         raw.dt_fi_ingreso_fiscalizacion as ingreso using (idfiscalizacion)
         left join
         raw.dt_fi_tipoorigenact as tipo_origen on tipo_origen.codigo = ingreso.codorigenfis
         left join
         raw.dt_fi_unidadorigen as unidad_origen on unidad_origen.codigo = ingreso.codunidadorigen
         left join
         raw.dt_fi_solicitadapor as solicitada_por on solicitada_por.codigo = ingreso.codtiposol
         left join
         raw.dt_fi_tipoterminofiscalizacion as tipo_termino on tipo_termino.codigo = informe.codtipotermino
         left join
         semantic.dt_offices as dt_offices on dt_offices.dt_office = ingreso.codoficina
         left join
         raw.dt_fi_estadofiscalizacion as estado on ingreso.codestadofis = estado.codigo
         left join
         raw.cae as cae on informe.codcae::smallint = cae.codigo::smallint
         left join
         raw.dt_fi_tipo_documento as tipo_documento on tipo_documento.codigo = ingreso.codtipodoc
         left join
         semantic.facilities as facilities on facilities.address = btrim(lower(informe.empdfdireccion)) and facilities.comuna = informe.empdfcodcomuna and facilities.company = informe.rutempresa
 );

 comment on table semantic.events is 'each row is an inspection that happened in a facility';
 comment on column semantic.events.difficulty is 'Valor que se asigna a la inspección realizada por el funcionario (inspector), a partir de las materias denunciadas.';

 create index concurrently events_event_ix on semantic.events(event);
 create index concurrently events_company_ix on semantic.events(company);
 create index concurrently events_facility_ix on semantic.events(facility);
 create index concurrently events_inspection_start_date_ix on semantic.events(inspection_start_date);
 create index concurrently events_inspection_end_date_ix on semantic.events(inspection_end_date);
 create index concurrently events_inspection_date_range_ix on semantic.events using gist (daterange(inspection_start_date, inspection_end_date, '[]'));
 create index concurrently events_inspector_ix on semantic.events(inspector);
 create index concurrently events_requester_ix on semantic.events(requester);
 create index concurrently events_status_ix on semantic.events(status);
 create index concurrently events_tipo_termino_ix on semantic.events(tipo_termino);
