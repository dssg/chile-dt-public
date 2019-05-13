create table raw.dt_fi_auditasignacion (like public.dt_fi_auditasignacion including all);
create table raw.dt_fi_auditfiscalizacion (like public.dt_fi_auditfiscalizacion including all);
create table raw.dt_fi_detallemateria (like public.dt_fi_detallemateria including all);
create table raw.dt_fi_detallemateriaturno (like public.dt_fi_detallemateriaturno including all);
create table raw.dt_fi_materiafiscalizar (like public.dt_fi_materiafiscalizar including all);
create table raw.dt_fi_otrosafectados (like public.dt_fi_otrosafectados including all);
create table raw.dt_fi_solicitadapor (like public.dt_fi_solicitadapor including all);
create table raw.dt_fi_tipo_documento (like public.dt_fi_tipo_documento including all);
create table raw.dt_fi_tipodocumento (like public.dt_fi_tipodocumento including all);
create table raw.dt_fi_tipomateria (like public.dt_fi_tipomateria including all);
create table raw.dt_fi_tiponacionalidades (like public.dt_fi_tiponacionalidades including all);
create table raw.dt_fi_tipoorigenact (like public.dt_fi_tipoorigenact including all);
create table raw.dt_fi_tipoterminofiscalizacion (like public.dt_fi_tipoterminofiscalizacion including all);
create table raw.dt_fi_unidadorigen (like public.dt_fi_unidadorigen including all);
create table raw.dt_mul_capitulonormasan (like public.dt_mul_capitulonormasan including all);
create table raw.dt_mul_categnorma (like public.dt_mul_categnorma including all);
create table raw.dt_mul_conceptonormasan (like public.dt_mul_conceptonormasan including all);
create table raw.dt_mul_detallenormasan (like public.dt_mul_detallenormasan including all);
create table raw.dt_mul_tipocategorias (like public.dt_mul_tipocategorias including all);
create table raw.dt_mul_tipocategoriasturno (like public.dt_mul_tipocategoriasturno including all);
create table raw.dt_fi_estadofiscalizacion (like public.dt_fi_estadofiscalizacion including all);

insert into raw.dt_fi_auditasignacion
select *
from public.dt_fi_auditasignacion;

insert into raw.dt_fi_auditfiscalizacion
select *
from public.dt_fi_auditfiscalizacion;

insert into raw.dt_fi_detallemateria
select *
from public.dt_fi_detallemateria;

insert into raw.dt_fi_detallemateriaturno
select *
from public.dt_fi_detallemateriaturno;

insert into raw.dt_fi_materiafiscalizar
select *
from public.dt_fi_materiafiscalizar;

insert into raw.dt_fi_otrosafectados
select *
from public.dt_fi_otrosafectados;

insert into raw.dt_fi_solicitadapor
select *
from public.dt_fi_solicitadapor;

insert into raw.dt_fi_tipo_documento
select *
from public.dt_fi_tipo_documento;

insert into raw.dt_fi_tipodocumento
select *
from public.dt_fi_tipodocumento;

insert into raw.dt_fi_tipomateria
select *
from public.dt_fi_tipomateria;

insert into raw.dt_fi_tiponacionalidades
select *
from public.dt_fi_tiponacionalidades;

insert into raw.dt_fi_tipoorigenact
select *
from public.dt_fi_tipoorigenact;

insert into raw.dt_fi_tipoterminofiscalizacion
select *
from public.dt_fi_tipoterminofiscalizacion;

insert into raw.dt_fi_unidadorigen
select *
from public.dt_fi_unidadorigen;

insert into raw.dt_mul_categnorma
select *
from public.dt_mul_categnorma;

