# coding: utf-8

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool


@listens_for(Pool, "connect")
def assume_role(dbapi_con, connection_record):
    print("Triage is assuming the role!")
    dbapi_con.cursor().execute('set role direccion_trabajo_inspections_write;')
    print("Everything is OK!")
