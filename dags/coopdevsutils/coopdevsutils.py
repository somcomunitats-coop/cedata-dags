import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging


def getconnectionparams(host, port,  user, password, dbname):
    return psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port)


def getalchemyconnectionparams(engine, host, port,  user, password, dbname):
    return create_engine(
        ''+engine+'://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + dbname + '')


def querytodataframe(query, columns, conn):
    sql_query = pd.read_sql_query(query, conn)
    return pd.DataFrame(sql_query, columns=columns)


def querytovalue(query, conn):
    rs = conn.execute(query)
    for row in rs:
        return row[0]


def dataframetotable(table, bbdd, dataframe, schema="public"):
    dataframe.to_sql(table, bbdd, if_exists='append', index=False, schema=schema)


def executequery(query, conn):
    conn.execute(query)

