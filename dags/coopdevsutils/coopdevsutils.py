from projectsecrets.secrets import secrets  # quan es puja a pro
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def _getvalue(bbdd, key):
    return secrets.bbdd[bbdd][key]


def getconnection(bbdd):
    return psycopg2.connect(host=_getvalue(bbdd, 'host'),
                            user=_getvalue(bbdd, 'user'),
                            password=_getvalue(bbdd, 'password'),
                            dbname=_getvalue(bbdd, 'dbname'))


def getconnectionparams(host, port,  user, password, dbname):
    return psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port)


def getalchemyconnectionparams(engine, host, port,  user, password, dbname):
    return create_engine(
        ''+engine+'://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + dbname + '')


def getalchemyconnection(bbdd):
    return create_engine(
        ''+_getvalue(bbdd, 'engine')+'://' + _getvalue(bbdd, 'user') + ':' + _getvalue(bbdd, 'password') + '@'
        + _getvalue(bbdd, 'host') + ':'
        + _getvalue(bbdd, 'port') + '/'+_getvalue(bbdd, 'dbname')+'')


def querytodataframe(query, columns, conn):
    sql_query = pd.read_sql_query(query, conn)
    return pd.DataFrame(sql_query, columns=columns)


def querytovalue(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    return cursor.fetchone()[0]


def dataframetotable(table, bbdd, dataframe, schema="public"):
    dataframe.to_sql(table, bbdd, if_exists='append', index=False, schema=schema)


def executequery(query, conn):
    conn.run(query)

