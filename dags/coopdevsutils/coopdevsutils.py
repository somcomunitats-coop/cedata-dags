
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text


def _getvalue(bbdd, key):
    pass
    #return secrets.bbdd[bbdd][key]


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
    x = cursor.fetchone()[0]
    cursor.close()
    return x


def dataframetotable(table, bbdd, dataframe, schema="public", if_exists="append"):
    dataframe.to_sql(table, bbdd, if_exists=if_exists, index=False, schema=schema)


def executequery(query, conn):
    qr = text(query)
    conn.execute(qr)

