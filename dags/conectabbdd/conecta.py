from coopdevsutils.coopdevsutils import querytodataframe, dataframetotable, executequery, getalchemyconnection
from airflow.hooks.base import BaseHook


def connecta():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook().get_sqlalchemy_engine()
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_sqlalchemy_engine()
    query = "insert into test_inici (b) values(round(random()*10) )"
    executequery(query, connraw)
    df = querytodataframe('select sum(b) as a from test_inici', ['a'], connraw)
    dataframetotable(table='test_desti', bbdd=conndwh, dataframe=df)
    return 'ok'
