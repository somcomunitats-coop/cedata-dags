from coopdevsutils.coopdevsutils import querytodataframe, dataframetotable, executequery, getalchemyconnection
from airflow.hooks.base import BaseHook


def connecta():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook()
    connraw = BaseHook.get_connection('Rawdata').get_hook()
    query = "insert into test_inici (b) values(round(random()*10) )"
    executequery(query, connraw)
    df = querytodataframe('select sum(b) as a from test_inici', ['a'], connraw.get_conn())
    dataframetotable(table='test_desti', bbdd=conndwh.get_sqlalchemy_engine(), dataframe=df)
    return 'ok'
