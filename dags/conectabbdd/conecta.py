from coopdevsutils.coopdevsutils import querytodataframe, dataframetotable, executequery, getalchemyconnection


def connecta():
    conndwh = getalchemyconnection('datawarehouse')
    connraw = getalchemyconnection('rawdata')
    query = "insert into test_inici (b) values(round(random()*10) )"
    executequery(query, connraw)
    df = querytodataframe('select sum(b) as a from test_inici', ['a'], connraw)

    dataframetotable('test_desti', conndwh, df)
    conndwh.close()
    connraw.close()
    return 'ok'
