from coopdevsutils.coopdevsutils import querytodataframe, dataframetotable, executequery, querytovalue
from airflow.hooks.base import BaseHook


def connecta():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook().get_sqlalchemy_engine()
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_sqlalchemy_engine()
    query = "insert into test_inici (b) values(round(random()*10) )"
    executequery(query, connraw)
    df = querytodataframe('select sum(b) as a from test_inici', ['a'], connraw)
    dataframetotable(table='test_desti', bbdd=conndwh, dataframe=df)
    return 'ok'


def connecta_sentilo():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook().get_sqlalchemy_engine()
    connraw = BaseHook.get_connection('Sentilo').get_hook().get_sqlalchemy_engine()
    df = querytodataframe('select count(*) as a from sentilo_observations;', ['a'], connraw)
    dataframetotable(table='test_desti', bbdd=conndwh, dataframe=df)
    return 'ok'


def curves_raw_to_dwh():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook().get_sqlalchemy_engine()
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_sqlalchemy_engine()
    # Agafar última data d'insert a dwh
    max_updated_at = querytovalue("select coalesce(max(updated_at),'20220401') as updated_at from ODS_curveregistry"
                                  , conndwh)
    # agafar les dades de rawdata des d'aquella data
    executequery('delete from stg_curveregistry ;', conndwh)

    df = querytodataframe("select ts, meter, contract, input_active_energy_kwh, output_active_energy_kwh, created_at"
                          ", updated_at as updated_at from curveregistry where updated_at>='"
                          + max_updated_at.strftime("%Y%m%d %H:%M:%S") + "';"
                          , ['ts', 'meter', 'contract', 'input_active_energy_kwh', 'output_active_energy_kwh'
                              , 'created_at', 'updated_at'], connraw)
    # portar les dades a STG
    dataframetotable(table='stg_curveregistry', bbdd=conndwh, dataframe=df)
    # Esborrar per timestamp, meter, contract
    executequery('delete from ods_curveregistry '
                 'where exists (select * '
                 'from stg_curveregistry s '
                 'where s.ts = ods_curveregistry.ts '
                 'and s.meter = ods_curveregistry.meter '
                 'and s.contract = ods_curveregistry.contract '
                 ');', conndwh)
    # Inserir dades
    executequery('insert into ods_curveregistry '
                 'select ts, meter, contract, input_active_energy_kwh, output_active_energy_kwh, created_at, updated_at'
                 ' from stg_curveregistry; ', conndwh)
    return 'ok'



def curves_dl_to_dwh():
    conndwh = BaseHook.get_connection('Datawarehouse').get_hook().get_sqlalchemy_engine()
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_sqlalchemy_engine()

    ## GENERATED
    # Agafar última data d'insert a dwh
    max_updated_at = querytovalue("select coalesce(max(updated_at),'20220401') as updated_at from ods_generated_energy"
                                  , conndwh)
    # agafar les dades de rawdata des d'aquella data
    executequery('delete from stg_generated_energy ;', conndwh)

    df = querytodataframe("select ts, cups, input_active_energy_kwh, output_active_energy_kwh, source, created_at"
                          ", updated_at as updated_at from dl_generated_energy where updated_at>='"
                          + max_updated_at.strftime("%Y%m%d %H:%M:%S") + "';"
                          , ['ts', 'cups', 'input_active_energy_kwh', 'output_active_energy_kwh', 'source'
                              , 'created_at', 'updated_at'], connraw)
    # portar les dades a STG
    dataframetotable(table='stg_generated_energy', bbdd=conndwh, dataframe=df)
    # Esborrar per timestamp, meter, contract
    executequery('delete from ods_generated_energy '
                 'where exists (select * '
                 'from stg_generated_energy s '
                 'where s.ts = ods_generated_energy.ts '
                 'and s.cups = ods_generated_energy.cups '
                 ');', conndwh)
    # Inserir dades
    executequery('insert into ods_generated_energy '
                 'select ts, cups, input_active_energy_kwh, output_active_energy_kwh, source, created_at, updated_at'
                 ' from stg_generated_energy; ', conndwh)

    ## CONSUMED
    # Agafar última data d'insert a dwh
    max_updated_at = querytovalue("select coalesce(max(updated_at),'20220401') as updated_at from ods_consumed_energy"
                                  , conndwh)
    # agafar les dades de rawdata des d'aquella data
    executequery('delete from stg_consumed_energy ;', conndwh)

    df = querytodataframe("select ts, cups, input_active_energy_kwh, output_active_energy_kwh, source, created_at"
                          ", updated_at as updated_at from dl_consumed_energy where updated_at>='"
                          + max_updated_at.strftime("%Y%m%d %H:%M:%S") + "';"
                          , ['ts', 'cups', 'input_active_energy_kwh', 'output_active_energy_kwh', 'source'
                              , 'created_at', 'updated_at'], connraw)
    # portar les dades a STG
    dataframetotable(table='stg_generated_energy', bbdd=conndwh, dataframe=df)
    # Esborrar per timestamp, meter, contract
    executequery('delete from ods_consumed_energy '
                 'where exists (select * '
                 'from stg_consumed_energy s '
                 'where s.ts = ods_consumed_energy.ts '
                 'and s.cups = ods_consumed_energy.cups '
                 ');', conndwh)
    # Inserir dades
    executequery('insert into ods_consumed_energy '
                 'select ts, cups, input_active_energy_kwh, output_active_energy_kwh, source, created_at, updated_at'
                 ' from stg_consumed_energy; ', conndwh)



    return 'ok'
