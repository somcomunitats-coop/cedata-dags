import datetime
import pandas as pd
from pandas import json_normalize
import numpy as np
import requests
from datetime import datetime, timedelta


from airflow.hooks.base import BaseHook
from airflow.models import Variable
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe



def load_incremental_data_pk_date():
    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()

    qrytbl = "select distinct table_name from external.auto_incremental_tables"
    dft = querytodataframe(qrytbl, ['table_name'], conndwh)

    for indextbl, rowtbl in dft.iterrows():

        qrycol = """
            select table_name, table_schema, ordinal_position, column_name, column_type, is_time_reference, is_pk
            from external.auto_incremental_tables
            where table_name='"""+rowtbl["table_name"]+"'"

        df = querytodataframe(qrycol, ['table_name', 'table_schema', 'ordinal_position', 'column_name', 'column_type',
                                       'is_time_reference', 'is_pk'], conndwh)

        # print(df)
        tab = df.iloc[0]["table_name"]
        cols = ""
        incrementaldate = ""
        pk = ""

        for index, row in df.iterrows():
            cols = cols + row["column_name"] + " " + row["column_type"] + ","
            if row["is_time_reference"] == 1:
                incrementaldate = row["column_name"]
            if row["is_pk"]:
                pk = row["column_name"]

        createstg = ("drop table if exists external.hist_stg_" + tab + "; create table if not exists external.hist_stg_"
                     + tab + " (" + cols[:-1] + ")")

        cols += " last bool, dt_start date, dt_end date"
        # si no existeix la taula la creem inicialment amb tots els valors
        create = "create table if not exists external.hist_" + tab + " as " + \
                 "select * , true as last, create_date::date dt_start, '99991231'::date as dt_end " \
                 ", current_timestamp as ts_create, current_timestamp as ts_update " + \
                 " from " + tab

        # print(create)
        executequery(create, conndwh)
        executequery(createstg, conndwh)


        qry = "insert into external.hist_stg_" + tab + " " \
            " select * , current_timestamp as ts_create, current_timestamp as ts_update " \
            " from " + tab + " where " + incrementaldate + ">=current_date - interval '30 days'; \r\n" \
            " delete from external.hist_stg_" + tab + " " \
            " where exists (" \
            "	select *" \
            "	from external.hist_" + tab + "  h" \
            "	where h.last" \
            "		and h." + pk + "=hist_stg_" + tab + "." + pk + " " \
            "		and h." + incrementaldate + "=hist_stg_" + tab + "." + incrementaldate + "" \
            "); \r\n" \
            " update  " \
            " external.hist_"+ tab + " " \
            " set last=false, dt_end=current_date, ts_update=current_timestamp   " \
            " from external.hist_stg_"+tab+" s " \
            " where s." + pk + "=hist_"+tab+"." + pk + " " \
            " and hist_"+tab+".last " \
            "; \r\n" \
            " insert into external.hist_"+tab+" " \
            " select *, true, current_date, '9999-12-31', current_timestamp as ts_create, current_timestamp as ts_update  " \
            " from external.hist_stg_"+tab+" s " \
            " where not exists ( " \
            "	select * " \
            "	from external.hist_"+tab+" h " \
            "	where h." + pk + "=s." + pk + " and h.last " \
            "); \r\n" \
            " update  " \
            " external.hist_"+ tab + " " \
            " set last=false, dt_end=current_date, ts_update=current_timestamp    " \
            " where not exists (select * " \
            " from "+ tab +" s " \
            " where s.id=hist_"+ tab +".id" \
            " ) and last"
        print(qry)

        executequery(qry, conndwh)

    # queries manuals
    qry = """
    ------------------------------------------
    ---- odoo_res_company_res_partner_rel ----
    ------------------------------------------

    truncate table external.hist_stg_odoo_res_company_res_partner_rel;
    insert into external.hist_stg_odoo_res_company_res_partner_rel
    select res_partner_id, res_company_id
    from odoo_res_company_res_partner_rel tc ;
    
    -- caducar els ja no existents
    update external.hist_odoo_res_company_res_partner_rel set dt_end=current_Date, last=false
        , ts_update=current_timestamp
    where not exists (select *
        from external.hist_stg_odoo_res_company_res_partner_rel s
        where s.res_partner_id=hist_odoo_res_company_res_partner_rel.res_partner_id
            and s.res_company_id=res_company_id.res_partner_id
    )
    and last;
    
    -- afegir els nous
    insert into external.hist_odoo_res_company_res_partner_rel
    select res_partner_id, res_company_id, current_Date, '99991231'::date as dt_end, 
        current_timestamp as ts_create, current_timestamp as ts_update 
    from  external.hist_stg_odoo_res_company_res_partner_rel s
    where exists (select *
        from external.hist_odoo_res_company_res_partner_rel h
        where s.res_partner_id=hist_odoo_res_company_res_partner_rel.res_partner_id
            and s.res_company_id=res_company_id.res_partner_id
            and h.last
    );

    """

    executequery(qry, conndwh)


