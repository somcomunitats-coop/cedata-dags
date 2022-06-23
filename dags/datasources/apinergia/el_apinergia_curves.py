#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import psycopg2
import datetime
from pathlib import Path

from psycopg2 import extras
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from somenergia_apinergia.apinergia import Apinergia, Authentication


def config():
    username = Variable.get('APINERGIA_USERNAME')
    password = Variable.get('APINERGIA_PASSWORD')
    base_url = Variable.get('APINERGIA_BASEURL')

    Authentication.set_url(base_url)

    return Apinergia(base_url, username, password)


def insert_contract_meter(conraw, contractid, meter):
    insert_query = "insert into contract(id, id_api) values(%s, %s)  on conflict do nothing"
    conraw.execute(insert_query, (contractid, meter))


def insert_curves(conraw, curves):

    created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # TODO beautify this json to table conversion
    dict_readings = [
        {
            'ts': c['measurements']['date'],
            'meter': c['meteringPointId'],
            'contract': c['contractId'],
            'input_active_energy_kwh': c['measurements']['ai'],
            'output_active_energy_kwh': c['measurements']['ao'],
            'created_at': created_at,
            'updated_at': updated_at
        }
        for c in curves
    ]

    # # TODO create meters and contracts FK?
    # for dr in dict_readings:
    #     db_contract, db_meter = insert_contract_meter(conraw, dr['contract'], dr['meter'])
    #     dr['contract'] = db_contract
    #     dr['meter'] = db_meter

    values = [tuple(dr.values()) for dr in dict_readings]

    insert_query = "insert into curveregistry values %s"

    psycopg2.extras.execute_values(conraw, insert_query, values)

    conraw.execute(f"select * from curveregistry where updated_at = '{updated_at}'")
    inserted_registries = conraw.fetchall()
    return inserted_registries


def db_init(conraw):
    query = Path('datasources/apinergia/apinergia_schema.sql').read_text(encoding='utf8')
    result = conraw.execute(query)
    return result


def el_curves(conraw, api, contractid, cch_type, start_date, end_date):
    result = api.get_cch_curves(contractid, cch_type, start_date, end_date)
    insert_curves(conraw, result)
    return result


def download_curves(contractid, cch_type, start_date, end_date):
    api = config()
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_conn()
    with connraw as conn:
        with conn.cursor() as curs:
            el_curves(curs, api, contractid, cch_type, start_date, end_date)


def get_last_date_contract(contractid):
    connraw = BaseHook.get_connection('Rawdata').get_hook().get_conn()
    with connraw as conn:
        with conn.cursor() as curs:
            curs.execute("select  coalesce(max(ts),'2022-06-01') as ts from curveregistry where contract='"
                         + contractid + "';")
            results = curs.fetchall()
            return results[0][0]


def get_contracts():
    return ['0163929']
