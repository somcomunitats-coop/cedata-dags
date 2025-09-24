import requests
import pandas as pd

from airflow.hooks.base import BaseHook
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe
import json

def calc_geography_cm_place():
    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()

    qry = """
        select p.id, p.lat, p.lng 
        from odoo_cm_place p 
        where not exists (
            select *
            from  external.geography_cm_place c
            where c.id_cm_place=p.id
        )
    """


    df = querytodataframe(qry, ["id", "lat", "lng"], conndwh)
    for index, row in df.iterrows():
        geourl = f'https://www.cartociudad.es/geocoder/api/geocoder/reverseGeocode?lon={row["lng"]}&lat={row["lat"]}'
        rgeo = requests.get(geourl)
        dgeo = json.loads(rgeo.text)
        res = [[row["id"], dgeo['muni'], None, dgeo['province'],
                dgeo['comunidadAutonoma'], dgeo['postalCode']]]
        df = pd.DataFrame(res,
                          columns=['id_cm_place', 'municipi', 'comparca', 'provincia', 'ccaa', 'codpostal'])
        dataframetotable('geography_cm_place', conndwh, df, schema="external")