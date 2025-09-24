import requests
import pandas as pd

from airflow.hooks.base import BaseHook
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe
import json

def calc_geography_cm_place():
    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()

    qry = """
        select p.id, p.lat, p.lng
        ,case when isnumeric((string_to_Array(p.address_txt ,','))[1]) then 
                case when length((string_to_Array(p.address_txt ,','))[1])=4 then '0' else '' end || (string_to_Array(p.address_txt ,','))[1]
            when isnumeric((string_to_Array(p.address_txt ,','))[2]) then 
                case when length((string_to_Array(p.address_txt ,','))[2])=4 then '0' else '' end || (string_to_Array(p.address_txt ,','))[2]  
        end as cp
        from odoo_cm_place p 
        where not exists (
            select *
            from  external.geography_cm_place c
            where c.id_cm_place=p.id
        )
    """


    df = querytodataframe(qry, ["id", "lat", "lng", "cp"], conndwh)
    for index, row in df.iterrows():
        lat = row["lat"].replace(',','')
        geourl = f'https://www.cartociudad.es/geocoder/api/geocoder/reverseGeocode?lon={row["lng"]}&lat={lat}'
        try:
            rgeo = requests.get(geourl)
            dgeo = json.loads(rgeo.text)
            res = [[row["id"], dgeo['muni'], None, dgeo['province'],
                    dgeo['comunidadAutonoma'], dgeo['postalCode']]]
            df = pd.DataFrame(res,
                              columns=['id_cm_place', 'municipi', 'comarca', 'provincia', 'ccaa', 'codpostal'])
            dataframetotable('geography_cm_place', conndwh, df, schema="external")
        except:
            print(geourl)
            res = [[row["id"], None, None, None,
                    None,  row["cp"][:5]]]
            df = pd.DataFrame(res,
                              columns=['id_cm_place', 'municipi', 'comarca', 'provincia', 'ccaa', 'codpostal'])
            dataframetotable('geography_cm_place', conndwh, df, schema="external")