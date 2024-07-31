import requests
import pandas as pd

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe

def download_helpscout():
    url = "https://api.helpscout.net/v2/oauth2/token"

    payload = {
        "client_id": Variable.get("hs_client_id"),
        "client_secret": Variable.get("hs_client_secret"),
        "grant_type": Variable.get("client_credentials"),
    }

    headers = {
        'Accept': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)
    res = response.json()
    access_token = res['access_token']

    page = 1

    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    data = {'id': [], 'number': [], 'threads': [], 'type': [], 'folderId': [], 'status': [], 'state': []
        , 'subject': [], 'mailboxId': [], 'createdById': [], 'createdByType': [], 'createdByFirst': [],
            'createdByLast': []
        , 'createdAt': [], 'closedByUserId': [], 'closedByUserType': [], 'closedByUserFirst': [], 'closedByUserLast': []
        , 'userUpdatedAt': [], 'customerWaitingSinceTime': [], 'tags': []
            }

    while page == 1 or response['page']['totalPages'] > response['page']['number']:

        url = 'https://api.helpscout.net/v2/conversations/?status=all&page=' + str(
            page) + '&query=(modifiedAt:[NOW-24HOUR TO *])'
        response = requests.get(url, headers=headers).json()
        for x in response['_embedded']['conversations']:
            # Threads
            # url2 = 'https://api.helpscout.net/v2/conversations/'+str(x['id'])+'/threads'
            # response = requests.get(url2, headers=headers).json()
            # print(response)

            data['id'].append(x['id'])
            data['number'].append(x['number'])
            data['threads'].append(x['threads'])
            data['type'].append(x['type'])
            data['folderId'].append(x['folderId'])
            data['status'].append(x['status'])
            data['state'].append(x['state'])
            data['subject'].append(x['subject'])
            data['mailboxId'].append(x['mailboxId'])
            data['createdById'].append(x['createdBy']['id'])
            data['createdByType'].append(x['createdBy']['type'])
            data['createdByFirst'].append(x['createdBy']['first'])
            data['createdByLast'].append(x['createdBy']['last'])
            data['createdAt'].append(x['createdAt'])
            data['closedByUserId'].append(x['closedByUser']['id'])
            data['closedByUserType'].append(x['closedByUser']['type'])
            data['closedByUserFirst'].append(x['closedByUser']['first'])
            data['closedByUserLast'].append(x['closedByUser']['last'])
            data['userUpdatedAt'].append(x['userUpdatedAt'])
            data['customerWaitingSinceTime'].append(x['customerWaitingSince']['time'])
            data['tags'].append(str(x['tags']))
        page = page + 1

    df = pd.DataFrame(data)
    print(df)

    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()
    dataframetotable("stg_helpscout", conndwh, df, schema="external", if_exists="replace")

    qry = """
        create table if not exists external.helpscout as 
        select *
        from external.stg_helpscout sh;
        
        
        delete from external.helpscout
        where exists (
            select * 
            from external.stg_helpscout h
            where h.id=helpscout.id
        );
        
        insert into external.helpscout
        select * 
        from external.stg_helpscout h;
    """
    executequery(qry, conndwh)

    # TODO baixar mailboxes