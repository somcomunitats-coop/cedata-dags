import requests
import pandas as pd

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe

def download_helpscout():
    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()

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
            datathread = {'id': [], 'number': [], 'type': [], 'status': [], 'state': [], 'source_type': [],
                          'source_via': []
                , 'customerId': [], 'customerFirst': [], 'customerLast': []
                , 'createdById': [], 'createdByType': [], 'createdByFirst': [], 'createdByLast': []
                , 'assignedToId': [], 'assignedToType': [], 'assignedToFirst': [], 'assignedToLast': []
                , 'createdAt': []
                          }
            pagethread = 1
            urlthread = 'https://api.helpscout.net/v2/conversations/' + str(x['id']) + '/threads'
            responsethread = requests.get(urlthread, headers=headers).json()
            while pagethread == 1 or responsethread['page']['totalPages'] > responsethread['page']['number']:
                for y in responsethread['_embedded']['threads']:
                    datathread['id'].append(x['id'])
                    datathread['number'].append(y['id'])
                    datathread['type'].append(y['type'])
                    datathread['status'].append(y['status'])
                    datathread['state'].append(y['state'] if 'state' in y else "")
                    datathread['source_type'].append(y['source']['type'])
                    datathread['source_via'].append(y['source']['via'])

                    if 'customer' in y:
                        datathread['customerId'].append(y['customer']['id'])
                        datathread['customerFirst'].append(y['customer']['first'])
                        datathread['customerLast'].append(y['customer']['last'])
                    else:
                        datathread['customerId'].append("")
                        datathread['customerFirst'].append("")
                        datathread['customerLast'].append("")

                    datathread['createdById'].append(y['createdBy']['id'])
                    datathread['createdByType'].append(y['createdBy']['type'])
                    datathread['createdByFirst'].append(y['createdBy']['first'])
                    datathread['createdByLast'].append(y['createdBy']['last'])

                    if 'assignedTo' in y:
                        datathread['assignedToId'].append(y['assignedTo']['id'])
                        datathread['assignedToType'].append(
                            y['assignedTo']['type'] if 'type' in y['assignedTo'] else "")
                        datathread['assignedToFirst'].append(y['assignedTo']['first'])
                        datathread['assignedToLast'].append(y['assignedTo']['last'])
                    else:
                        datathread['assignedToId'].append("")
                        datathread['assignedToType'].append("")
                        datathread['assignedToFirst'].append("")
                        datathread['assignedToLast'].append("")

                    datathread['createdAt'].append(y['createdAt'])
                pagethread = pagethread + 1
            dfthread = pd.DataFrame(datathread)
            dataframetotable("stg_helpscout_thread", conndwh, dfthread, schema="external", if_exists="replace")

            qry = """
                    create table if not exists external.helpscout_thread as 
                    select *
                    from external.stg_helpscout_thread sh;


                    delete from external.helpscout_thread
                    where exists (
                        select * 
                        from external.stg_helpscout_thread h
                        where h.id=helpscout_thread.id
                    );

                    insert into external.helpscout_thread
                    select * 
                    from external.stg_helpscout_thread h;
                """
            executequery(qry, conndwh)


            data['id'].append(x['id'])
            data['number'].append(x['number'])
            data['threads'].append(x['threads'])
            data['type'].append(x['type'])
            data['folderId'].append(x['folderId'])
            data['status'].append(x['status'])
            data['state'].append(x['state'])
            data['subject'].append(x['subject'] if 'subject' in x else "")
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
            data['customerWaitingSinceTime'].append(x['customerWaitingSince']['time'] if 'time' in x['customerWaitingSince'] else "" )
            data['tags'].append(str(x['tags']))
        page = page + 1

    df = pd.DataFrame(data)

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