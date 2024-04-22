from coopdevsutils.coopdevsutils import querytodataframe, dataframetotable, executequery, getalchemyconnection
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import requests


def getmailerlitedata():
    conndwh = BaseHook.get_connection('DWH').get_hook().get_sqlalchemy_engine()
    query = "create table if not exists external.mailerlite_subscribers(data date primary key, subscribers int);"
    executequery(query, conndwh)

    query = "create table if not exists external.aux_mailerlite_subscribers_detail(it jsonb);"
    executequery(query, conndwh)

    query = "create table if not exists external.mailerlite_subscribers_detail(email varchar(500)," +\
            "createddate timestamp);"
    executequery(query, conndwh)

    query = "delete from external.aux_mailerlite_subscribers_detail"
    executequery(query, conndwh)

    offset = 0
    elements = 0
    currentelements = 5000

    headers = {
        "Accept": "application/json",
        "X-MailerLite-ApiDocs": "true",
        "X-MailerLite-ApiKey": Variable.get("X-MailerLite-ApiKey")
    }

    while currentelements == 5000:
        url = "https://api.mailerlite.com/api/v2/subscribers?offset=" + str(offset) + "&limit=5000&type=active"
        response = requests.get(url, headers=headers)
        query = "insert into external.aux_mailerlite_subscribers_detail values('" + response.text.replace("'", "''") \
                + "');"
        executequery(query, conndwh)
        currentelements = len(response.json())
        elements += currentelements
        offset += 5000

    query = "delete from external.mailerlite_subscribers where data=CURRENT_DATE;"
    executequery(query, conndwh)
    query = "insert into external.mailerlite_subscribers values(CURRENT_DATE, " + str(elements) + ");"
    executequery(query, conndwh)

    query = "delete from external.mailerlite_subscribers_detail;"
    executequery(query, conndwh)

    query = "insert into external.mailerlite_subscribers_detail	" +\
        "select mail, datacrecio::timestamp " +\
        "from ( " +\
        "select json_array_elements(i.it::json) ->> 'email' as mail " +\
        ",json_array_elements(i.it::json) ->> 'date_created' as datacrecio " +\
        "from external.aux_mailerlite_subscribers_detail i " +\
        ") a;"
    executequery(query, conndwh)


def getinstagram():
    conndwh = BaseHook.get_connection('DWH').get_hook().get_sqlalchemy_engine()
    query = "create table if not exists external.instagram_followers(data date primary key, followers int);"
    executequery(query, conndwh)
    elements = 0
    headers = {
        "Accept": "application/json",
    }
    url = "https://www.instagram.com/lazona_mercat/?__a=1"
    response = requests.get(url, headers=headers)
    elements = response.json()['graphql']['user']['edge_followed_by']['count']
    query = "delete from external.instagram_followers where data=CURRENT_DATE;"
    executequery(query, conndwh)
    query = "insert into external.instagram_followers values(CURRENT_DATE, " + str(elements) + ");"
    executequery(query, conndwh)
