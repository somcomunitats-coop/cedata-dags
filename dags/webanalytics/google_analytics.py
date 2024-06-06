from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest
)
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from coopdevsutils.coopdevsutils import dataframetotable, executequery, querytodataframe


def ga4_result_to_df(resp):
    result_dict = {}
    for dimensionHeader in resp.dimension_headers:
        result_dict[dimensionHeader.name] = []
    for metricHeader in resp.metric_headers:
        result_dict[metricHeader.name] = []

    for rowIdx, row in enumerate(resp.rows):
        for i, dimension_value in enumerate(row.dimension_values):
            dimension_name = resp.dimension_headers[i].name
            result_dict[dimension_name].append(dimension_value.value)
        for i, metric_value in enumerate(row.metric_values):
            metric_name = resp.metric_headers[i].name
            result_dict[metric_name].append(metric_value.value)
    return pd.DataFrame(result_dict)

def load_ga_daily_data():
    conndwh = BaseHook.get_connection('DWH_CE').get_hook().get_sqlalchemy_engine()

    gcp_sa_credentials = {
        "private_key": Variable.get("ga_private_key_secret"),
        "client_email": Variable.get("ga_client_email"),
        "token_uri": Variable.get("ga_token_uri"),
    }
    property_id = Variable.get("ga_property_id")

    credentials = service_account.Credentials.from_service_account_info(gcp_sa_credentials)
    client = BetaAnalyticsDataClient(credentials=credentials)

    # contentId, contentType
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name='date'), Dimension(name='dateHour'), Dimension(name='deviceCategory'), Dimension(name='hostName'),
                    Dimension(name='linkUrl'), Dimension(name='pagePath'),
                    Dimension(name='pageTitle'), Dimension(name='city'),
                    ],
        metrics=[Metric(name="activeUsers"), Metric(name="sessions"), Metric(name="engagedSessions")],
        date_ranges=[DateRange(start_date=datetime.strftime(datetime.now() - timedelta(3), '%Y-%m-%d'), end_date="today")]
    )

    response = client.run_report(request)
    df = ga4_result_to_df(response)
    dataframetotable("stg_ga_visits", conndwh, df, schema="external", if_exists="replace")

    qry = """
    delete from external.ga_visits 
    where exists( 
        select *
        from external.stg_ga_visits s
        where s.date= ga_visits.date);
    
    insert into external.ga_visits
    select *
    from external.stg_ga_visits;     
    """

    executequery(qry, conndwh)


