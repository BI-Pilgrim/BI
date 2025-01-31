from datetime import datetime, timedelta
import json
import time
from typing import TYPE_CHECKING, Dict
from airflow import DAG, XComArg
from airflow.models import Variable
from airflow.utils.dates import days_ago
import pandas as pd
from affiniv.scraper import AffinivScraper, LoginInitializer
from utils.google_cloud import get_bq_client
import logging
from airflow.sensors.base import BaseSensorOperator
from airflow.decorators import dag, task
from google.cloud import bigquery


if TYPE_CHECKING:
    from airflow.utils.context import Context


survey_id = SURVEY_ID = "3800908829962028804"
TABLE_ID = "shopify-pubsub-project.pilgrim_bi_affiniv.order_nps_survey_responses"

class ReportGeneratedSensor(BaseSensorOperator):
    def __init__(self, *, survey_id:str, login_initializer:XComArg, report_id:str, **kwargs):
        super().__init__(**kwargs)
        self.login_initializer = login_initializer
        self.start = datetime.now()-timedelta(hours=5)
        self.end = datetime.now()
        self.report_id = report_id
        self.survey_id = survey_id
    
    
    def poke(self, context:'Context'):
        login_data = context['ti'].xcom_pull(task_ids=self.login_initializer.operator.task_id, key=self.login_initializer.key)
        login_data = json.loads(login_data)
        aff_scraper = AffinivScraper(None, None, LoginInitializer(**login_data))
        resp = aff_scraper.get_report_jobs(self.survey_id, self.start, self.end)
        if self.report_id is None: self.report_id = resp[0].id
        for report in resp:
            if(report.id == self.report_id and report.s3ReportLocation is not None):
                return True
        return False
    


# Define the DAG

@dag("affiniv_sync", schedule='0 2 * * *', start_date=days_ago(1), tags=["affiniv"])
def affiniv_sync():

    @task.python
    def login():
        username = Variable.get("affiniv_username")
        password = Variable.get("affiniv_password")
        
        # Initialize the scraper
        scraper = AffinivScraper(username=username, password=password)
        return json.dumps({"company_id": scraper.login_data.company.id, "session_headers":dict(scraper.session.headers), "token":scraper.token})

    @task.python
    def trigger_report(survey_id:str, login_data:str):
        login_data = json.loads(login_data)
        scraper = AffinivScraper(None, None, LoginInitializer(**login_data))
        start_date = datetime.now()
        end_date = start_date-timedelta(days=1)
        scraper.trigger_report(survey_id, start_date, end_date)
        reports = scraper.get_report_jobs(survey_id, datetime.now()-timedelta(hours=5), datetime.now())
        return reports[0].id
    
    def download_push_report(url:str):
        df = pd.read_csv(url)
        df['Response Date'] = df['Response Date'].apply(lambda x:pd.to_datetime(x))
        long_cols = [col for col in df.columns if len(col)>50]
        longcol_df = df[long_cols]
        long_q_data = json.loads(longcol_df.to_json(index=False, orient='records'))
        df.drop(columns=long_cols, inplace=True)
        df["long_questions"] = long_q_data
        df["long_questions"] = df["long_questions"].apply(lambda x:json.dumps(x))
        load_data_to_bigquery(pd.DataFrame(df), datetime.now())
        

    def load_data_to_bigquery(df:pd.DataFrame, extracted_at, _retry=0, max_retry=3):
        """Load the data into BigQuery."""
        
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        client_bq = get_bq_client(credentials_info)
        
        df["ee_extracted_at"] = extracted_at
        set_cols = set()
        columns = []
        for col in df.columns:
            col = col.lower().replace(" ", "_")
            if col in set_cols:
                columns.append(f"{col}_1")
            else:
                columns.append(col)
                set_cols.add(col)
        df = df.set_axis(columns, axis=1)
        # table = self.client.get_table(self.table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # schema=table.schema,
        )
        try:
            job = client_bq.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
            job.result()
        except Exception as e:
            print("error ", e)
            if _retry+1<max_retry: 
                mint = 60
                print(f"SLEEPING :: Error inserting to BQ retrying in {mint} min")
                time.sleep(60*mint) # 15 min
                return load_data_to_bigquery(df, extracted_at, _retry=_retry+1, max_retry=max_retry)
            raise e
        

    @task.python()
    def fetch_and_download_survey(survey_id:str, report_id:str, login_data:str):
        login_data = json.loads(login_data)
        scraper = AffinivScraper(None, None, LoginInitializer(**login_data))
        reports = scraper.get_report_jobs(survey_id, datetime.now()-timedelta(hours=5), datetime.now())
        for report in reports:
            if(report.id == report_id and report.s3ReportLocation is not None):
                download_push_report(report.s3ReportLocation)
                return True
    
    # Define the task
    login_data = login()
    report_id = trigger_report(SURVEY_ID, login_data)
    report_sensor = ReportGeneratedSensor(task_id="check_report_State", survey_id=SURVEY_ID, login_initializer=login_data, report_id=report_id, 
                                          timeout=timedelta(hours=3), poke_interval=timedelta(minutes=10), mode='reschedule')
    save_report = fetch_and_download_survey(SURVEY_ID, report_id, login_data)

    login_data >> report_id >> report_sensor >> save_report

dag_sync = affiniv_sync()