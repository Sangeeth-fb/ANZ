from __future__ import print_function

import csv
import time
from datetime import timedelta, date
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from python.ANZChallenge import MySQLHelper as SQL

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='report_generator',
    default_args=args,
    schedule_interval="0 12 * * *",
    tags=['refresh']
)


def generate_consumed_data():
    sql = "select sum(bytesIn) as downloaded_size,sum(bytesOut) uploaded_size,url,mobileNum from MobileLog " \
           " where ds between CURDATE() - INTERVAL 1 DAY AND '{0}' " \
           " Group by MobileNum, url;".format(date.today())

    anzsql = SQL.MySQLHelper(sql)
    rows = anzsql.select()

    f = open("/Users/sangeethjairaj/learning/consumed_data_{0}.csv".format(date.today() - timedelta(days=1)), "w")
    anzfile = csv.writer(f)
    anzfile.writerows(rows)


def generate_time_spent():
    sql = "select sum(session_time), MobileNum, url " \
          "from " \
          "(select time_to_sec(TIMEDIFF(max(SessionEndTime), min(SessionStartTime))) session_time, url, mobileNum, timestamp " \
          "from MobileLog where " \
          "ds between CURDATE() - INTERVAL 1 DAY AND '{0}' " \
          "Group by MobileNum, url, timestamp ) as c " \
          "GROUP BY MobileNum, url ".format(date.today())


    anzsql = SQL.MySQLHelper(sql)
    rows = anzsql.select()

    f = open("/Users/sangeethjairaj/learning/time_spent_{0}.csv".format(date.today() - timedelta(days=1)), "w")
    anzfile = csv.writer(f)
    anzfile.writerows(rows)
    f.close()


consumed_data_report = PythonOperator(
    task_id='consumed_data_report',
    python_callable=generate_consumed_data,
    dag=dag,
)

time_spent_report = PythonOperator(
    task_id="time_spent_report",
    python_callable=generate_time_spent,
    dag=dag,
)

if __name__ == "__main__":
    generate_consumed_data()
    generate_time_spent()

