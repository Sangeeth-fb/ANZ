import os
import json
from kafka import KafkaProducer
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='log_producer',
    default_args=args,
    schedule_interval=timedelta(days=1),
    tags=['refresh']
)


def generate_stream(**kwargs):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],  # set up Producer
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    producer.flush()
    stream_sample = json.load(open(os.getcwd() + "/mobile_log.json"))

    for message in stream_sample:
        producer.send('MobileLog', message)

    producer.close()


task = PythonOperator(
    task_id='generate_stream',
    python_callable=generate_stream,
    dag=dag,
)

if __name__ == "__main__":
    generate_stream()
