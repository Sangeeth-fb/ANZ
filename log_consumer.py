import json
from kafka import KafkaConsumer
from datetime import date
from python.ANZChallenge import MySQLHelper as SQL


def consume_stream(**kwargs):
    consumer = KafkaConsumer('MobileLog', bootstrap_servers=['localhost:9092'],  # set up Producer,
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        if message.value['mobileNum']:
            sql = "INSERT INTO MobileLog (mobileNum, url, SessionStartTime, SessionEndTime, bytesIn, bytesOut, timestamp, ds) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (message.value['mobileNum'], message.value['url'], message.value['SessionStartTime'],
                   message.value['SessionEndTime'], message.value['bytesIn'], message.value['bytesOut'],
                   message.timestamp, date.today()
                   )
            anzsql = SQL.MySQLHelper(sql, val)
            anzsql.insert()
    consumer.close()


if __name__ == "__main__":
    consume_stream()
