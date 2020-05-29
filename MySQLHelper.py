import mysql.connector

anzdb = mysql.connector.connect(
    host="localhost",
    user="anzuser",
    passwd="pw",
    database="anzdb"
)


class MySQLHelper:

    def __init__(self, sql, val=""):
        self.sql = sql
        self.val = val

    def insert(self):
        try:
            anzdb.connect()
            anzcursor = anzdb.cursor()
            anzcursor.execute(self.sql, self.val)
            anzdb.commit()
        finally:
            anzdb.close()

    def select(self):
        try:
            anzdb.connect()
            anzcursor = anzdb.cursor()
            anzcursor.execute(self.sql)
            rows = anzcursor.fetchall()
        finally:
            anzdb.close()

        return rows
