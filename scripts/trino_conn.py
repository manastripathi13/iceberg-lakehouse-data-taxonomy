import os

import trino


def connect():
    return trino.dbapi.connect(host=os.getenv("TRINO_HOST", "localhost"), port=8080, user="demo", catalog="lakehouse")


def q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()
