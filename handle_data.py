import psycopg2

from config_env import (
    DB_ADDR,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PSWD,
)

conn = psycopg2.connect(
    host=DB_ADDR['lan'],
    port=DB_PORT,
    dbname=DB_NAME['development'],
    user=DB_USER['production'],
    password=DB_PSWD['production']
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE genesis(
    id integer PRIMARY KEY,
    email text,
    name text,
    address text)
""")
conn.commit()
conn.close()
