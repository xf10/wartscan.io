import os

import psycopg2

import utils

POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DB = os.environ.get('POSTGRES_DB')


def db_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(database=POSTGRES_DB,
                            host="db",
                            user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD,
                            port="5432",
                            connect_timeout=3,
                            keepalives=1,
                            keepalives_idle=5,
                            keepalives_interval=2,
                            keepalives_count=2)


def commit_sql(con, sql):
    with con:
        with con.cursor() as cur:
            cur.execute(sql)
    con.commit()


def createtables(con):
    with con:
        with con.cursor() as cur:
            cur.execute("""
                            CREATE TABLE IF NOT EXISTS txs ( id SERIAL NOT NULL PRIMARY KEY,
                            type TEXT,
                            hash TEXT,
                            amount BIGINT,
                            fee BIGINT,
                            nonce BIGINT,
                            pinHeight INTEGER,
                            height INTEGER,
                            sender TEXT,
                            recipient TEXT,
                            block_timestamp INTEGER);""")

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS blocks ( id SERIAL NOT NULL PRIMARY KEY,
                            height INTEGER,
                            difficulty DECIMAL,
                            timestamp INTEGER,
                            timestamp_seen INTEGER,
                            hash TEXT,
                            merkleRoot TEXT,
                            nonce TEXT,
                            janushash1 NUMERIC,
                            janushash2 NUMERIC,
                            prevHash TEXT,
                            raw TEXT,
                            target TEXT,
                            version TEXT,
                            minedby TEXT,
                            forked INT DEFAULT 0,
                            txn INT);""")

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS balances ( id SERIAL NOT NULL PRIMARY KEY,
                            account TEXT,
                            balance BIGINT,
                            label TEXT,
                            first_movement INTEGER,
                            last_movement INTEGER,
                            miningratio REAL DEFAULT 0,
                            miningratio24h REAL DEFAULT 0,
                            UNIQUE(account));""")

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS mempool ( id SERIAL NOT NULL PRIMARY KEY,
                            type TEXT,
                            hash TEXT,
                            amount BIGINT,
                            fee BIGINT,
                            nonce BIGINT,
                            pinHeight INTEGER,
                            sender TEXT,
                            recipient TEXT);""")
            cur.execute("""
                            CREATE TABLE IF NOT EXISTS historic_chart_data ( id SERIAL NOT NULL PRIMARY KEY,
                            timestamp INTEGER,
                            hashrate REAL,
                            difficulty REAL,
                            tps REAL,
                            dau INTEGER);""") # dau = daily active users

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS statistics ( id SERIAL NOT NULL PRIMARY KEY,
                            type TEXT,
                            name TEXT,
                            label TEXT,
                            value NUMERIC,
                            unit TEXT DEFAULT '',
                            UNIQUE(name));""")

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS nodes ( id SERIAL NOT NULL PRIMARY KEY,
                            first_seen INTEGER,
                            last_seen INTEGER,
                            height INTEGER,
                            host TEXT,
                            version TEXT,
                            UNIQUE(host));""")

            cur.execute("""
                            CREATE TABLE IF NOT EXISTS price_data ( id SERIAL NOT NULL PRIMARY KEY,
                            timestamp BIGINT,
                            price REAL);""")
            cur.execute("""
                            CREATE TABLE IF NOT EXISTS logs ( id SERIAL NOT NULL PRIMARY KEY,
                            event TEXT,
                            status TEXT,
                            timestamp INTEGER,
                            duration REAL);""")
            cur.execute("""
            CREATE INDEX IF NOT EXISTS "blocks_height" ON "blocks" ("height" DESC);
            CREATE INDEX IF NOT EXISTS "blocks_minedby" ON "blocks" ("minedby");
            CREATE INDEX IF NOT EXISTS "blocks_timestamp" ON "blocks" ("timestamp");
            CREATE INDEX IF NOT EXISTS "blocks_forked" ON "blocks" ("forked");
            
            CREATE INDEX IF NOT EXISTS "txs_sender" ON "txs" ("sender");
            CREATE INDEX IF NOT EXISTS "txs_recipient" ON "txs" ("recipient");
            CREATE INDEX IF NOT EXISTS "txs_height" ON "txs" ("height" DESC);
            CREATE INDEX IF NOT EXISTS "txs_block_timestamp" ON "txs" ("block_timestamp");
            CREATE INDEX IF NOT EXISTS "txs_hash" ON "txs" ("hash");
            """)
    con.commit()