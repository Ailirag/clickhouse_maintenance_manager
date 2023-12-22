# https://pypi.org/project/peewee-async/
import os
import peewee_async
import peewee
import psycopg2 as psql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from secure_json import Settings
import datetime
import json

settings = Settings(f'{os.getcwd()}{os.sep}settings.json').data


con = psql.connect(dbname='postgres',
                   user=settings.database.user,
                   host=settings.database.host,
                   port=settings.database.port,
                   password=settings.database.pwd)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute(f"SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = '{settings.database.database_name}'")
not_exists, = cur.fetchone()

if not_exists:
    cur.execute(f'CREATE DATABASE "{settings.database.database_name}"')
con.close()

database_connection = peewee_async.PostgresqlDatabase(
        database=settings.database.database_name,
        user=settings.database.user,
        host=settings.database.host,
        port=settings.database.port,
        password=settings.database.pwd,
    )


class DataBases(peewee.Model):
    uuid = peewee.UUIDField(primary_key=True)
    DBName = peewee.CharField()
    table_name = peewee.CharField()
    engine = peewee.CharField()
    total_rows = peewee.BigIntegerField()
    total_mb = peewee.BigIntegerField()
    last_data = peewee.DateTimeField(default=datetime.datetime.now())
    stored_days = peewee.IntegerField(default=0)
    save_history_changes = peewee.BooleanField(default=False)

    delete_conditions = peewee.TextField(default="[]")

    class Meta:
        database = database_connection


class SystemLogTables(peewee.Model):
    name = peewee.CharField()

    class Meta:
        database = database_connection


class Users(peewee.Model):
    login = peewee.CharField()
    password = peewee.CharField()
    active = peewee.BooleanField()
    created = peewee.DateTimeField(default=datetime.datetime.now())

    class Meta:
        database = database_connection


async def update_last_date_time(uuid, date_from_click):
    clear_date = (datetime.datetime.strptime(date_from_click, '%Y-%m-%d %H:%M:%S')
                    + datetime.timedelta(seconds=settings.clickhouse.time_offset))\
                    .strftime('%Y-%m-%d %H:%M:%S')
    (DataBases
     .update(last_data=clear_date)
     .where(DataBases.uuid == uuid)
     .execute())


async def update_database(dict_data: dict):
    (DataBases
     .update(
        DBName=dict_data['database'],
        table_name=dict_data['name'],
        engine=dict_data['engine'],
        total_rows=dict_data['total_rows'],
        total_mb=int(dict_data['total_bytes']) // 1048576)
     .where(DataBases.uuid == dict_data['uuid'])
     .execute())

async def update_settings_database(uuid, dict_data: dict):

    json_data = json.dumps(dict_data)

    (DataBases
     .update(
        settings=json_data)
     .where(DataBases.uuid == uuid)
     .execute())


async def insert_row_database(dict_data: dict):
    (DataBases
     .insert(
        uuid=dict_data['uuid'],
        DBName=dict_data['database'],
        table_name=dict_data['name'],
        engine=dict_data['engine'],
        total_rows=dict_data['total_rows'],
        total_mb=int(dict_data['total_bytes']) // 1048576)
     .execute())


DataBases.create_table()
SystemLogTables.create_table()
Users.create_table()

