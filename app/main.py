import asyncio
import json
import traceback

import aiohttp
from tools import tools
from .webapi import *
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import sys


async def clickhouse_query(query, to_json=False):
    headers = {'X-ClickHouse-User': f'{settings.clickhouse.user}',
               'X-ClickHouse-Key': f'{settings.clickhouse.pwd}'}
    try:
        async with aiohttp.ClientSession(connector=None) as session:
            async with session.post(f'{settings.clickhouse.url}:{settings.clickhouse.port}', data=query, headers=headers) as response:
                data = await response.text()
                if to_json:
                    try:
                        return json.loads(data)['data'], False
                    except:
                        return data, True
                else:
                    return data, False
    except Exception as e:
        tools.logging(f'Error on clickhouse query. {str(type(e)) + str(e)}')
        return {
            'status_code': 404,
            'text': 'Error'
        }, True


async def clickhouse_checker():

    while True:

        query = '''select database, name, engine, total_rows, total_bytes, uuid from system.tables
                where not database in ('system', 'INFORMATION_SCHEMA', 'information_schema')
                format JSON'''
        try:
            all_database = await clickhouse_query(query, True)
        except Exception as e:
            tools.logging(f'Error getting database data from clickhouse. {str(type(e)) + str(e)}')
            data = list()

        for database in all_database[0]:
            rows = (DataBases
                    .select(DataBases.uuid)
                    .where(DataBases.uuid == database['uuid']))
            if not rows:
                await insert_row_database(database)
            else:
                await update_database(database)

        rows = DataBases.select().where(DataBases.table_name == 'EventLogItems')
        for database in rows:
            query = f'select DateTime from {database.DBName}.EventLogItems order by DateTime desc limit 1 format JSON'
            result, Error = await clickhouse_query(query, True)
            
            if len(result) == 0:
                date_for_insert = '1970-01-01 00:00:00'
            else:
                date_for_insert = result[0]['DateTime']

            if not Error:
                await update_last_date_time(database.uuid, date_for_insert)
        
        await asyncio.sleep(30)


async def run_mutation_delete_unstored_days(database):
    border = datetime.datetime.utcnow() - datetime.timedelta(days=database.stored_days)
    query = f"alter table {database.DBName}.EventLogItems DELETE WHERE DateTime < toStartOfDay(toDateTime('{datetime.datetime.strftime(border, '%Y-%m-%d %H:%M:%S')}'))"
    result, error = await clickhouse_query(query)
    if error:
        tools.logging(f'    ERROR. Base: [{database.DBName}]. Deleting old data was failed.')
    else:
        tools.logging(f'    Base [{database.DBName}]. Mutation delete unstored days stared.')


async def delete_unstored_days():

    tools.logging('start delete_unstored_days')

    databases = DataBases.select().where(DataBases.table_name == 'EventLogItems' and DataBases.stored_days != 0)
    for database in databases:
        await run_mutation_delete_unstored_days(database)

        # т.к. операции дорогостоящие, делаем паузу между мутациями
        await asyncio.sleep(30)

    tools.logging('end delete_unstored_days')

# JSON settings for DataBases
# stored_days - integer - Number of stored days for the base
# changes_to_storage - bool - Save changes to persistent storage
# delete_conditions - array of string - conditions to delete data from base

async def save_changes_to_HistoryChangesEventLog(database):
    result, error = await clickhouse_query(f'''CREATE TABLE IF NOT EXISTS {database.DBName}.HistoryChangesEventLog
                    (
                        Id Int64 Codec(DoubleDelta, LZ4),
                        DateTime DateTime('UTC') Codec(Delta, LZ4),
                        User LowCardinality(String),
                        User_AD LowCardinality(String),
                        Computer LowCardinality(String),
                        Application LowCardinality(String),
                        Connection Int64 Codec(DoubleDelta, LZ4),
                        Event LowCardinality(String),
                        Metadata LowCardinality(String),
                        DataPresentation String Codec(ZSTD),
                        Session Int64 Codec(DoubleDelta, LZ4)
                    )
                    engine = MergeTree()
                    PARTITION BY (toYYYYMM(DateTime))
                    ORDER BY (DateTime)
                    SETTINGS index_granularity = 8192;''')

    if error:
        tools.logging(f"ERROR. Base [{database.DBName}]. Can't create HistoryChangesEventLog)")

    result, error = await clickhouse_query(f'''
                    INSERT  
                    INTO {database.DBName}.HistoryChangesEventLog  
                    (Id, DateTime, User, Computer, Application, Connection, Event, Metadata, DataPresentation, Session, User_AD) 
                        SELECT * FROM 
                            (SELECT Id, DateTime, User, Computer, Application, Connection, Event, Metadata, DataPresentation, Session, Data_AD.Data as User_AD  FROM {database.DBName}.EventLogItems AS  {database.DBName}
                                LEFT JOIN ( SELECT distinct
                                            User, Data
                                            FROM {database.DBName}.EventLogItems
                                            WHERE toStartOfDay(DateTime) BETWEEN date_add(day, -2, toStartOfDay(now()+{settings.clickhouse.time_offset}))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now()+{settings.clickhouse.time_offset}))) 
                                            AND Event = 'Сеанс.Аутентификация' AND notEmpty(Data) = 1 ) AS Data_AD
                                ON EventLogItems.User = Data_AD.User
                                 WHERE Event = 'Данные.Изменение'
                                 AND notEmpty({database.DBName}.DataPresentation) = 1 AND {database.DBName}.DateTime  BETWEEN date_add(day, -1, toStartOfDay(now()+{settings.clickhouse.time_offset}))  AND date_add(second, 86399, date_add(day, -1, toStartOfDay(now()+{settings.clickhouse.time_offset})))
                                ) AS DataForDay''')

    if error:
        tools.logging(f"E   RROR. Base [{database.DBName}]. Can't insert data to HistoryChangesEventLog")
    else:
        tools.logging(f'    Base [{database.DBName}]. Insert data in HistoryChangesEventLog. Ok.')


async def save_history_change_data():

    tools.logging('start save_history_change_data')

    databases = DataBases.select().where(DataBases.table_name == 'EventLogItems' and DataBases.save_history_changes == True)
    for database in databases:
        await save_changes_to_HistoryChangesEventLog(database)
        await asyncio.sleep(15)

    tools.logging('end save_history_change_data')


async def delete_data_from_storage_by_condition(database, condition):
    query = f'alter table {database.DBName}.EventLogItems DELETE WHERE {condition}'
    result, error = await clickhouse_query(query)

    if error:
        tools.logging(f"    ERROR. Base [{database.DBName}]. Can't delete data on condition [{condition}]")
    else:
        tools.logging(f'    Base [{database.DBName}]. Delete data on condition. Ok.')


async def delete_data_by_conditions():

    tools.logging('start delete_data_by_conditions')

    databases = DataBases.select().where(DataBases.table_name == 'EventLogItems' and DataBases.delete_conditions != '[]')
    for database in databases:
        try:
            conditions = json.loads(database.delete_conditions)
        except:
            tools.logging(f'Condition error on {database.DBName}. {traceback.format_exc()}')
            continue
        for condition in conditions:
            await delete_data_from_storage_by_condition(database, condition)

            await asyncio.sleep(5)

    tools.logging('end delete_data_by_conditions')


async def clear_system_tables():

    tools.logging('start clearing system tables')

    rows = SystemLogTables.select()

    for table in rows:
        query = f"truncate table {table.name}"
        result, error = await clickhouse_query(query)

        if error:
            tools.logging(f'ERROR. System log table [{table.name}] not truncated.')

    tools.logging('end clearing system tables')


async def worker():

    scheduler = AsyncIOScheduler()
    scheduler.add_job(save_history_change_data, "cron", jitter=60, hour='2', minute='0')
    scheduler.add_job(delete_unstored_days, "cron", jitter=60, hour='2', minute='15')
    scheduler.add_job(delete_data_by_conditions, "cron", jitter=60, hour='2', minute='20')
    scheduler.add_job(clear_system_tables, "cron", jitter=600, hour=5, minute='10')
    scheduler.start()

    # For fast run
    await save_history_change_data()
    await delete_data_by_conditions()
    await delete_unstored_days()



@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    loop.create_task(clickhouse_checker())
    loop.create_task(worker())