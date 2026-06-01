import os
import inspect
import uuid
import sqlite3
import time

import buelon.helpers.sqlite3_helper
import buelon.helpers.postgres

DEBUG_TABLE = 'debug'

# pg = buelon.helpers.postgres.get_postgres_from_env()


def counter(_id: str, change: int | float = 1, table_name: str = 'counter'):
    """
    a counter that can be used to track the progress of a task

    Args:
        _id: the id of the counter
        change (default: 1): the amount to change the counter by
        table_name (default: 'counter'): the name of the table to use

    Returns:
        the current count of the counter
    """
    db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
    try:
        db.query(f'update {table_name} set count = count + {change} where id = ?;', _id)
        t = db.download_table(sql=f'select * from {table_name} where id = ?;', parameters=(_id,))
        # pg.query(f'update {table_name} set count = count + {change} where id = \'{_id}\';')
        # t = pg.download_table(sql=f'select * from {table_name} where id = \'{_id}\';')
    except:
        t = []
    if not t:
        t.append({'id': _id, 'count': change})
        db.upload_table(table_name, t, id_column='id')
        # pg.upload_table(table_name, t, id_column='id')
        return change
    row = t[0]
    # row['count'] += change
    # db.upload_table(table_name, t, id_column='id')
    return row['count']


def timeit(func: callable):
    return func
    module_name, func_name = os.path.basename(inspect.getmodule(func).__file__).split('.')[0], func.__name__

    def inner(*args, **kwargs):
        t = time.time()
        v = func(*args, **kwargs)
        counter(f'{module_name}.{func_name}', time.time() - t, DEBUG_TABLE)
        return v

    async def inner_async(*args, **kwargs):
        t = time.time()
        if inspect.isasyncgenfunction(func):
            async def gen():
                nonlocal t
                async for v in func(*args, **kwargs):
                    yield v
                counter(f'{module_name}.{func_name}', time.time() - t, DEBUG_TABLE)

            return gen()
        v = await func(*args, **kwargs)
        counter(f'{module_name}.{func_name}', time.time() - t, DEBUG_TABLE)
        return v

    return inner if not inspect.iscoroutinefunction(func) else inner_async


def time_from_here():
    t = time.time()
    db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
    i = f'{uuid.uuid4()}'
    db.upload_table('tags', [{'id': i, 'time': t}], id_column='id')
    return i


def time_to_here(_id: str, tag: str):
    db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
    t = []
    try:
        t = db.download_table('delete from tags where id = ? returning *', _id)
    except sqlite3.OperationalError:
        pass
    if t:
        counter(tag, time.time() - t[0]['time'], DEBUG_TABLE)
