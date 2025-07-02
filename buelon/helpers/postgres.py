from __future__ import annotations

import os
import json
import base64
import asyncio
import datetime
import contextlib
import uuid
from typing import Union, List, Dict, Any, Optional, Callable, Optional, Callable, Dict, List, Union, AsyncGenerator, Generator

import asyncpg

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errors

import buelon.helpers.persistqueue

try:
    import dotenv
    dotenv.load_dotenv(os.environ.get('ENV_PATH', '.env'))
except ModuleNotFoundError:
    pass


def get_postgres_from_env() -> Postgres:
    """
    Returns a Postgres object with credentials from environment variables.

    If environment variables are not set, it uses default values.

    Returns:
        Postgres: A Postgres object with connection details.
    """
    return Postgres(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=os.environ.get('POSTGRES_PORT', '5432'),
        user=os.environ.get('POSTGRES_USER', 'daniel'),
        password=os.environ.get('POSTGRES_PASSWORD', 'MyPassword123'),
        database=os.environ.get('POSTGRES_DATABASE', 'my_db')
    )


def sort(val):
    """
    Assigns a sorting priority to PostgreSQL data types.

    Args:
        val (str): A PostgreSQL data type.

    Returns:
        int: The sorting priority of the data type.
    """
    _sort = {'text': 0, 'real': 1, 'bigint': 2, 'integer': 3, 'json': 4, 'timestamp': 5, 'date': 6, 'boolean': 7,
             'character(10)': 8, 'character': 8}
    return _sort[val] if val in _sort else 0


def get_type(val):
    """
    Determines the PostgreSQL data type for a given Python value.

    Args:
        val: A Python value of any type.

    Returns:
        str: The corresponding PostgreSQL data type.
    """
    if val is None:
        return 'boolean'  # 'character(10)'
    if isinstance(val, bool):
        return 'boolean'
    if isinstance(val, str):
        return 'text'
    if isinstance(val, float):
        return 'real'
    if isinstance(val, int):
        if -9223372036854775805 > val or val > 9223372036854775805:
            return 'text'
        if -2147483645 > val or val > 2147483645:
            return 'bigint'
        return 'integer'
    if isinstance(val, (list, dict)):
        return 'json'
    if isinstance(val, datetime.datetime):
        return 'timestamptz'  # 'timestamp'
    if isinstance(val, datetime.date):
        return 'date'
    if isinstance(val, bytes):
        return 'bytea'
    return 'text'


def guess_data_type(column: List):
    """
    Guesses the most appropriate data type for a column of values.

    Args:
        column (List): A list of values from a single column.

    Returns:
        str: The guessed PostgreSQL data type for the column.
    """
    v = {get_type(cell) for cell in column}
    v = list(sorted(v, key=lambda a: sort(a)))
    v = v[0]
    return v


def guess_table_schema(table: List[Dict]):
    """
    Guesses the schema for a table represented as a list of dictionaries.

    Args:
        table (List[Dict]): A list of dictionaries representing table rows.

    Returns:
        Dict[str, str]: A dictionary mapping column names to their guessed data types.
    """
    vv = {}
    for row in table:
        for k, v in row.items():
            if k not in vv:
                vv[k] = []
            vv[k].append(v)
    return {k: guess_data_type(v) for k, v in vv.items()}


class Postgres:
    """
    A class to handle PostgreSQL database operations.

    Attributes:
        host (str): The database host.
        port (str): The database port.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
    """
    host: str = 'localhost'
    port: str = '5432'
    user: str = 'daniel'
    password: str = 'MyPassword123'
    database: str = 'my_db'

    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        """
        Initializes the Postgres object with connection details.

        Args:
            host (str, optional): The database host.
            port (str, optional): The database port.
            user (str, optional): The database user.
            password (str, optional): The database password.
            database (str, optional): The database name.
        """
        self.host = host or self.host
        self.port = port or self.port
        self.user = user or self.user
        self.password = password or self.password
        self.database = database or self.database

    @property
    def url(self):
        import urllib.parse
        user = urllib.parse.quote_plus(self.user)
        password = urllib.parse.quote_plus(self.password)
        return f"postgresql://{user}:{password}@{self.host}:{self.port}/{self.database}"
        # return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def connect(self):
        return psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        )

    async def async_connect(self):
        return await asyncpg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    # static value
    __pool_value = {'value': None}

    async def get_pool(self):
        if not self.__pool_value['value']:
            self.__pool_value['value'] = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                timeout=60 * 60
            )
        return self.__pool_value['value']

    @contextlib.asynccontextmanager
    async def async_pool_connection(self, conn=None):
        if conn:
            yield conn
            return

        # pool = await self.get_pool()
        #
        # async with pool.acquire(timeout=15 * 60) as conn:
        #     yield conn

        yield await self.async_connect()


    def query(self, query: str, *args):
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query to execute.
            conn (connection, optional): An existing database connection to use.
            cur (cursor, optional): An existing database cursor to use.

        Returns:
            List[Tuple]: The result of the query.
        """
        with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        ) as conn:
            cur = conn.cursor()

            cur.execute(query, args)
            r = []
            try:
                r = cur.fetchall()
            except psycopg2.ProgrammingError: pass
            conn.commit()

            return r

    async def async_query(self, query: str, *args, conn: asyncpg.Connection | None = None, close_connection: bool = True,):
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query to execute.
            conn (connection, optional): An existing database connection to use.
            cur (cursor, optional): An existing database cursor to use.

        Returns:
            List[Tuple]: The result of the query.
        """
        if not conn:
            async with self.async_pool_connection(conn) as conn:
                return await self.async_query(query, *args, conn=conn)
            # conn = await self.async_connect()
            # asyncpg.connect(
            #     host=self.host,
            #     port=self.port,
            #     user=self.user,
            #     password=self.password,
            #     database=self.database
            # )

        try:
            async with conn.transaction():
                return [list(row) async for row in conn.cursor(query, *args)]
            # return [list(r) for r in (await conn.fetch(query, *args))]
        finally:
            if close_connection:
                await conn.close()
        # with psycopg2.connect(
        #         host=self.host,
        #         port=self.port,
        #         user=self.user,
        #         password=self.password,
        #         database=self.database
        # ) as conn:
        #     cur = conn.cursor()
        #
        #     cur.execute(query, args)
        #     r = []
        #     try:
        #         r = cur.fetchall()
        #     except psycopg2.ProgrammingError: pass
        #     conn.commit()
        #
        #     return r

    def download_table(self, table_name: str = None, columns='*', suffix='', sql=None, stream=False, callback=None, queue: bool = False, row_transform: callable = None) -> Union[list[dict], Generator, buelon.helpers.persistqueue.JsonlPersistentQueue]:
        """
        Downloads data from a PostgreSQL table.

        Args:
            table_name (str, optional): The name of the table to download.
            columns (str, optional): The columns to select. Defaults to '*'.
            suffix (str, optional): Additional SQL to append to the query.
            sql (str, optional): A custom SQL query to execute instead of selecting from a table.

        Returns:
            List[Dict]: A list of dictionaries representing the table rows.
        """
        query = (sql or f'select {columns} from {table_name} {suffix};')

        with psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        ) as conn:
            cur = conn.cursor()

            cur.execute(query)

            column_names = tuple(col[0] for col in cur.description)

            if callable(row_transform):
                def convert_row_to_dict(row):
                    return row_transform(dict(zip(column_names, self.check_values(row))))
            else:
                def convert_row_to_dict(row):
                    return dict(zip(column_names, self.check_values(row)))

            if not stream and callback is None and not queue:
                return [convert_row_to_dict(row) for row in cur.fetchall()]
            else:
                def gen():
                    for row in cur:
                        yield convert_row_to_dict(row)

                if queue:
                    q = buelon.helpers.persistqueue.JsonlPersistentQueue()

                    for row in gen():
                        q.put(row)

                    return q

                if stream:
                    return gen()

                if not callable(callback):
                    raise ValueError('batch must be callable')

                for row in gen():
                    callback(row)

    @classmethod
    def datetime_to_string(cls, dt: datetime.datetime) -> str:
        return 'datetime::' + dt.isoformat()

    @classmethod
    def date_to_string(cls, dt: datetime.date) -> str:
        return 'date::' + dt.isoformat()

    @classmethod
    def string_to_datetime(cls, s: str) -> datetime.datetime | datetime.date | bytes | str:
        if s.startswith('datetime::'):
            s = s[10:]
            return datetime.datetime.fromisoformat(s)
        if s.startswith('date::'):
            s = s[6:]
            return datetime.datetime.fromisoformat(s).date()
        if s.startswith('bytes::'):
            s = s[7:]
            return bytes.fromhex(s)
        return s

    @classmethod
    def transform_row_to_json_friendly(cls, row):
        for k in row:
            if isinstance(row[k], datetime.datetime):
                row[k] = cls.datetime_to_string(row[k])
            elif isinstance(row[k], datetime.date):
                row[k] = cls.date_to_string(row[k])
            elif isinstance(row[k], memoryview):
                row[k] = bytes(row[k])
            if isinstance(row[k], bytes):
                row[k] = 'bytes::' + row[k].hex()
        return row

    @classmethod
    def transform_row_from_json_friendly(cls, row):
        for k in row:
            if isinstance(row[k], str):
                row[k] = cls.string_to_datetime(row[k])
        return row

    async def async_download_table(
            self,
            table_name: Optional[str] = None,
            columns: str = '*',
            suffix: str = '',
            sql: Optional[str] = None,
            stream: bool = False,
            callback: Optional[Callable] = None,
            conn: asyncpg.Connection | None = None,
            close_connection: bool = True,
            queue: bool = False,
            row_transform: callable = None
    ) -> Union[List[Dict], AsyncGenerator, buelon.helpers.persistqueue.JsonlPersistentQueue]:
        """
        Downloads data from a PostgreSQL table asynchronously.

        Args:
            table_name (str, optional): The name of the table to download.
            columns (str, optional): The columns to select. Defaults to '*'.
            suffix (str, optional): Additional SQL to append to the query.
            sql (str, optional): A custom SQL query to execute instead of selecting from a table.
            stream (bool, optional): Whether to stream results. Defaults to False.
            callback (callable, optional): Function to call for each row when not streaming.

        Returns:
            Union[List[Dict], AsyncGenerator]: Either a list of dictionaries representing the table rows,
            or an async generator if streaming is enabled.
        """
        query = (sql or f'select {columns} from {table_name} {suffix};')

        # if not conn:
            # conn = await self.async_connect()
            # asyncpg.connect(
            #     host=self.host,
            #     port=self.port,
            #     user=self.user,
            #     password=self.password,
            #     database=self.database
            # )
        try:
            if not stream and callback is None and not queue:
                # Fetch all rows at once
                async with self.async_pool_connection(conn) as conn:
                    rows = await conn.fetch(query)
                    if callable(row_transform):
                        return [row_transform(dict(row)) for row in rows]
                    return [dict(row) for row in rows]
                # async with conn.transaction():
                #     return [dict(row) async for row in conn.cursor(query)]
            else:
                # Create an async generator for streaming
                close_connection = False
                async def gen():
                    nonlocal conn
                    async with self.async_pool_connection(conn) as conn:
                        try:
                            if callable(row_transform):
                                async for row in self.stream_with_server_cursor(conn, query):
                                    yield row_transform(dict(row))
                            else:
                                async for row in self.stream_with_server_cursor(conn, query):
                                    yield dict(row)
                            # async with conn.transaction():
                            #     if callable(row_transform):
                            #         # async for row in conn.cursor(query):
                            #         #     yield row_transform(dict(row)) if callable(row_transform) else dict(row)
                            #         async with conn.cursor(query, prefetch=2000) as cursor:
                            #             async for row in cursor:
                            #                 yield row_transform(dict(row))
                            #     else:
                            #         # async for row in conn.cursor(query):
                            #         #     yield dict(row)
                            #         async with conn.cursor(query, prefetch=2000) as cursor:
                            #             async for row in cursor:
                            #                 yield dict(row)
                        finally:
                            if close_connection:
                                await conn.close()

                if queue:
                    q = buelon.helpers.persistqueue.JsonlPersistentQueue()

                    try:
                        async for row in gen():
                            q.put(row)
                    except:
                        q.delete_file()
                        raise

                    return q

                if stream:
                    return gen()

                if not callable(callback):
                    raise ValueError('callback must be callable')

                # Process rows with callback
                async for row in gen():
                    callback(row)

        finally:
            if close_connection:
                await conn.close()

    @classmethod
    def unique_name(cls):
        return f'c{uuid.uuid4().hex}'

    async def stream_with_server_cursor(self, connection, query, chunk_size=2000):
        # Start transaction
        async with connection.transaction():
            # Declare the cursor
            cursor_name = self.unique_name()

            await connection.execute(
                f"DECLARE {cursor_name} NO SCROLL CURSOR WITHOUT HOLD FOR {query}"
            )

            while True:
                # Fetch next batch of rows
                rows = await connection.fetch(
                    f"FETCH {chunk_size} FROM {cursor_name}"
                )
                if not rows:  # No more rows
                    break

                for row in rows:
                    yield row

    def check_values(self, values):
        def check_value(value):
            if isinstance(value, str):
                value = value.replace("''", "'")
            return value

        return [check_value(value) for value in values]

    def upload_table(
            self,
            table_name: str,
            table: List[Dict],
            partition: str | None = None,
            partition_type: str | None = 'LIST',
            partition_query: str | None = None,
            id_column=None,
            check: bool = True,
            sql_prefix: str | None = None,
    ) -> None:
        """
        Uploads data to a PostgreSQL table.

        Args:
            table_name (str): The name of the table to upload to.
            table (List[Dict]): The data to upload, as a list of dictionaries.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            partition_query (str, optional): A custom SQL query to execute for partitioning.
                EXAMPLE: "CREATE TABLE IF NOT EXISTS "table_name_val" PARTITION OF "table_name" FOR VALUES IN ('val');"
            id_column (str, optional): The name of the ID column.
            check (bool): Whether to perform checks on data and table structure and apply changes to db
            sql_prefix (str, optional): A prefix to add to the SQL query.

        Returns:
            None
        """
        def convert_value(value):
            if isinstance(value, str):
                return value.replace("'", "''")
            if isinstance(value, (int, float, bool, type(None), datetime.datetime)):
                return value
            elif isinstance(value, (tuple, list, dict)):
                return f"{json.dumps(value)}"
            elif isinstance(value, (datetime.datetime, datetime.date)):
                return value
            elif isinstance(value, bytes):
                return value
            raise ValueError(f'cannot place {value} in type "{type(value)}"')
        # check for multiple id_columns
        ids = tuple(set(id_column)) if isinstance(id_column, (set, tuple, list)) else (id_column, )
        id_column = '", "'.join(set(id_column)) if isinstance(id_column, (set, tuple, list)) else id_column

        # get all headers
        keys = {}
        for row in table:
            for key in row:
                if key not in keys:
                    keys[key] = None

        # put headers in order, giving None values to missing headers
        for i, row in enumerate(table):
            table[i] = {k: convert_value(row.get(k, None)) for k in keys}

        with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        ) as conn:
            cur = conn.cursor()
            if check:
                self.check_for_table(conn, cur, table_name, table, id_column, partition, partition_type)

            if sql_prefix:
                cur.execute(sql_prefix)
                conn.commit()

            if partition_query:
                try:
                    cur.execute(partition_query)
                    conn.commit()
                except psycopg2.errors.DuplicateTable:
                    conn.rollback()
                    for _query in partition_query.split(';'):
                        try:
                            cur.execute(_query)
                            conn.commit()
                        except psycopg2.errors.DuplicateTable:
                            conn.rollback()

            keys = ', '.join([f'"{k}"' for k in table[0].keys()])
            vals = ', '.join(['%s'] * len(table[0].keys()))
            q = f'INSERT INTO {table_name} ({keys}) VALUES ({vals})'
            if id_column is not None:
                f = {*ids}
                if partition:
                    f.add(partition)
                def _filter(k):
                    return k not in f  # {id_column, partition}  # k != id_column
                conflict = f'("{id_column}")' if not partition or partition == id_column else f'("{id_column}", "{partition}")'
                pers = ['%s'] * (len(tuple(filter(_filter, table[0].keys()))))  # - (1 + (not not partition)))
                p1, p2 = '(' if len(pers) > 1 else '', ')' if len(pers) > 1 else ''
                q = f'''INSERT INTO {table_name} ({keys}) VALUES ({vals})
            ON CONFLICT {conflict} DO UPDATE SET {p1}{', '.join([f'"{k}"' for k in filter(_filter, table[0].keys())])}{p2} = {p1}{', '.join(pers)}{p2};'''
                table = (tuple(list(row.values()) + [v for k, v in row.items() if _filter(k)  # k != id_column
                                                     ]) for row in table)

            else:
                table = (tuple(row.values()) for row in table)
            # table = (tuple(row.values()) for row in table)
            try:
                psycopg2.extras.execute_batch(cur, q, table)
            except psycopg2.errors.InFailedSqlTransaction:
                conn.rollback()
                raise

            conn.commit()

    async def async_upload_table(
            self,
            table_name: str,
            table: List[Dict],
            partition: Optional[str] = None,
            partition_type: str = 'LIST',
            partition_query: Optional[str] = None,
            id_column: Optional[Union[str, List[str], set[str], tuple[str]]] = None,
            check: bool = True,
            conn: asyncpg.Connection | None = None,
            close_connection: bool = True,
    ) -> None:
        """
        Uploads data to a PostgreSQL table asynchronously.

        Args:
            table_name (str): The name of the table to upload to.
            table (List[Dict]): The data to upload, as a list of dictionaries.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            partition_query (str, optional): A custom SQL query to execute for partitioning.
                EXAMPLE: "CREATE TABLE IF NOT EXISTS "table_name_val" PARTITION OF "table_name" FOR VALUES IN ('val');"
            id_column (Union[str, List[str], Set[str], Tuple[str]], optional): The name of the ID column(s).
            check (bool): Whether to perform checks on data and table structure and apply changes to db
            conn (asyncpg.Connection, optional): A connection to the postgres server
            close_connection (bool): Wether to close the connection or not
        """

        def convert_value(value):
            if isinstance(value, str):
                return value.replace("'", "''")
            if isinstance(value, (int, float, bool, type(None), datetime.datetime)):
                return value
            elif isinstance(value, (tuple, list, dict)):
                return json.dumps(value)
            elif isinstance(value, (datetime.datetime, datetime.date)):
                return value
            elif isinstance(value, bytes):
                return value
            raise ValueError(f'cannot place {value} in type "{type(value)}"')

        # check for multiple id_columns
        ids = tuple(set(id_column)) if isinstance(id_column, (set, tuple, list)) else (id_column,)
        id_column = '", "'.join(set(id_column)) if isinstance(id_column, (set, tuple, list)) else id_column

        # get all headers
        keys = {}
        for row in table:
            for key in row:
                if key not in keys:
                    keys[key] = None

        # put headers in order, giving None values to missing headers
        for i, row in enumerate(table):
            table[i] = {k: convert_value(row.get(k, None)) for k in keys}

        if not conn:
            async with self.async_pool_connection(conn) as conn:
                return await self.async_upload_table(
                    table_name,
                    table,
                    partition,
                    partition_type,
                    partition_query,
                    id_column,
                    check,
                    conn,
                )
            # conn = await self.async_connect()
            # asyncpg.connect(
            #     host=self.host,
            #     port=self.port,
            #     user=self.user,
            #     password=self.password,
            #     database=self.database
            # )

        try:
            if check:
                await self.async_check_for_table(conn, table_name, table, id_column, partition, partition_type)

            if partition_query:
                for query in partition_query.split(';'):
                    if not query.strip():
                        continue
                    try:
                        await conn.execute(query)
                    except asyncpg.exceptions.DuplicateTableError:
                        continue

            keys = ', '.join([f'"{k}"' for k in table[0].keys()])
            vals = ', '.join(['$' + str(i + 1) for i in range(len(table[0].keys()))])

            base_query = f'INSERT INTO {table_name} ({keys}) VALUES ({vals})'

            if id_column is not None:
                f = {*ids}
                if partition:
                    f.add(partition)

                def _filter(k):
                    return k not in f

                conflict = f'("{id_column}")' if not partition or partition == id_column else f'("{id_column}", "{partition}")'
                filtered_keys = list(filter(_filter, table[0].keys()))
                update_vals = ', '.join(
                    [f'"{k}" = ${i}' for i, k in enumerate(filtered_keys, len(table[0].keys()) + 1)])

                query = f'{base_query} ON CONFLICT {conflict} DO UPDATE SET {update_vals}'

                # Prepare data for insertion with conflict handling
                data = []
                for row in table:
                    values = list(row.values())
                    filtered_values = [v for k, v in row.items() if _filter(k)]
                    data.append(values + filtered_values)
            else:
                query = base_query
                data = [list(row.values()) for row in table]

            # Use asyncpg's executemany for batch insertion
            async with conn.transaction():
                await conn.executemany(query, data)

        finally:
            if close_connection:
                await conn.close()

    def table_schema(self, table_name: str, cur):
        """
        Retrieves the schema of a table.

        Args:
            table_name (str): The name of the table.
            cur: A database cursor.

        Returns:
            List[Tuple]: A list of tuples containing column names and data types.
        """
        query = f"SELECT column_name, data_type FROM information_schema.columns where table_name = '{table_name}' ORDER BY ordinal_position;"
        cur.execute(query)
        return cur.fetchall()  # tuple(map(lambda row: row[0], cur.fetchall()))

    def check_for_table(self, conn, cur, table_name: str, table: List[Dict], id_column=None, partition: str = None,
                        partition_type: str = 'LIST', skip_alterations=False):
        """
        Checks if a table exists and creates or alters it as necessary.

        Args:
            conn: A database connection.
            cur: A database cursor.
            table_name (str): The name of the table to check.
            table (List[Dict]): The data to be inserted into the table.
            id_column (str, optional): The name of the ID column.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            skip_alterations (bool, optional): If True, skips table alterations. Defaults to False.
        """
        schema = guess_table_schema(table)

        cur.execute(
            f"SELECT EXISTS (SELECT 1 FROM pg_tables  WHERE tablename = '{table_name}') AS table_existence;")
        table_exists = cur.fetchall()[0][0]

        if not table_exists:
            try:
                primary = ''
                unique = ''
                if partition:
                    if id_column:
                        if id_column == partition or partition in [s.strip() for s in id_column.replace('"', '').split(',')]:
                            unique = f', PRIMARY KEY ("{id_column}")'
                        else:
                            unique = f', PRIMARY KEY ("{id_column}", "{partition}")'
                else:
                    primary = ' primary key UNIQUE'
                    if id_column:
                        unique = f', UNIQUE ("{id_column}")'

                end = ';' if not isinstance(partition, str) else f' PARTITION BY {partition_type} ("{partition}");'

                def column_and_data_type(k, v):
                    if k == id_column and not partition:
                        return f'"{k}" {v}{primary}'
                    return f'"{k}" {v}'

                sql = f'''create table if not exists {table_name}({", ".join([column_and_data_type(k, v) for k, v in schema.items()])}{unique}){end}'''

                cur.execute(sql)
                conn.commit()
            except psycopg2.errors.UniqueViolation as e:
                conn.rollback()
                print(e)
        elif not skip_alterations:
            db_schema = dict(self.table_schema(table_name, cur))
            conn.commit()

            for col in set(schema.keys()) ^ set(db_schema.keys()):
                if col in schema:
                    try:
                        cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {schema[col]};')
                        conn.commit()
                    except psycopg2.errors.DuplicateColumn as e:
                        conn.rollback()
                        print(e)
                elif col in db_schema:
                    pass

            for col in set(schema.keys()) & set(db_schema.keys()):
                if schema[col] != db_schema[col]:
                    if sort(schema[col]) < sort(db_schema[col]):
                        # print(f'comparing {sort(schema[col])} < {sort(db_schema[col])} == sort({schema[col]}) < sort({db_schema[col]})')
                        self.convert_table_column_type(table_name, f'"{col}"', schema[col], conn, cur)
                    elif sort(schema[col]) > sort(db_schema[col]):
                        pass

    async def async_check_for_table(
            self,
            conn: asyncpg.Connection,
            table_name: str,
            table: List[Dict],
            id_column: Optional[str] = None,
            partition: Optional[str] = None,
            partition_type: str = 'LIST',
            skip_alterations: bool = False
    ) -> None:
        """
        Checks if a table exists and creates or alters it as necessary.

        Args:
            conn: An asyncpg connection.
            table_name (str): The name of the table to check.
            table (List[Dict]): The data to be inserted into the table.
            id_column (str, optional): The name of the ID column.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            skip_alterations (bool, optional): If True, skips table alterations. Defaults to False.
        """
        schema = guess_table_schema(table)

        # Check if table exists
        table_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1)",
            table_name
        )

        if not table_exists:
            try:
                primary = ''
                unique = ''
                if partition:
                    if id_column:
                        if id_column == partition or partition in [s.strip() for s in
                                                                   id_column.replace('"', '').split(',')]:
                            unique = f', PRIMARY KEY ("{id_column}")'
                        else:
                            unique = f', PRIMARY KEY ("{id_column}", "{partition}")'
                else:
                    primary = ' primary key UNIQUE'
                    if id_column:
                        unique = f', UNIQUE ("{id_column}")'

                end = ';' if not isinstance(partition, str) else f' PARTITION BY {partition_type} ("{partition}");'

                def column_and_data_type(k, v):
                    if k == id_column and not partition:
                        return f'"{k}" {v}{primary}'
                    return f'"{k}" {v}'

                sql = f'''CREATE TABLE IF NOT EXISTS {table_name}
                         ({", ".join([column_and_data_type(k, v) for k, v in schema.items()])}{unique}){end}'''

                await conn.execute(sql)

            except asyncpg.UniqueViolationError as e:
                print(e)
                raise

        elif not skip_alterations:
            async with conn.transaction():
                # Get existing schema
                db_schema = dict(await self.async_table_schema(table_name, conn))

                # Add missing columns
                for col in set(schema.keys()) ^ set(db_schema.keys()):
                    if col in schema:
                        try:
                            alter_sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" {schema[col]};'
                            await conn.execute(alter_sql)
                        except asyncpg.DuplicateColumnError as e:
                            print(e)

                # Check for type conversions
                for col in set(schema.keys()) & set(db_schema.keys()):
                    if schema[col] != db_schema[col]:
                        if sort(schema[col]) < sort(db_schema[col]):
                            await self.async_convert_table_column_type(
                                table_name, f'"{col}"', db_schema[col], schema[col], conn)

    async def async_table_schema(self, table_name: str, conn: asyncpg.Connection) -> List[tuple]:
        """
        Retrieves the schema of a table.

        Args:
            table_name (str): The name of the table.
            cur: A database cursor.

        Returns:
            List[Tuple]: A list of tuples containing column names and data types.
        """
        query = f"SELECT column_name, data_type FROM information_schema.columns where table_name = '{table_name}' ORDER BY ordinal_position;"
        # cur.execute(query)
        # return cur.fetchall()  # tuple(map(lambda row: row[0], cur.fetchall()))
        return await conn.fetch(query)

    def convert_table_column_type(self, table_name: str, column_name: str, new_type: str, conn, cur):
        """
        Converts the data type of a table column.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column to convert.
            new_type (str): The new data type for the column.
            conn: A database connection.
            cur: A database cursor.
        """
        base_values = {'text': "''", 'real': '0', 'bigint': '0', 'integer': '0', 'json': "''",
                     'timestamp': datetime.datetime.utcnow(), 'date': "'2023-01-01'", 'boolean': 'false', 'character(10)': "''"}
        conn.commit()
        try:
            cur.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};')
            conn.commit()
        except psycopg2.errors.DatatypeMismatch as e:
            base_value = base_values[new_type]
            try:
                conn.rollback()
                cur.execute(f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING (nullif({column_name}, {base_value}))::{new_type};')
                conn.commit()
            except:  # (psycopg2.errors.InFailedSqlTransaction, psycopg2.errors.CannotCoerce) as e:
                conn.rollback()
                cur.execute(f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING {column_name}::text::{new_type};')
                conn.commit()

    async def async_convert_table_column_type(
            self,
            table_name: str,
            column_name: str,
            old_type: str,
            new_type: str,
            conn: asyncpg.Connection
    ) -> None:
        """
        Converts the data type of a table column asynchronously.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column to convert.
            new_type (str): The new data type for the column.
            conn (asyncpg.Connection): An asyncpg connection object.
        """
        base_values = {
            'text': "''",
            'real': '0',
            'bigint': '0',
            'integer': '0',
            'json': "''",
            'timestamp': f"'{datetime.datetime.utcnow()}'",
            'date': "'2023-01-01'",
            'boolean': 'false',
            'character(10)': "''"
        }

        # First attempt: Direct conversion
        try:
            async with conn.transaction():
                await conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};'
                    # f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};'
                )
                return
        except asyncpg.exceptions.DatatypeMismatchError:
            # Second attempt: Use nullif with base value
            try:

                base_value = base_values[new_type]
                async with conn.transaction():
                    await conn.execute(
                        # f'ALTER TABLE {table_name} '
                        # f'ALTER COLUMN {column_name} '
                        # f'TYPE {new_type} '
                        # f'USING (nullif({column_name}, {base_value}))::{new_type};'
                        f'ALTER TABLE {table_name} '
                        f'ALTER COLUMN {column_name} '
                        f'TYPE {new_type} '
                        f'USING (nullif({column_name}, {base_value}))::{new_type};'
                    )
                    return
            except:# (asyncpg.PostgresError, KeyError, asyncpg.InvalidTextRepresentationError, asyncpg.UndefinedFunctionError, Exception) as e:
                # print(type(e), e)
                # Final attempt: Convert through text
                try:
                    async with conn.transaction():
                        await conn.execute(
                            # f'ALTER TABLE {table_name} '
                            # f'ALTER COLUMN {column_name} '
                            # f'TYPE {new_type} '
                            # f'USING {column_name}::text::{new_type};'
                            f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING {column_name}::text::{new_type};'
                        )
                except:
                    if old_type not in {'bool', 'boolean'}:
                        raise
                    async with conn.transaction():
                        await conn.execute(
                            # f'ALTER TABLE {table_name} '
                            # f'ALTER COLUMN {column_name} '
                            # f'TYPE {new_type} '
                            # f'USING {column_name}::text::{new_type};'
                            f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING (case when {column_name} then \'1\' else \'0\' end)::{new_type};'
                        )


