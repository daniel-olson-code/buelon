"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.
"""

import socket
import threading
import os
import sys
import time

# import psutil
import buelon.settings
# `from ... import` rather than the dotted form: `buelon/__init__.py` imports this
# module while it is still executing, so `buelon.helpers` is not an attribute yet.
from buelon.helpers.optional import optional_import, is_available
import buelon.helpers.postgres
import buelon.helpers.created_cache

# Postgres is the bucket's only backend beyond plain files, and it ships as
# `buelon[bucket]` / `buelon[postgres]` rather than in the base install
# (BUGS.md #58). `buelon/__init__.py` imports this module eagerly, so the
# import must not hard-fail; the stub raises on first use instead.
psycopg2 = optional_import('psycopg2', 'bucket', submodules=('extras', 'errors'))

try:
    import dotenv
    dotenv.load_dotenv(os.environ.get('ENV_PATH', '.env'))
except ModuleNotFoundError:
    pass

# Environment variables for client and server configuration
USING_POSTGRES: bool = os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

PERSISTENT_PATH: str = f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = os.path.join(buelon.settings.DIR_PATH, 'bucket')

database: dict[str, bytes] = {}
database_keys_in_order = []
# MAX_DATABASE_SIZE: int = min(1024 * 1024 * 1024 * 1, int(psutil.virtual_memory().total / 8))
MAX_DATABASE_SIZE: int = 50 * 1024 * 1024

# if not USING_POSTGRES:
#     if not os.path.exists(save_path):
#         os.makedirs(save_path)


def receive(conn: socket.socket, size: int = 1024) -> bytes:
    """
    Receive data from a socket connection until the end marker is found.

    Args:
        conn (socket.socket): The socket connection to receive data from.
        size (int, optional): The maximum number of bytes to receive at once. Defaults to 1024.

    Returns:
        bytes: The received data without the end marker.
    """
    data = b''
    while not data.endswith(BUCKET_END_TOKEN):
        v = conn.recv(size)
        data += v
    token_len = len(BUCKET_END_TOKEN)
    return data[:-token_len]


def send(conn: socket.socket, data: bytes) -> None:
    """
    Send data through a socket connection with an end marker.

    Args:
        conn (socket.socket): The socket connection to send data through.
        data (bytes): The data to be sent.
    """
    conn.sendall(data + BUCKET_END_TOKEN)


def operational_errors() -> tuple:
    """
    The driver errors worth retrying, or an empty tuple if it is not installed.

    An `except` clause is evaluated when an exception reaches it, not at
    def time -- so a bare `except psycopg2.OperationalError` would ask the
    missing-dependency stub for an attribute and raise ModuleNotFoundError
    *over the top of* whatever error the file-mode bucket was actually
    reporting. `except ():` matches nothing, which is the behaviour we want
    on a base install (BUGS.md #58).
    """
    if is_available(psycopg2):
        return (psycopg2.OperationalError,)
    return ()


def retry_connection(func: callable):

    """
    Decorator to retry a function call if a ConnectionResetError occurs.

    Args:
        func (callable): The function to be decorated.

    Returns:
        callable: The decorated function.
    """
    def wrapper(self, *args, **kwargs) -> bytes | None:
        tries = 4
        kwargs['timeout'] = kwargs.get('timeout', 60 * 5.)
        for i in range(tries):
            try:
                return func(self, *args, **kwargs)
            except (TimeoutError, ConnectionResetError):
                kwargs['timeout'] *= 2
            except operational_errors():
                time.sleep((i + 1) * 5.)
                # self.db = buelon.helpers.postgres.get_postgres_from_env()
        raise
    return wrapper


class Client:
    """A client for interacting with the bucket server."""
    PORT: int
    HOST: str

    db: buelon.helpers.postgres.Postgres

    def __init__(self, override_db: buelon.helpers.postgres.Postgres | None = None):
        """Initialize the client with host and port from environment variables."""
        self.PORT = BUCKET_CLIENT_PORT
        self.HOST = BUCKET_CLIENT_HOST

        if USING_POSTGRES:
            self.db = (override_db if override_db else buelon.helpers.postgres.get_postgres_from_env())
            try:
                with buelon.helpers.created_cache.AlreadyCreated(f'bucket_{POSTGRES_TABLE}') as obj:
                    if not obj.created:
                        with self.db.connect() as conn:
                            cur = conn.cursor()
                            cur.execute(f'CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (key TEXT PRIMARY KEY, data BYTEA, epoch REAL);')
                            if '.' in POSTGRES_TABLE:
                                args = POSTGRES_TABLE.split('.')
                                schema = args[-2]
                                table_name = args[-1]
                            else:
                                schema = 'public'
                                table_name = POSTGRES_TABLE
                            # cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_key ON {POSTGRES_TABLE} USING hash (key);')
                            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name} ON {POSTGRES_TABLE} USING hash (key);')
                            conn.commit()
            except psycopg2.errors.InsufficientPrivilege:
                print('Insufficient privileges to use postgres bucket.')

    def __getattr__(self, item):
        if item == 'db':
            if USING_POSTGRES:
                if not hasattr(self, 'db'):
                    v = buelon.helpers.postgres.get_postgres_from_env()
                    self.db = v
                    return v

    def __enter__(self):
        return self

    def __del__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @retry_connection
    def set(self, key: str, data: bytes, timeout: float | None = 60 * 5., persistent: bool = False) -> None:
        """
        Set data for a given key on the server.

        Args:
            key (str): The key to associate with the data.
            data (bytes): The data to store.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.
        """
        if persistent:
            key = PERSISTENT_PATH + '/' + key  # f'{PERSISTENT_PATH}/{key}'
        if USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'INSERT INTO {POSTGRES_TABLE} (key, data, epoch) VALUES (%s, %s, %s) '
                             'ON CONFLICT (key) DO UPDATE SET (data, epoch) = (EXCLUDED.data, EXCLUDED.epoch)', (key, data, time.time()))
                conn.commit()
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.settimeout(timeout)
                s.connect((self.HOST, self.PORT))
                if len(data) < 2048:
                    send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'set', f'{timeout}'.encode(), data]))
                    receive(s)
                else:
                    send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'big-set', f'{timeout}'.encode(), f'{len(data)}'.encode()]))
                    receive(s)
                    send(s, data)
                    receive(s)

    @retry_connection
    def get(self, key: str, timeout: float | None = 60 * 5., persistent: bool = False) -> bytes | None:
        """
        Retrieve data for a given key from the server.

        Args:
            key (str): The key to retrieve data for.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.

        Returns:
            bytes or None: The retrieved data, or None if the key doesn't exist.
        """
        if persistent:
            key = PERSISTENT_PATH + '/' + key  # f'{PERSISTENT_PATH}/{key}'
        if USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'SELECT data FROM {POSTGRES_TABLE} WHERE key = %s', (key,))
                data = cur.fetchone()
                if not data:
                    return None
                return bytes(data[0])
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(timeout)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'get', f'{timeout}'.encode(), b'__null__']))

            data: bytes = receive(s)
            if data[:len(b'__big__')] == b'__big__':
                size = int(data[len(b'__big__'):])
                send(s, b'ok')
                data = receive(s, size)
                return data
            if data == b'__null__':
                return None
            return data

    async def async_get(self, key: str, timeout: float | None = 60 * 5., persistent: bool = False) -> bytes | None:
        """
        Retrieve data for a given key from the server.

        Args:
            key (str): The key to retrieve data for.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.

        Returns:
            bytes or None: The retrieved data, or None if the key doesn't exist.
        """
        if persistent:
            key = PERSISTENT_PATH + '/' + key  # f'{PERSISTENT_PATH}/{key}'
        if USING_POSTGRES:
            aconn = await self.db.async_connect()
            rows = await aconn.fetch(f'SELECT data FROM {POSTGRES_TABLE} WHERE key = $${key}$$')
            if not rows:
                return None
            return rows[0][0]  # bytes(tuple(rows[0])[0])
        raise ValueError('Not implemented yet.')

    @retry_connection
    def delete(self, key: str, timeout: float | None = 60 * 5., persistent: bool = False) -> None:
        """
        Delete data for a given key on the server.

        Args:
            key (str): The key to delete data for.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.
        """
        if persistent:
            key = PERSISTENT_PATH + '/' + key  # f'{PERSISTENT_PATH}/{key}'
        if USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE key = %s', (key, ))
                conn.commit()
            return

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(timeout)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'delete', f'{timeout}'.encode(), b'__null__']))
            receive(s)

    def bulk_get(self, keys: list, save: bool = False, persistent: bool = False):
        output = {}
        if USING_POSTGRES:
            output = {k: None for k in keys}
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'SELECT key, data FROM {POSTGRES_TABLE} WHERE key in %s', (tuple(keys),))
                data = cur.fetchall()

                if not data:
                    return output

                for key, value in data:
                    output[key] = bytes(value)
        else:
            for key in keys:
                output[key] = self.get(key, persistent=persistent)

        return output

    async def async_bulk_get(self, keys: list, save: bool = False, persistent: bool = False):
        output = {}
        if USING_POSTGRES:
            output = {k: None for k in keys}
            ks = str(tuple(keys))
            async for row in await self.db.async_download_table(stream=True, sql=f'SELECT key, data FROM {POSTGRES_TABLE} WHERE key in {ks}'):
                output[row['key']] = bytes(row['data'])
            return output
        else:
            raise ValueError('Not implemented yet.')

        return output

    def bulk_set(self, keys_values: dict, save: bool = False, persistent: bool = False):
        """
        Set multiple key-value pairs on the server.

        Args:
            keys_values (dict): A dictionary of key-value pairs to set.
            save (bool, optional): Whether to save the data to disk. Defaults to False.
        """
        if USING_POSTGRES:
            return self.postgres_bulk_set(keys_values, save)
        else:
            for k, v in keys_values.items():
                self.set(k, v, save=save, persistent=persistent)

    def clean_up(self, object_lifetime: int | float = 60 * 60 * 24 * 7):
        """
        Clean up the database by removing expired keys.
        """
        t = time.time()

        if USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE epoch < {t - object_lifetime} and key not like \'{PERSISTENT_PATH}%\';')
                conn.commit()
        # else:
        #     for root, dirs, files in os.walk(save_path, topdown=True):
        #         dirs[:] = [d for d in dirs if d != PERSISTENT_PATH]
        #         for file in files:
        #             path = os.path.join(root, file)
        #             if (t - os.path.getmtime(path)) > object_lifetime:
        #                 os.remove(path)

    def postgres_bulk_set(self, keys_values: dict, save: bool = False):
        """
        Set multiple key-value pairs in the PostgreSQL database.

        Args:
            keys_values (dict): A dictionary of key-value pairs to set.
            save (bool, optional): Whether to save the data to disk. Defaults to False.
        """
        with self.db.connect() as conn:
            cur = conn.cursor()
            # psycopg2.extras.execute_batch(cur, q, table)
            q = f'''INSERT INTO {POSTGRES_TABLE} (key, data, epoch) VALUES (%s, %s, %s) 
                ON CONFLICT (key) DO UPDATE SET (data, epoch) = (EXCLUDED.data, EXCLUDED.epoch)'''
            table = ((k, v, time.time()) for k, v in keys_values.items())
            psycopg2.extras.execute_batch(cur, q, table)
            # for k, v in keys_values.items():
            #     cur.execute(f'INSERT INTO {POSTGRES_TABLE} (key,data, epoch) VALUES (%s, %s, %s) '
            #                  'ON CONFLICT (key) DO UPDATE SET (data, epoch) = (EXCLUDED.data, EXCLUDED.epoch)', (k, v, time.time()))
            conn.commit()


def check_file_directory(path: str) -> None:
    """
    Ensure the directory for a file path exists, creating it if necessary.

    Args:
        path (str): The file path to check.
    """
    if os.path.dirname(path) == '' or os.path.dirname(path) == '.' or os.path.dirname(path) == '.\\' or os.path.dirname(path) == '/' or os.path.dirname(path) == '\\' or os.path.dirname(path) == './':  # os.path.dirname(path) in {'', '.', '/', '\\', './', '.\\'}:
        return
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


def server_get(key: str) -> bytes | None:
    """
    Retrieve data for a given key from the server.

    Args:
        key (str): The key to retrieve data for.

    Returns:
        bytes or None: The retrieved data, or None if the key doesn't exist.
    """
    global database, save_path
    if key not in database:
        if not os.path.exists(os.path.join(save_path, key)):
            return b'__null__'
        with open(os.path.join(save_path, key), 'rb') as f:
            database[key] = f.read()
    return database[key]


def server_set(key: str, data: bytes) -> None:
    """
    Set data for a given key on the server.

    Args:
        key (str): The key to associate with the data.
        data (bytes): The data to store.
    """
    global database, save_path
    database[key] = data
    # # This is set later
    # check_file_directory(os.path.join(save_path, key))
    # with open(os.path.join(save_path, key), 'wb') as f:
    #     f.write(data)


def server_delete(key: str) -> None:
    """
    Delete data for a given key from the server.

    Args:
        key (str): The key to delete data for.
    """
    global database, save_path
    if key in database:
        del database[key]
    try:
        os.remove(os.path.join(save_path, key))
    except FileNotFoundError:
        pass


def handle_client(conn: socket.socket) -> None:
    """
    Handle a client connection, processing set, get, and delete requests.

    Args:
        conn (socket.socket): The client connection socket.
    """
    global database, save_path
    k: bytes
    m: bytes
    data: bytes
    key: str
    method: str

    def ok():
        send(conn, b'ok')

    with conn:

        k, m, t, data = receive(conn).split(BUCKET_SPLIT_TOKEN)
        key = k.decode('utf-8')
        method = m.decode('utf-8')
        timeout = float(t)

        conn.settimeout(timeout)

        if method == 'big-set':
            size = int(data)
            ok()
            data = receive(conn, size)
            server_set(key, data)
            ok()
        if method == 'set':
            # # ok()
            # # database[key] = data
            # check_file_directory(os.path.join(save_path, key))
            # with open(os.path.join(save_path, key), 'wb') as f:
            #     f.write(data)  # receive(conn))
            server_set(key, data)
            ok()
        elif method == 'get':
            # # if key not in database:
            # #     send(conn, b'__null__')
            # # else:
            # #     send(conn, database[key])
            # if not os.path.exists(os.path.join(save_path, key)):
            #     send(conn, b'__null__')
            # else:
            #     with open(os.path.join(save_path, key), 'rb') as f:
            #         send(conn, f.read())
            data = server_get(key)
            if len(data) < 2048:
                send(conn, server_get(key))
            else:
                send(conn, f'__big__{len(data)}'.encode())
                receive(conn)
                send(conn, data)
        elif method == 'delete':
            # # if key in database:
            # #     del database[key]
            # try:
            #     os.remove(os.path.join(save_path, key))
            # except FileNotFoundError:
            #     pass
            server_delete(key)
            ok()

    if method == 'set' or method == 'big-set':
        check_file_directory(os.path.join(save_path, key))
        with open(os.path.join(save_path, key), 'wb') as f:
            f.write(data)
        database_keys_in_order.append(key)

    while sys.getsizeof(database) > MAX_DATABASE_SIZE:
        key = database_keys_in_order.pop(0) if database_keys_in_order else next(iter(database))
        try:
            del database[key]
        except KeyError:
            pass


class Server:
    """A server for handling bucket storage requests."""
    PORT: int
    HOST: str

    def __init__(self):
        """Initialize the server with host and port from environment variables."""
        self.PORT = BUCKET_SERVER_PORT
        self.HOST = BUCKET_SERVER_HOST

    def loop(self):
        """
        Start the server loop, listening for and handling client connections.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, self.PORT))
            s.listen(10)
            while True:
                try:
                    conn, addr = s.accept()
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    conn.settimeout(60. * 60.)

                    threading.Thread(target=handle_client, args=(conn, )).start()
                except TimeoutError as e:
                    pass


def cleanup(slow=True):
    if USING_POSTGRES:
        postgres = Client()
        # with postgres.db.connect() as conn:
        #     cur = conn.cursor()
        #     cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE epoch + {60 * 60 * 24 * 2} > {time.time()}')
        #     conn.commit()
        return

    for root, dirs, files in os.walk(save_path, topdown=True):
        dirs[:] = [d for d in dirs if d != PERSISTENT_PATH]
        for f in files:
            if os.path.getmtime(os.path.join(root, f)) + 60 * 60 * 24 > time.time():
                os.remove(os.path.join(root, f))
            if slow:
                time.sleep(0.01)



def main() -> None:
    """
    Run the bucket server.
    """
    server: Server = Server()
    print('running bucket server', server.HOST, '@', server.PORT)
    server.loop()


# try:
# except (ImportError, ModuleNotFoundError):
#     pass


if __name__ == '__main__':
    main()

