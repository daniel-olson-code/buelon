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

import psutil
import redis
import psycopg2
from kazoo.client import KazooClient
import kazoo.exceptions
import tqdm

import buelon.helpers.postgres

try:
    import dotenv
    dotenv.load_dotenv('.env')
except ModuleNotFoundError:
    pass


# Environment variables for client and server configuration
USING_POSTGRES: bool = os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

REDIS_HOST: str = os.environ.get('REDIS_HOST', 'null')
REDIS_PORT: int = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB: int = int(os.environ.get('REDIS_DB', 0))
REDIS_EXPIRATION: int | None = os.environ.get('REDIS_EXPIRATION', 60*60*24*7)
try:
    REDIS_EXPIRATION = int(REDIS_EXPIRATION)
except ValueError:
    REDIS_EXPIRATION = None
USING_REDIS: bool = os.environ.get('USING_REDIS', 'false') == 'true'  # False  # REDIS_HOST != 'null'

BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

USING_ZOOKEEPER = os.environ.get('USING_ZOOKEEPER', 'false') == 'true'
ZOOKEEPER_HOSTS: str = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
ZOOKEEPER_PATH: str = f"{os.environ.get('ZOOKEEPER_PATH', '/buelon/bucket')}"

PERSISTENT_PATH: str = f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = os.path.join('.bue', 'bucket')

database: dict[str, bytes] = {}
database_keys_in_order = []
# MAX_DATABASE_SIZE: int = min(1024 * 1024 * 1024 * 1, int(psutil.virtual_memory().total / 8))
MAX_DATABASE_SIZE: int = 50 * 1024 * 1024

if not USING_REDIS and not USING_POSTGRES and not USING_ZOOKEEPER:
    if not os.path.exists(save_path):
        os.makedirs(save_path)


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
            except psycopg2.OperationalError:
                time.sleep((i + 1) * 5.)
                self.db = buelon.helpers.postgres.get_postgres_from_env()
        raise
    return wrapper


def redis_secure_connection(func: callable):
    def wrapper(*args, **kwargs) -> bytes | None:
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError:
            self: Client = args[0]
            try:
                del self.redis_client
            except Exception as e:
                print(f'Change `Exception` to {type(e)}. e: {e}')
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
            return func(*args, **kwargs)
    return wrapper


class Client:
    """A client for interacting with the bucket server."""
    PORT: int
    HOST: str

    db: buelon.helpers.postgres.Postgres

    def __init__(self):
        """Initialize the client with host and port from environment variables."""
        self.PORT = BUCKET_CLIENT_PORT
        self.HOST = BUCKET_CLIENT_HOST

        if USING_ZOOKEEPER:
            self.zk = KazooClient(hosts=ZOOKEEPER_HOSTS)
            self.zk.start(timeout=60 * 15)
            self.zk.ensure_path(ZOOKEEPER_PATH)
        elif USING_POSTGRES:
            self.db = buelon.helpers.postgres.get_postgres_from_env()
            try:
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
        elif USING_REDIS:
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

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
        if USING_ZOOKEEPER:
            try:
                self.zk.stop()
                self.zk.close()
            except kazoo.exceptions.WriterNotClosedException:
                pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if USING_ZOOKEEPER:
            try:
                self.zk.stop()
                self.zk.close()
            except kazoo.exceptions.WriterNotClosedException:
                pass
        elif USING_REDIS:
            self.redis_client.connection.disconnect()

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
        if USING_ZOOKEEPER:
            if not persistent:
                key = 'temp/' + key  # f'temp/{key}'
            key = ZOOKEEPER_PATH + '/' + key  # f'{ZOOKEEPER_PATH}/{key}'
            self.zk.ensure_path(key)
            self.zk.set(key, data)
        elif USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'INSERT INTO {POSTGRES_TABLE} (key, data, epoch) VALUES (%s, %s, %s) '
                             'ON CONFLICT (key) DO UPDATE SET (data, epoch) = (EXCLUDED.data, EXCLUDED.epoch)', (key, data, time.time()))
                conn.commit()
        elif USING_REDIS:
            self.redis_set(key, data, True)
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
        if USING_ZOOKEEPER:
            if not persistent:
                key = 'temp/' + key  # f'temp/{key}'
            key = ZOOKEEPER_PATH + '/' + key  # f'{ZOOKEEPER_PATH}/{key}'
            try:
                return self.zk.get(key)[0]
            except (KeyError, kazoo.exceptions.NoNodeError):
                return None
        elif USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'SELECT data FROM {POSTGRES_TABLE} WHERE key = %s', (key,))
                data = cur.fetchone()
                if not data:
                    return None
                return bytes(data[0])
        if USING_REDIS:
            return self.redis_get(key)
        # raise RuntimeError('Not using redis.')
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
        if USING_ZOOKEEPER:
            if not persistent:
                key = 'temp/' + key  # f'temp/{key}'
            key = ZOOKEEPER_PATH + '/' + key  # f'{ZOOKEEPER_PATH}/{key}'
            self.zk.delete(key)
        elif USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE key = %s', (key, ))
                conn.commit()
            return
        if USING_REDIS:
            self.redis_delete(key)
            return

        # raise RuntimeError('Not using redis.')
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

    def bulk_set(self, keys_values: dict, save: bool = False, persistent: bool = False):
        """
        Set multiple key-value pairs on the server.

        Args:
            keys_values (dict): A dictionary of key-value pairs to set.
            save (bool, optional): Whether to save the data to disk. Defaults to False.
        """
        if USING_ZOOKEEPER:
            for k, v in tqdm.tqdm(keys_values.items(), desc=f'setting {len(keys_values)} paths'):
                self.set(k, v, persistent=persistent)
            # for path in tqdm.tqdm(keys_values, desc=f'ensuring {len(keys_values)} paths'):
            #     self.zk.ensure_path(path)
            # transaction = self.zk.transaction()
            # for k, v in keys_values.items():
            #     transaction.set_data(k, v)
            #     # transaction.create(k, v, makepath=True)
            # results = transaction.commit()
            # print('done uploading paths')
            # # print('results', results)
        elif USING_POSTGRES:
            return self.postgres_bulk_set(keys_values, save)
        elif USING_REDIS:
            return self.redis_bulk_set(keys_values, save)
        else:
            for k, v in keys_values.items():
                self.set(k, v, save=save, persistent=persistent)

    def clean_up(self):
        """
        Clean up the database by removing expired keys.
        """
        t = time.time()
        object_lifetime = 60 * 60 * 24
        if USING_ZOOKEEPER:
            self.zk.delete(ZOOKEEPER_PATH + '/temp', recursive=True)  # (f'{ZOOKEEPER_PATH}/temp', recursive=True)
            self.zk.ensure_path(ZOOKEEPER_PATH + '/temp')  # (f'{ZOOKEEPER_PATH}/temp')
            base_path = ZOOKEEPER_PATH + '/temp'  # f'{ZOOKEEPER_PATH}/temp'
            for child in self.zk.get_children(base_path):
                path = ZOOKEEPER_PATH + '/temp/' + child  # f'{ZOOKEEPER_PATH}/temp/{child}'
                data, config = self.zk.get(path)
                if (t - (config.mtime / 1000)) > object_lifetime:
                    self.zk.delete(path)
        elif USING_POSTGRES:
            with self.db.connect() as conn:
                cur = conn.cursor()
                cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE epoch < {t - object_lifetime} and key not like \'{PERSISTENT_PATH}%\';')
                conn.commit()
        elif USING_REDIS:
            pass
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

    @redis_secure_connection
    def redis_bulk_set(self, keys_values: dict, save: bool = False):
        """
        Set multiple key-value pairs in Redis.

        Args:
            keys_values (dict): A dictionary of key-value pairs to set.
            save (bool, optional): Whether to save the data to disk. Defaults to False.
        """
        self.redis_client.mset(keys_values)

        if save:
            self.redis_client.save()

        for k in keys_values:
            self.redis_client.expire(k, 60*60*24*7)

    @redis_secure_connection
    def redis_set(self, key: str, data: bytes, save: bool = False, expiration: int | None = None) -> None:
        """
        Set data for a given key in Redis.

        Args:
            key (str): The key to associate with the data.
            data (bytes): The data to store.
            save (bool, optional): Whether to save the data to disk. Defaults to False.
            expiration (int, optional): The expiration time for the key in seconds. Negative value will ensure no expiration. Defaults to either environment variable `REDIS_EXPIRATION` or 60 * 60 * 24 * 7 (1 week).
        """
        if not isinstance(expiration, int):
            expiration = REDIS_EXPIRATION
        else:
            if expiration < 0:
                expiration = None

        self.redis_client.set(key, data, ex=expiration)

        if save:
            self.redis_client.save()

    @redis_secure_connection
    def redis_get(self, key: str) -> bytes | None:
        """
        Retrieve data for a given key from Redis.

        Args:
            key (str): The key to retrieve data for.

        Returns:
            bytes or None: The retrieved data, or None if the key doesn't exist.
        """
        data = self.redis_client.get(key)
        if data is None:
            return None
        return data

    @redis_secure_connection
    def redis_delete(self, key: str) -> None:
        """
        Delete data for a given key from Redis.

        Args:
            key (str): The key to delete data for.
        """
        # self.redis_client.delete(key)
        self.redis_client.delete(key)


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
        with postgres.db.connect() as conn:
            cur = conn.cursor()
            cur.execute(f'DELETE FROM {POSTGRES_TABLE} WHERE epoch + {60 * 60 * 24 * 2} > {time.time()}')
            conn.commit()
        return

    # if USING_REDIS:
    #     redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    #     redis_client.delete(*[
    #         k for k in
    #         redis_client.scan_iter(match='*')
    #         if float(redis_client.get(k)) + 60 * 60 * 24 * 2 > time.time()
    #     ])

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


try:
    from cython.c_bucket import *
except (ImportError, ModuleNotFoundError):
    pass


if __name__ == '__main__':
    main()

