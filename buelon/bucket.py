"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.
"""

import socket
import threading
import os
import sys

import psutil

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass


# Environment variables for client and server configuration
BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = '.bucket'

database: dict[str, bytes] = {}
database_keys_in_order = []
MAX_DATABASE_SIZE: int = min(1024 * 1024 * 1024 * 1, int(psutil.virtual_memory().total / 8))


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
    def wrapper(*args, **kwargs):
        tries = 4
        kwargs['timeout'] = kwargs.get('timeout', 60 * 5.)
        for i in range(tries):
            try:
                return func(*args, **kwargs)
            except (TimeoutError, ConnectionResetError):
                kwargs['timeout'] *= 2
        raise
    return wrapper


class Client:
    """A client for interacting with the bucket server."""
    PORT: int
    HOST: str

    def __init__(self):
        """Initialize the client with host and port from environment variables."""
        self.PORT = BUCKET_CLIENT_PORT
        self.HOST = BUCKET_CLIENT_HOST

    @retry_connection
    def set(self, key: str, data: bytes, timeout: float | None = 60 * 5.) -> None:
        """
        Set data for a given key on the server.

        Args:
            key (str): The key to associate with the data.
            data (bytes): The data to store.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.
        """
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

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'set')
            # receive(s)
            #
            # send(s, data)
            # receive(s)

    @retry_connection
    def get(self, key: str, timeout: float | None = 60 * 5.) -> bytes | None:
        """
        Retrieve data for a given key from the server.

        Args:
            key (str): The key to retrieve data for.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.

        Returns:
            bytes or None: The retrieved data, or None if the key doesn't exist.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(timeout)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'get', f'{timeout}'.encode(), b'__null__']))

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'get')

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
    def delete(self, key: str, timeout: float | None = 60 * 5.) -> None:
        """
        Delete data for a given key on the server.

        Args:
            key (str): The key to delete data for.
            timeout (float, optional): The timeout for the socket connection in seconds. Defaults to 60 * 5.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(timeout)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'delete', f'{timeout}'.encode(), b'__null__']))
            receive(s)

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'delete')
            # receive(s)


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

