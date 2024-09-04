import asyncio
import enum
import traceback
import sqlite3
import os
import socket
import threading
import queue
import time
import string
from typing import Any

import unsync
try:
    import dotenv
    dotenv.load_dotenv('.env')
except ModuleNotFoundError:
    pass

import buelon.bucket
import buelon.core.step
import buelon.core.pipe_interpreter
import buelon.helpers.json_parser
import buelon.core.pipe_debug


class Method(enum.Enum):
    GET_STEPS = 'get-steps'
    DONE = 'done'
    PENDING = 'pending'
    CANCEL = 'cancel'
    RESET = 'reset'
    ERROR = 'error'
    UPLOAD_STEP = 'upload-step'
    STEP_COUNT = 'step-count'
    RESET_ERRORS = 'reset-errors'
    DELETE_STEPS = 'delete-steps'
    FETCH_ERRORS = 'fetch-errors'
    FETCH_ROWS = 'fetch-rows'


PIPELINE_HOST = os.environ.get('PIPELINE_HOST', '0.0.0.0')
PIPELINE_PORT = int(os.environ.get('PIPELINE_PORT', 65432))

db_path = os.path.join('database.db')

bucket_client = buelon.bucket.Client()

# Initialize a global tag_usage dictionary
tag_usage = {}
tag_lock = threading.Lock()

# Create a queue to handle incoming connections
connection_queue = queue.Queue()

PIPELINE_SPLIT_TOKEN = b'|-**-|'
PIPELINE_END_TOKEN = b'[-_-]'
LENGTH_OF_PIPELINE_END_TOKEN = len(PIPELINE_END_TOKEN)


def receive(conn: socket.socket) -> bytes:
    data = b''
    while not data.endswith(PIPELINE_END_TOKEN):
        v = conn.recv(1024)
        data += v
    return data[:-LENGTH_OF_PIPELINE_END_TOKEN]


def send(conn: socket.socket, data: bytes) -> None:
    conn.sendall(data+PIPELINE_END_TOKEN)


def load_db():
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('CREATE TABLE IF NOT EXISTS steps ('
                    'id, priority, scope, velocity, tag, status, epoch, msg, trace);')
        cur.execute('CREATE TABLE IF NOT EXISTS tags ('
                    'tag, velocity);')
        conn.commit()


def delete_steps():
    WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = (b'delete-steps'
                + buelon.hub.PIPELINE_SPLIT_TOKEN
                + b'nothing')
        send(s, data)


def _delete_steps():
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute('delete from steps;')
        conn.commit()


def reset_errors(include_workers=False):
    WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = (b'reset-errors'
                + buelon.hub.PIPELINE_SPLIT_TOKEN
                + (b'true' if include_workers else b'false'))
        send(s, data)


def _reset_errors(include_workers=b'false'):
    suffix = '' if include_workers != b'true' else f' or status = \'{buelon.core.step.StepStatus.working.value}\''
    query = f'''
    update steps 
    set status = \'{buelon.core.step.StepStatus.pending.value}\' 
    where status = \'{buelon.core.step.StepStatus.error.value}\'
    {suffix};'''
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()


def get_step_count(types: str | None = None) -> list[dict]:
    WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = (b'step-count'
                + buelon.hub.PIPELINE_SPLIT_TOKEN
                + buelon.helpers.json_parser.dumps({'types': types}))
        send(s, data)
        return buelon.helpers.json_parser.loads(receive(s))


def _get_step_count(types: str | None = None) -> list[dict]:
    if types == '*':
        where = ''
    else:
        where = f'''
        where 
            status not in (
                '{buelon.core.step.StepStatus.success.value}', 
                '{buelon.core.step.StepStatus.cancel.value}'
            )
        '''

    query = f'''
    select 
        status,
        count(*) as amount
    from 
        steps
    {where}
    group by 
        status;
    '''
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(query)

        headers = [row[0] for row in cur.description]
        table = [dict(zip(headers, row)) for row in cur.fetchall()]

        for row in table:
            row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

    return table


def upload_step(_step: buelon.core.step.Step, status: buelon.core.step.StepStatus) -> None:
    WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = (b'upload-step'
                + buelon.hub.PIPELINE_SPLIT_TOKEN
                + buelon.helpers.json_parser.dumps([_step.to_json(), status.value]))
        send(s, data)


def _upload_step(step_json: dict, status_value: int) -> None:
    status = buelon.core.step.StepStatus(status_value)
    _step = buelon.core.step.Step().from_json(step_json)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        sql = ('INSERT INTO steps (id, priority, scope, velocity, tag, status, epoch, msg, trace) '
               'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);')
        cur.execute(sql, (_step.id, _step.priority, _step.scope, _step.velocity, _step.tag, f'{status.value}',
                          time.time(), '', ''))
        conn.commit()


def upload_pipe_code(code: str):
    client = HubClient()

    variables = buelon.core.pipe_interpreter.get_steps_from_code(code)
    steps = variables['steps']
    starters = variables['starters']
    print('uploading', len(steps), 'steps')
    add_later = []
    for _step in steps.values():
        if _step.id in starters:
            add_later.append(_step)
            continue
        set_step(_step)
        # status = buelon.core.step.StepStatus.pending if _step.id in starters else buelon.core.step.StepStatus.queued
        # upload_step(_step, status)
        # upload_step(_step, buelon.core.step.StepStatus.queued)
        client.sync_upload_step(_step.to_json(), buelon.core.step.StepStatus.queued.value)

    for _step in add_later:
        set_step(_step)
        # upload_step(_step, buelon.core.step.StepStatus.pending)
        client.sync_upload_step(_step.to_json(), buelon.core.step.StepStatus.pending.value)


def upload_pipe_code_from_file(file_path: str):
    with open(file_path, 'r') as f:
        code = f.read()
        upload_pipe_code(code)


def get_step(step_id: str) -> buelon.core.step.Step | None:
    s = buelon.core.step.Step()
    b = bucket_client.get(f'step/{step_id}')
    if b is None:
        return
    data = buelon.helpers.json_parser.loads(b)
    return s.from_json(data)


def set_step(_step: buelon.core.step.Step) -> None:
    b = buelon.helpers.json_parser.dumps(_step.to_json())
    bucket_client.set(f'step/{_step.id}', b)


def get_data(step_id: str) -> Any:
    key = f'step-data/{step_id}'
    v = bucket_client.get(key)
    if v is None:
        _reset(step_id)
        raise ValueError(f'No data found for step {step_id}')
    return buelon.helpers.json_parser.loads(v)


def set_data(step_id: str, data: Any) -> None:
    key = f'step-data/{step_id}'
    b = buelon.helpers.json_parser.dumps(data)
    bucket_client.set(key, b)


def remove_data(step_id: str) -> None:
    key = f'step-data/{step_id}'
    bucket_client.delete(key)


def check_to_delete_bucket_files(
        step_id: str,
        already: set | None = None,
        steps: list | None = None
) -> None:
    _check_to_delete_bucket_files(step_id, already, steps)


def _check_to_delete_bucket_files(
        step_id: str,
        already: set | None = None,
        steps: list | None = None
) -> None:
    first_iteration = already is None and steps is None
    _step = get_step(step_id)
    already = set() if first_iteration else already
    steps = [] if first_iteration else steps
    already.add(_step.id)
    steps.append(_step)

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                check_to_delete_bucket_files(parent, already, steps)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                check_to_delete_bucket_files(child, already, steps)

    if first_iteration:
        ids = [s.id for s in steps]
        finished_statuses = {f'{v}' for v in [buelon.core.step.StepStatus.cancel.value, buelon.core.step.StepStatus.success.value]}
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            sql = f'SELECT status FROM steps WHERE id IN ({", ".join("?" * len(ids))})'
            cur.execute(sql, ids)
            rows = cur.fetchall()

            if all([f'{row[0]}' in finished_statuses for row in rows]):
                for s in steps:
                    remove_data(s.id)


def _done(step_id: str):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        _step = get_step(step_id)
        # set_data(_step.id, result.data)
        sql_update_step = (f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.success.value}\', epoch = ? WHERE id = ?')
        cur.execute(sql_update_step, (time.time(), _step.id))

        # cur.execute(sql_set_status, (_step.id, ))
        conn.commit()

        if _step.children:
            sql_update_children = (f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.pending.value}\', epoch = ? WHERE id IN ({", ".join("?" * len(_step.children))})')
            cur.execute(sql_update_children, (time.time(), *_step.children))
            conn.commit()

        # check_to_delete_bucket_files(step_id)


def _pending(step_id: str):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        sql_update_step = (f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.pending.value}\', epoch = ? WHERE id = ?')
        cur.execute(sql_update_step, (time.time(), step_id))


def _cancel(
        step_id: str,
        already: set | None = None
) -> None:
    first_iteration = already is None
    _step = get_step(step_id)
    already = set() if first_iteration else already

    sql_update_step = f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.cancel.value}\', epoch = ? WHERE id = ?'

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), _step.id))
        conn.commit()

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _cancel(parent, already)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _cancel(child, already)

    # if first_iteration:
    #     check_to_delete_bucket_files(step_id)


def _reset(step_id: str, already=None):
    _step = get_step(step_id)
    already = set() if not already else already
    # status = buelon.core.step.StepStatus.pending.value if _step.parents else buelon.core.step.StepStatus.queued.value
    status = buelon.core.step.StepStatus.queued.value if _step.parents else buelon.core.step.StepStatus.pending.value
    sql_update_step = f'UPDATE steps SET status = \'{status}\', epoch = ? WHERE id = ?'

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), _step.id))
        conn.commit()

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _reset(child, already)

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _reset(parent, already)


def _error(step_id: str, msg: str, trace: str):
    _step = get_step(step_id)
    sql_update_step = (f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.error.value}\', epoch = ?, msg = ?, trace = ? WHERE id = ?')

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), msg, trace, _step.id))
        conn.commit()


def _fetch_errors(count: int, exclude: list[str] | str | None = None) -> dict:
    if not isinstance(count, int):
        raise ValueError('count must be an integer')

    if not isinstance(exclude, (type(None), str, list)):
        raise ValueError('exclude must be a string or a list')

    if isinstance(exclude, list):
        if not all(isinstance(x, str) for x in exclude):
            raise ValueError('exclude must be a list of strings')

    allow_chars = ' abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,!@#$^&*()_+=-[]{};:"|,.<>/?~`|'
    def clean_query_string(query_string: str) -> str:
        return ''.join(c for c in query_string if c in allow_chars)

    exclude_query = ''
    if isinstance(exclude, str):
        exclude_query = f' AND not (lower(msg) like \'%{clean_query_string(exclude)}%\' OR lower(trace) like \'%{clean_query_string(exclude)}%\')'
    elif isinstance(exclude, list):
        exclude_query = (' AND not ('
                         + ' OR '.join(
                                f'lower(msg) like \'%{clean_query_string(ex)}%\' OR lower(trace) like \'%{clean_query_string(ex)}%\''
                                for ex in exclude)
                         + ')')


    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        sql = (f'SELECT id, msg, trace FROM steps WHERE status = \'{buelon.core.step.StepStatus.error.value}\''
               f' {exclude_query}'
               f' LIMIT ?')
        cur.execute(sql, (count,))

        headers = [row[0] for row in cur.description]
        table = [dict(zip(headers, row)) for row in cur.fetchall()]
        conn.commit()

        error_size_query = (f'SELECT COUNT(*)'
                            f' FROM steps'
                            f' WHERE status = \'{buelon.core.step.StepStatus.error.value}\''
                            f' {exclude_query}')
        cur.execute(error_size_query)
        error_size = cur.fetchone()[0]
        if isinstance(error_size, (tuple, list)):
            error_size = error_size[0]

        for row in table:
            row['step'] = get_step(row['id']).to_json()

        return {
            'total': error_size,
            'count': len(table),
            'table': table
        }


def get_step_status(step_id: str):
    """
    Get the hub status of a step.

    Args:
        step_id (str): The id of the step to retrieve.

    Returns:
        buelon.core.step.StepStatus: The status of the step.
    """
    global tag_usage
    if ',' in step_id:
        return [
            row
            for s in step_id.split(',')
            for row in get_step_status(s.strip())
        ]

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        sql = (f'SELECT * '
               f'FROM steps '
               f'WHERE id = \'{step_id}\'')

        cur.execute(sql)
        rows = cur.fetchall()

        table = [dict(zip([row[0] for row in cur.description], row)) for row in rows]
        for row in table:
            row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

        return table


def get_steps(scopes: list, limit=50, chunk_size=100):
    """
    Get the steps in the scope, ordered by priority, velocity, then scope position.

    Args:
        scopes (list): A list of scopes to filter the steps.
        limit (int): The maximum number of steps to retrieve. Defaults to 50.
        chunk_size (int): The number of steps to fetch in each chunk. Defaults to 100.

    Returns:
        list: A list of steps ordered by priority, velocity, and scope position.
    """
    global tag_usage

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # Fetch velocities for each tag
        velocity_sql = 'SELECT tag, velocity FROM tags'  # f'SELECT tag, velocity FROM tags WHERE scope IN ({",".join("?" * len(scopes))})'
        cur.execute(velocity_sql)  # (velocity_sql, (*scopes,))
        tag_velocities = dict(cur.fetchall())

        # Initialize tag_usage for new tags
        for tag in tag_velocities:
            if tag not in tag_usage:
                tag_usage[tag] = 0

        case_statement = ' '.join([f"WHEN ? THEN {i}" for i in range(len(scopes))])
        offset = 0
        steps = []

        expiration_time = time.time() - (60 * 60 * .2)  # (60 * 60 * 2)
        while len(steps) < limit:
            sql = (f'SELECT id, priority, scope, velocity, tag '
                   f'FROM steps '
                   f'WHERE scope IN ({",".join("?" * len(scopes))}) '
                   f'AND ('
                   f'   status = \'{buelon.core.step.StepStatus.pending.value}\' '
                   f'   or (epoch < {expiration_time} and status = \'{buelon.core.step.StepStatus.working.value}\')'
                   f')'
                   f'ORDER BY  '#' CASE scope {case_statement} END, '
                   f'   priority desc, epoch '  # , COALESCE(velocity, 1.0/0.0)
                   f'LIMIT ? OFFSET ?')

            cur.execute(sql, (*scopes, #*scopes,
                              chunk_size, offset))
            rows = cur.fetchall()
            if not rows:
                break  # Exit loop if no more rows are fetched

            for row in rows:
                step_id, priority, scope, velocity, tag = row
                if tag not in tag_velocities:
                    tag_velocities[tag] = None
                    tag_usage[tag] = 0
                if tag_velocities[tag] is None or tag_usage[tag] < tag_velocities[tag]:
                    steps.append(step_id)  # (get_step(step_id))
                    tag_usage[tag] += 1
                    if len(steps) >= limit:
                        break

            offset += chunk_size

        sql_set_to_working = f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.working.value}\', epoch = {time.time()} WHERE id IN ({", ".join("?" * len(steps))})'
        cur.execute(sql_set_to_working, steps)
        conn.commit()

    return steps


def reset_tag_usage():
    global tag_usage
    tag_usage = {}


# Function to decrement tag usage every second

def decrement_tag_usage():
    global tag_usage
    while True:
        time.sleep(1)  # Sleep for 1 second

        with tag_lock:
            for tag in list(tag_usage.keys()):  # Use list() to create a copy of keys for safe iteration
                tag_usage[tag] = max(0, tag_usage[tag] - 1)


class HubServer:
    def __init__(self, host='0.0.0.0', port=65432):
        self.host = host
        self.port = port
        self.transaction_queue = queue.Queue()
        self.execution_queue = queue.Queue()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                server.bind((self.host, self.port))
                break
            except OSError:
                print(f"Port {self.port} is already in use. Retrying...")
                time.sleep(5 * attempt)
                if attempt == attempts:
                    raise
        server.listen()
        print(f"Server listening on {self.host}:{self.port}")

        worker_thread = threading.Thread(target=self._process_transactions, daemon=True)
        worker_thread.start()

        executor_thread = threading.Thread(target=self._execute_transaction, daemon=True)
        executor_thread.start()

        while True:
            client_socket, addr = server.accept()
            client_thread = threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True)
            client_thread.start()

    def _handle_client(self, client_socket):
        data = receive(client_socket)

        if not data:
            client_socket.close()
            return

        method, payload = data.split(PIPELINE_SPLIT_TOKEN)
        print('received', method)
        method = Method(method.decode())
        self.transaction_queue.put((method, payload, client_socket))
        # with client_socket:
        #     try:
        #         while True:
        #             data = receive(client_socket)
        #
        #             if not data:
        #                 break
        #
        #             method, payload = data.split(PIPELINE_SPLIT_TOKEN)
        #             print('received', method)
        #             method = Method(method.decode())
        #             self.transaction_queue.put((method, payload, client_socket))
        #     except KeyboardInterrupt:
        #         raise
        #     except Exception as e:
        #         print(f"Error handling client: {e}")
        #         traceback.print_exc()

    def _process_transactions(self):
        while True:
            method, payload, client_socket = self.transaction_queue.get()
            try:
                with client_socket:
                    print('processing', method)
                    response = self._process_request(method, payload)

                    send(client_socket, response)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                traceback.print_exc()
                print(f"Error processing transaction: {e}")
            finally:
                self.transaction_queue.task_done()

    def _execute_transaction(self):
        while True:
            method, payload = self.execution_queue.get()
            try:
                print('executing', method)
                self._execute_request(method, payload)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                traceback.print_exc()
                print(f"Error processing transaction: {e}")
            finally:
                self.execution_queue.task_done()

    def _execute_request(self, method, payload):
        if method == Method.DONE:
            _done(payload.decode())
            return
        elif method == Method.PENDING:
            _pending(payload.decode())
            return
        elif method == Method.CANCEL:
            _cancel(payload.decode())
            return
        elif method == Method.RESET:
            _reset(payload.decode())
            return
        elif method == Method.ERROR:
            values = buelon.helpers.json_parser.loads(payload)
            _error(values['step_id'], values['msg'], values['trace'])
            return
        elif method == Method.UPLOAD_STEP:
            step_json, status_value = buelon.helpers.json_parser.loads(payload)
            _upload_step(step_json, status_value)
            return
        elif method == Method.RESET_ERRORS:
            _reset_errors(payload)
            return
        elif method == Method.DELETE_STEPS:
            _delete_steps()
            return
        raise ValueError(f'Invalid method: {method}')

    def _process_request(self, method: Method, payload: bytes):
        if (method == Method.DONE or method == Method.PENDING or method == Method.CANCEL or method == Method.RESET
                or method == Method.ERROR or method == Method.UPLOAD_STEP or method == Method.RESET_ERRORS
                or method == Method.DELETE_STEPS):
            self.execution_queue.put((method, payload))
        if method == Method.GET_STEPS:
            scopes = buelon.helpers.json_parser.loads(payload)
            steps = get_steps(scopes)
            return buelon.helpers.json_parser.dumps(steps)
        if method == Method.FETCH_ROWS:
            step_id = buelon.helpers.json_parser.loads(payload)['step_id']
            rows = get_step_status(step_id)
            return buelon.helpers.json_parser.dumps(rows)
        # elif method == Method.DONE:
        #     _done(payload.decode())
        # elif method == Method.PENDING:
        #     _pending(payload.decode())
        # elif method == Method.CANCEL:
        #     _cancel(payload.decode())
        # elif method == Method.RESET:
        #     _reset(payload.decode())
        # elif method == Method.ERROR:
        #     values = buelon.helpers.json_parser.loads(payload)
        #     _error(values['step_id'], values['msg'], values['trace'])
        # elif method == Method.UPLOAD_STEP:
        #     step_json, status_value = buelon.helpers.json_parser.loads(payload)
        #     _upload_step(step_json, status_value)
        elif method == Method.STEP_COUNT:
            kwargs = buelon.helpers.json_parser.loads(payload)
            result = _get_step_count(kwargs['types'])
            return buelon.helpers.json_parser.dumps(result)
        # elif method == Method.RESET_ERRORS:
        #     _reset_errors(payload)
        # elif method == Method.DELETE_STEPS:
        #     _delete_steps()
        elif method == Method.FETCH_ERRORS:
            try:
                config = buelon.helpers.json_parser.loads(payload)
                count = int(config.get('count', 5))
                exclude = config.get('exclude', None)

                if not isinstance(exclude, (type(None), str, list)):
                    raise ValueError('exclude must be a string or a list')

                if isinstance(exclude, list):
                    if not all(isinstance(x, str) for x in exclude):
                        raise ValueError('exclude must be a list of strings')
            except ValueError:
                count = 5
                exclude = None
            return buelon.helpers.json_parser.dumps(_fetch_errors(count, exclude))
        return b'ok'


class HubClient:
    def __init__(self, host=None, port=None, max_reconnect_attempts=2):
        self.host = host or os.environ.get('PIPE_WORKER_HOST', 'localhost')
        self.port = port or int(os.environ.get('PIPE_WORKER_PORT', 65432))
        self.conn = None
        self.max_reconnect_attempts = max_reconnect_attempts

    def __enter__(self):
        # self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        # self.close()

    def connect(self):
        if not self.conn:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((self.host, self.port))
            self.conn.settimeout(60*60)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def send_request(self, method, data: bytes):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
            conn.connect((self.host, self.port))
            conn.settimeout(60*60)
            request = method.value.encode() + PIPELINE_SPLIT_TOKEN + data
            send(conn, request)
            response = receive(conn)
        return response
        # attempt = 0
        # while attempt < self.max_reconnect_attempts:
        #     try:
        #         if not self.conn:
        #             self.connect()
        #         request = method.value.encode() + PIPELINE_SPLIT_TOKEN + data
        #         send(self.conn, request)
        #         response = receive(self.conn)
        #         return response
        #     except (OSError, ConnectionError) as e:
        #         print(f"Connection error: {e}")
        #         attempt += 1
        #         self.close()
        #         if attempt >= self.max_reconnect_attempts:
        #             raise ConnectionError(f"Failed to send request after {self.max_reconnect_attempts} attempts: {e}")
        #         time.sleep(1)  # Backoff before retrying
        #         self.connect()  # Establish a new connection before retrying

    def get_steps(self, scopes):
        return buelon.helpers.json_parser.loads(self.send_request(Method.GET_STEPS, buelon.helpers.json_parser.dumps(scopes)))

    def get_rows(self, step_id):
        return buelon.helpers.json_parser.loads(self.send_request(Method.FETCH_ROWS, buelon.helpers.json_parser.dumps({'step_id': step_id})))

    def done(self, step_id):
        return self.send_request(Method.DONE, step_id.encode())

    def pending(self, step_id):
        return self.send_request(Method.PENDING, step_id.encode())

    def cancel(self, step_id):
        return self.send_request(Method.CANCEL, step_id.encode())

    def reset(self, step_id):
        return self.send_request(Method.RESET, step_id.encode())

    def error(self, step_id, msg, trace):
        return self.send_request(Method.ERROR, buelon.helpers.json_parser.dumps({'step_id': step_id, 'msg': msg, 'trace': trace}))

    def upload_step(self, step_json, status_value):
        return self.send_request(Method.UPLOAD_STEP, buelon.helpers.json_parser.dumps([step_json, status_value]))

    def get_step_count(self, types: str | None = None):
        return buelon.helpers.json_parser.loads(
            self.send_request(
                Method.STEP_COUNT,
                buelon.helpers.json_parser.dumps({'types': types})
            )
        )

    def reset_errors(self, include_workers):
        return self.send_request(Method.RESET_ERRORS, b'true' if include_workers else b'false')

    def delete_steps(self):
        return self.send_request(Method.DELETE_STEPS, b'nothing')

    def fetch_errors(self, count: int, exclude: list[str] | str | None = None) -> dict:
        """

        Args:
            count (int): Number of errors to fetch
            exclude (lit[str], str, optional): Exclude errors with this string in the message. Defaults to None.

        Returns:
            dict: A dictionary containing `table`, `count`, and `total`.
                `table` is a list of error dictionaries, each containing `id`, `msg`, and `trace`.
        """
        return buelon.helpers.json_parser.loads(
            self.send_request(
                Method.FETCH_ERRORS,
                buelon.helpers.json_parser.dumps({
                    'count': count,
                    'exclude': exclude
                })#f'{count}'.encode()
            )
        )

    def __getattr__(self, item: str):
        if item.startswith('sync_'):
            if hasattr(self, item[5:]):
                return getattr(self, item[5:])
        raise AttributeError('')


def main():
    """
    Main function to start the server and worker thread.

    Handles keyboard interruption to shut down the server gracefully.
    """
    load_db()

    server = HubServer()
    server.start()


try:
    from buelon.cython.c_hub import *
except (ImportError, ModuleNotFoundError):
    pass


if __name__ == "__main__":
    main()
