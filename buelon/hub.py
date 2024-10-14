from __future__ import annotations
import asyncio
import enum
import traceback
import sqlite3
import os
import sys
import socket
import threading
import queue
import time
import uuid
import string
import platform
import signal
import collections
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import persistqueue
import unsync
import tqdm
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
import buelon.helpers.lazy_load_class


class Method(enum.Enum):
    GET_STEPS = 'get-steps'
    DONE = 'done'
    PENDING = 'pending'
    CANCEL = 'cancel'
    RESET = 'reset'
    ERROR = 'error'
    UPLOAD_STEP = 'upload-step'
    UPLOAD_STEPS = 'upload-steps'
    STEP_COUNT = 'step-count'
    RESET_ERRORS = 'reset-errors'
    DELETE_STEPS = 'delete-steps'
    FETCH_ERRORS = 'fetch-errors'
    FETCH_ROWS = 'fetch-rows'


USING_POSTGRES = os.environ.get('USING_POSTGRES_HUB', 'false') == 'true'
PIPELINE_HOST = os.environ.get('PIPELINE_HOST', '0.0.0.0')
PIPELINE_PORT = int(os.environ.get('PIPELINE_PORT', 65432))

db_path = os.path.join('database.db')

bucket_client = buelon.bucket.Client()

# Initialize a global tag_usage dictionary
tag_usage = collections.defaultdict(int) #{}
tag_lock = threading.Lock()

# Create a queue to handle incoming connections
connection_queue = queue.Queue()

PIPELINE_SPLIT_TOKEN = b'|-**-|'
PIPELINE_END_TOKEN = b'[-_-]'
LENGTH_OF_PIPELINE_END_TOKEN = len(PIPELINE_END_TOKEN)



def delete_last_line():
    sys.stdout.write("\033[F")
    sys.stdout.write("\033[K")


def timeout_func(func, args=None, kwargs=None, timeout_duration=1, default=None):
    if args is None: args = []
    if kwargs is None: kwargs = {}

    # only Darwin, Linux
    if platform.system() == 'Windows':
        # does not work in windows
        return func(*args, **kwargs)

    class CustomTimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise CustomTimeoutError()

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)
    try:
        result = func(*args, **kwargs)
    except CustomTimeoutError as exc:
        result = default
    finally:
        signal.alarm(0)

    return result


def receive(conn: socket.socket) -> bytes:
    data = b''
    while not data.endswith(PIPELINE_END_TOKEN):
        v = conn.recv(1024)
        data += v
        if not v and not data.endswith(PIPELINE_END_TOKEN):
            try:
                data = data.decode()
            except: pass
            raise ValueError(f'Invalid value received: `{data}`')
    return data[:-LENGTH_OF_PIPELINE_END_TOKEN]


def send(conn: socket.socket, data: bytes) -> None:
    conn.sendall(data+PIPELINE_END_TOKEN)


def load_db():
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('CREATE TABLE IF NOT EXISTS steps ('
                    'id, priority, scope, timeout, retries, velocity, tag, status, epoch, msg, trace);')
        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_id ON steps (id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_status ON steps (status);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_priority ON steps (priority);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_scope ON steps (scope);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_epoch ON steps (epoch);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_tag ON steps (tag);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_steps_velocity ON steps (velocity);')

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


def _delete_steps(self: HubServer):
    self.execute_db_query('delete from steps;')
    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute('delete from steps;')
    #     conn.commit()


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


def _reset_errors(self: HubServer, include_workers=b'false'):
    suffix = '' if include_workers != b'true' else f' or status = \'{buelon.core.step.StepStatus.working.value}\''
    query = f'''
    update steps 
    set status = \'{buelon.core.step.StepStatus.pending.value}\' 
    where status = \'{buelon.core.step.StepStatus.error.value}\'
    {suffix};'''
    self.execute_db_query(query)
    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(query)
    #     conn.commit()


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


def _get_step_count(self: HubServer, types: str | None = None) -> list[dict]:
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
    table = self.execute_db_query(query, json_table=True)
    for row in table:
        row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(query)
    #
    #     headers = [row[0] for row in cur.description]
    #     table = [dict(zip(headers, row)) for row in cur.fetchall()]
    #
    #     for row in table:
    #         row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

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


def _upload_step(self: HubServer, step_json: dict, status_value: int) -> None:
    status = buelon.core.step.StepStatus(status_value)
    _step: buelon.core.step.Step = buelon.core.step.Step().from_json(step_json)
    sql = ('INSERT INTO steps (id, priority, scope, timeout, retries, velocity, tag, status, epoch, msg, trace) '
           'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')
    params = (_step.id, _step.priority, _step.scope, _step.timeout, _step.retries, _step.velocity, _step.tag, f'{status.value}', time.time(), '', '')

    self.execute_db_query(sql, params)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(sql, params)
    #     conn.commit()


def _upload_steps(self: HubServer, step_jsons: list, status_values: list) -> None:
    sql = ('INSERT INTO steps (id, priority, scope, timeout, retries, velocity, tag, status, epoch, msg, trace) '
           'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')
    for step_json, status_value in zip(step_jsons, status_values):
        status = buelon.core.step.StepStatus(status_value)
        _step: buelon.core.step.Step = buelon.core.step.Step().from_json(step_json)
        params = (
            _step.id, _step.priority, _step.scope, _step.timeout, _step.retries, _step.velocity, _step.tag, f'{status.value}', time.time(), '', '')
        self.execute_db_query(sql, params)
    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     for step_json, status_value in zip(step_jsons, status_values):
    #         status = buelon.core.step.StepStatus(status_value)
    #         _step = buelon.core.step.Step().from_json(step_json)
    #         params = (
    #         _step.id, _step.priority, _step.scope, _step.velocity, _step.tag, f'{status.value}', time.time(), '', '')
    #         cur.execute(sql, params)
    #     conn.commit()


def upload_pipe_code(
        code: str,
        lazy_steps: bool = False,
        wait_until_finish: bool = False,
        return_job_ids: bool = False,
        return_job_def_id: str | list[str] | None = None,
        job_handler: callable | None = None
) -> str | list[str] | tuple[str | list[str] | None, list[str]] | None:
    client = HubClient()
    chunk_size = 2000  # 5000 if not lazy_steps else 500
    chunk = []
    statuses = []
    pending = buelon.core.step.StepStatus.pending.value
    queued = buelon.core.step.StepStatus.queued.value

    job_ids = []

    if wait_until_finish:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('drop table if exists bue_session_ids;')
        cur.execute('create table bue_session_ids (id text primary key, attempts int);')
        conn.commit()

    for job in tqdm.tqdm(buelon.core.pipe_interpreter.generate_steps_from_code(code), desc='Uploading Jobs'):
        if job_handler:
            try:
                job_handler(job)
            except: pass

        if return_job_def_id:
            if isinstance(return_job_def_id, str):
                if job.name == return_job_def_id:
                    return_job_def_id = job.id
            elif isinstance(return_job_def_id, list):
                if job.name in return_job_def_id:
                    return_job_def_id[return_job_def_id.index(job.name)] = job.id

        if return_job_ids:
            job_ids.append(job.id)
        # print('job', job)
        # continue
        chunk.append(job)
        statuses.append(pending if not job.parents else queued)
        if wait_until_finish:
            cur.execute(f'insert into bue_session_ids (id, attempts) values (\'{job.id}\', 0);')

        if len(chunk) >= chunk_size:
            set_steps(chunk)
            client.upload_steps([s.to_json() for s in chunk], statuses)
            chunk.clear()
            statuses.clear()

    if chunk:
        set_steps(chunk)
        client.upload_steps([s.to_json() for s in chunk], statuses)
        chunk.clear()
        statuses.clear()

    if wait_until_finish:
        conn.commit()
        time.sleep(10.)
        i = 1
        while (rows := cur.execute('select id, attempts from bue_session_ids limit 10;').fetchall()):
            key_map = {row[0]: row[1] for row in rows}
            print('waiting' + ('.' * (i % 4)))
            i += 1
            for row in client.get_rows(','.join(row[0] for row in rows)):
                # print(row)
                if (row['status'] == buelon.core.step.StepStatus.success.name
                        or row['status'] == buelon.core.step.StepStatus.cancel.name):
                    cur.execute(f'delete from bue_session_ids where id = \'{row["id"]}\';')
                    conn.commit()
                elif row['status'] == buelon.core.step.StepStatus.error.name:
                    print('Error:', row['msg'])
                    # print('Trace:', row['trace'])
                    print('')  # last line deleted
                    if key_map[row['id']] > 3:
                        raise Exception('Max errors met')
                    client.reset_errors(False)
                    cur.execute(f'update bue_session_ids set attempts = attempts + 1 where id = \'{row["id"]}\';')
                    conn.commit()
            time.sleep(5.)
            delete_last_line()

        cur.execute('drop table bue_session_ids;')
        conn.commit()
        conn.close()
        print('Done')

    if return_job_ids:
        if return_job_def_id:
            return return_job_def_id, job_ids
        return job_ids
    elif return_job_def_id:
        return return_job_def_id

    return
    variables = buelon.core.pipe_interpreter.get_steps_from_code(code, lazy_steps)
    steps = variables['steps']
    starters = variables['starters']
    add_later = {} if not lazy_steps else buelon.helpers.lazy_load_class.LazyMap()
    
    try:
        print('uploading', len(steps), 'steps')
        chunk = []
        for _step in steps.values():
            if _step.id in starters:
                add_later[_step.id] = _step
                continue

            chunk.append(_step)

            if len(chunk) >= chunk_size:
                set_steps(chunk)
                client.upload_steps([s.to_json() for s in chunk], len(chunk) * [buelon.core.step.StepStatus.queued.value])
                chunk.clear()

        if chunk:
            set_steps(chunk)
            client.upload_steps([s.to_json() for s in chunk], len(chunk) * [buelon.core.step.StepStatus.queued.value])
            chunk.clear()

        for _step in add_later.values():

            chunk.append(_step)

            if len(chunk) >= chunk_size:
                set_steps(chunk)
                client.upload_steps([s.to_json() for s in chunk], len(chunk) * [buelon.core.step.StepStatus.pending.value])
                chunk.clear()

        if chunk:
            set_steps(chunk)
            client.upload_steps([s.to_json() for s in chunk], len(chunk) * [buelon.core.step.StepStatus.pending.value])
            chunk.clear()
    finally:
        del steps
        del add_later


def upload_pipe_code_from_file(file_path: str, lazy_steps: bool = False, wait_until_finish: bool = False):
    with open(file_path, 'r') as f:
        code = f.read()
        upload_pipe_code(code, lazy_steps, wait_until_finish)


def submit_pipe_code_from_file(file_path: str, scope: str | None = None):
    with open(file_path) as f:
        return submit_pipe_code(f.read(), scope)


def submit_pipe_code(code: str, scope: str | None = None, priority: int = 0):
    if scope is None:
        scope = buelon.worker.DEFAULT_SCOPES.split(',')[-1]
    if not isinstance(priority, int):
        priority = 0

    bucket = buelon.bucket.Client()

    key = f'submit-jobs/{uuid.uuid1()}'
    bucket.set(key, code.encode())
    # Cython does not allow an f-string here
    code = "TAB = '  '\n!scope {scope}\n!priority {priority}\n\njob:\n  python\n  main\n  `\nimport buelon as pete\ndef main(*args):\n    bucket = pete.bucket.Client()\n    code = bucket.get('{key}').decode()\n    pete.hub.upload_pipe_code(code)\n`\n\npipe = | job\npipe()\n"
    code = code.format(scope=scope, key=key, priority=f'{priority}')
    upload_pipe_code(code)


def get_step(step_id: str) -> buelon.core.step.Step | None:
    s = buelon.core.step.Step()
    b = bucket_client.get(f'step/{step_id}')
    if b is None:
        return
    data = buelon.helpers.json_parser.loads(b)
    return s.from_json(data)


def bulk_get_step(step_ids: list[str]) -> dict[str, buelon.core.step.Step | None]:
    output = bucket_client.bulk_get([f'step/{step_id}' for step_id in step_ids])
    new_output = {}
    for k in output:
        new_key = k.split('/')[1]
        if output[k] is None:
            new_output[new_key] = None
            continue
        s = buelon.core.step.Step()
        new_output[new_key] = s.from_json(buelon.helpers.json_parser.loads(output[k]))

    return new_output


def set_step(_step: buelon.core.step.Step) -> None:
    b = buelon.helpers.json_parser.dumps(_step.to_json())
    bucket_client.set(f'step/{_step.id}', b)


def set_steps(steps: list[buelon.core.step.Step]) -> None:
    # if not buelon.bucket.USING_POSTGRES and not buelon.bucket.USING_REDIS:
    #     # raise ValueError('set_steps only works with postgres and redis')
    #     for _step in steps:
    #         set_step(_step)
    #     return

    bulk = {}
    for _step in steps:
        # set_step(_step)
        bulk[f'step/{_step.id}'] = buelon.helpers.json_parser.dumps(_step.to_json())

    bucket_client.bulk_set(bulk, True)


def remove_step(step_id: str) -> None:
    bucket_client.delete(f'step/{step_id}')


def get_data(step_id: str) -> Any:
    key = f'step-data/{step_id}'
    v = bucket_client.get(key)
    if v is None:
        # _reset(step_id)
        raise ValueError(f'No data found for step {step_id}')
    return buelon.helpers.json_parser.loads(v)


def bulk_get_data(step_ids: list[str]) -> Any:
    output = bucket_client.bulk_get([f'step-data/{step_id}' for step_id in step_ids])
    new_output = {}
    for k, v in output.items():
        step_id = k.split('/')[1]
        if v is None:
            raise ValueError(f'No data found for step {step_id}')
        new_output[step_id] = buelon.helpers.json_parser.loads(v)
    return output


def set_data(step_id: str, data: Any) -> None:
    key = f'step-data/{step_id}'
    b = buelon.helpers.json_parser.dumps(data)
    bucket_client.set(key, b)


def bulk_set_data(bulk: dict[str, Any]) -> None:
    new_bulk = {}
    for step_id in bulk:
        new_bulk[f'step-data/{step_id}'] = buelon.helpers.json_parser.dumps(bulk[step_id])
    bucket_client.bulk_set(new_bulk)


def remove_data(step_id: str) -> None:
    key = f'step-data/{step_id}'
    bucket_client.delete(key)


def check_to_delete_bucket_files(
        self: HubServer,
        step_id: str,
        already: set | None = None,
        steps: list | None = None
) -> None:
    _check_to_delete_bucket_files(self, step_id, already, steps)


def _check_to_delete_bucket_files(
    self: HubServer,
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
                # check_to_delete_bucket_files(parent, already, steps)
                check_to_delete_bucket_files(self, parent, already, steps)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                # check_to_delete_bucket_files(child, already, steps)
                check_to_delete_bucket_files(self, child, already, steps)

    if first_iteration:
        ids = [s.id for s in steps]
        finished_statuses = {f'{v}' for v in [buelon.core.step.StepStatus.cancel.value, buelon.core.step.StepStatus.success.value]}
        sql = f'SELECT status FROM steps WHERE id IN ({", ".join("?" * len(ids))})'

        # new
        rows = self.execute_db_query(sql, ids)

        # with sqlite3.connect(db_path) as conn:
        #     cur = conn.cursor()
        #     cur.execute(sql, ids)
        #     rows = cur.fetchall()

        if all([f'{row[0]}' in finished_statuses for row in rows]):
            for s in steps:
                remove_data(s.id)


def _done(self: HubServer, step_id: str):
    _step = get_step(step_id)
    sql_update_step = (
        f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.success.value}\', epoch = ? WHERE id = ?')
    params = (time.time(), _step.id)

    self.execute_db_query(sql_update_step, params)

    if _step.children:
        sql_update_children = (
            f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.pending.value}\', epoch = ? WHERE id IN ({", ".join("?" * len(_step.children))})')
        children_params = (time.time(), *_step.children)

        self.execute_db_query(sql_update_children, children_params)

    # check_to_delete_bucket_files(self, step_id)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     # set_data(_step.id, result.data)
    #
    #     cur.execute(sql_update_step, params)
    #
    #     # cur.execute(sql_set_status, (_step.id, ))
    #     conn.commit()
    #
    #     if _step.children:
    #
    #         cur.execute(sql_update_children, children_params)
    #         conn.commit()
    #
    #     # check_to_delete_bucket_files(step_id)


def _pending(self: HubServer, step_id: str):
    sql_update_step = (
        f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.pending.value}\', epoch = ? WHERE id = ?')
    params = (time.time(), step_id)
    self.execute_db_query(sql_update_step, params)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(sql_update_step, params)


def _cancel(
    self: HubServer,
    step_id: str,
    already: set | None = None
) -> None:
    first_iteration = already is None
    _step = get_step(step_id)
    already = set() if first_iteration else already

    sql_update_step = f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.cancel.value}\', epoch = ? WHERE id = ?'
    params = (time.time(), _step.id)
    self.execute_db_query(sql_update_step, params)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(sql_update_step, params)
    #     conn.commit()

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _cancel(self, parent, already)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _cancel(self, child, already)

    # if first_iteration:
    #     check_to_delete_bucket_files(self, step_id)


def _reset(self: HubServer, step_id: str, already=None):
    _step = get_step(step_id)
    already = set() if not already else already
    # status = buelon.core.step.StepStatus.pending.value if _step.parents else buelon.core.step.StepStatus.queued.value
    status = buelon.core.step.StepStatus.queued.value if _step.parents else buelon.core.step.StepStatus.pending.value
    sql_update_step = f'UPDATE steps SET status = \'{status}\', epoch = ? WHERE id = ?'
    params = (time.time(), _step.id)

    self.execute_db_query(sql_update_step, params)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(sql_update_step, params)
    #     conn.commit()

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _reset(self, child, already)

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _reset(self, parent, already)


def _error(self: HubServer, step_id: str, msg: str, trace: str):
    _step = get_step(step_id)
    sql_update_step = (f'UPDATE steps SET status = \'{buelon.core.step.StepStatus.error.value}\', epoch = ?, msg = ?, trace = ? WHERE id = ?')
    params = (time.time(), msg, trace, _step.id)

    self.execute_db_query(sql_update_step, params)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #     cur.execute(sql_update_step, params)
    #     conn.commit()


def _fetch_errors(self: HubServer, count: int, exclude: list[str] | str | None = None) -> dict:
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

    sql = (f'SELECT id, msg, trace FROM steps WHERE status = \'{buelon.core.step.StepStatus.error.value}\''
           f' {exclude_query}'
           f' LIMIT ?')
    params = (count, )
    error_size_query = (f'SELECT COUNT(*)'
                        f' FROM steps'
                        f' WHERE status = \'{buelon.core.step.StepStatus.error.value}\''
                        f' {exclude_query}')

    table = self.execute_db_query(sql, params, json_table=True)
    error_size = self.execute_db_query(error_size_query)[0][0]

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #
    #     cur.execute(sql, params)
    #
    #     headers = [row[0] for row in cur.description]
    #     table = [dict(zip(headers, row)) for row in cur.fetchall()]
    #     conn.commit()
    #
    #
    #     cur.execute(error_size_query)
    #     error_size = cur.fetchone()[0]
    #     if isinstance(error_size, (tuple, list)):
    #         error_size = error_size[0]

    for row in table:
        row['step'] = get_step(row['id']).to_json()

    return {
        'total': error_size,
        'count': len(table),
        'table': table
    }


def get_step_status(self: HubServer, step_id: str):
    """
    Get the hub status of a step.

    Args:
        self: The HubServer instance.
        step_id (str): The id of the step to retrieve.

    Returns:
        buelon.core.step.StepStatus: The status of the step.
    """
    global tag_usage
    if ',' in step_id:
        return [
            row
            for s in step_id.split(',')
            for row in get_step_status(self, s.strip())
        ]

    sql = (f'SELECT * '
           f'FROM steps '
           f'WHERE id = \'{step_id}\'')

    table = self.execute_db_query(sql, json_table=True)

    # with sqlite3.connect(db_path) as conn:
    #     cur = conn.cursor()
    #
    #     cur.execute(sql)
    #     rows = cur.fetchall()
    #
    #     table = [dict(zip([row[0] for row in cur.description], row)) for row in rows]

    for row in table:
        row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

    return table


def get_steps(
    scopes: list,
    limit=50,
    chunk_size=100,
    status: int = buelon.core.step.StepStatus.pending.value,
    include_working: bool = True,
    reverse: bool=False,
):
    """
    Get the steps in the scope, ordered by priority, velocity, then scope position.

    Args:
        scopes (list): A list of scopes to filter the steps.
        limit (int): The maximum number of steps to retrieve. Defaults to 50.
        chunk_size (int): The number of steps to fetch in each chunk. Defaults to 100.
        status (int | buelon.core.step.StepStatus.value): The status of the steps to retrieve. Defaults to pending.
        include_working (bool): Whether to include working steps. Defaults to True.
        reverse (bool): Whether to reverse the order of the steps. Defaults to False.

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
        status = buelon.core.step.StepStatus(status).value
        working_clause = ''
        if include_working:
            working_clause = (f' or (epoch < {expiration_time} '
                              f'     and status = \'{buelon.core.step.StepStatus.working.value}\') ')

        order_by = 'priority desc, epoch'
        if reverse:
            order_by = 'priority asc, epoch'

        while len(steps) < limit:
            sql = (f'SELECT id, priority, scope, velocity, tag '
                   f'FROM steps '
                   f'WHERE scope IN ({",".join("?" * len(scopes))}) '
                   f'AND ('
                   f'   status = \'{status}\' '
                   f'   {working_clause}'
                   f')'
                   f'ORDER BY  '#' CASE scope {case_statement} END, '
                   f'   {order_by} '  # , COALESCE(velocity, 1.0/0.0)
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
    tag_usage = collections.defaultdict(int)  # {}


# Function to decrement tag usage every second

def decrement_tag_usage():
    global tag_usage
    while True:
        time.sleep(1)  # Sleep for 1 second

        with tag_lock:
            for tag in list(tag_usage.keys()):  # Use list() to create a copy of keys for safe iteration
                tag_usage[tag] = max(0, tag_usage[tag] - 1)


class ThreadSafeSQLiteQueue:
    def __init__(self, path, auto_commit=True):
        self.path = path
        self.auto_commit = auto_commit
        self.local = threading.local()

    def _get_queue(self):
        if not hasattr(self.local, 'queue'):
            self.local.queue = persistqueue.SQLiteQueue(self.path, auto_commit=self.auto_commit)
        return self.local.queue

    def put(self, item):
        return self._get_queue().put(item)

    def get(self):
        return self._get_queue().get()

    def task_done(self):
        return self._get_queue().task_done()

    def qsize(self):
        return self._get_queue().qsize()


class HubServer:
    def __init__(self, host='0.0.0.0', port=65432):
        self.host = host
        self.port = port
        self.transaction_queue = queue.Queue()
        # self.execution_queue = queue.Queue()
        self.execution_queue = ThreadSafeSQLiteQueue('execution_queue', auto_commit=True)  # persistqueue.SQLiteQueue('execution_queue', auto_commit=True)
        self.db_pool = self.create_db_pool()

    def create_db_pool(self, max_connections=10):
        return ThreadPoolExecutor(max_workers=max_connections)

    def execute_db_query(self, query, params=None, json_table=False):
        def db_operation():
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                conn.commit()
                if json_table:
                    headers = [row[0] for row in cur.description]
                    table = [dict(zip(headers, row)) for row in cur.fetchall()]
                    return table
                return cur.fetchall()

        return self.db_pool.submit(db_operation).result()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
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
        finally:
            server.shutdown(socket.SHUT_RDWR)
            server.close()
            exit()

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

    # def _execute_transaction(self):
    #     while True:
    #         method, payload = self.execution_queue.get()
    #         try:
    #             print('executing', method)
    #             self._execute_request(method, payload)
    #         except KeyboardInterrupt:
    #             raise
    #         except Exception as e:
    #             traceback.print_exc()
    #             print(f"Error processing transaction: {e}")
    #         finally:
    #             self.execution_queue.task_done()

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
            # _done(payload.decode())
            _done(self, payload.decode())
            return
        elif method == Method.PENDING:
            _pending(self, payload.decode())
            return
        elif method == Method.CANCEL:
            _cancel(self, payload.decode())
            return
        elif method == Method.RESET:
            _reset(self, payload.decode())
            return
        elif method == Method.ERROR:
            values = buelon.helpers.json_parser.loads(payload)
            _error(self, values['step_id'], values['msg'], values['trace'])
            return
        elif method == Method.UPLOAD_STEP:
            step_json, status_value = buelon.helpers.json_parser.loads(payload)
            # _upload_step(step_json, status_value)
            _upload_step(self, step_json, status_value)
            return
        elif method == Method.UPLOAD_STEPS:
            step_jsons, status_values = buelon.helpers.json_parser.loads(payload)
            # _upload_steps(step_jsons, status_values)
            _upload_steps(self, step_jsons, status_values)
            # for step_json, status_value in zip(step_jsons, status_values):
            #     _upload_step(step_json, status_value)
            return
        elif method == Method.RESET_ERRORS:
            # _reset_errors(payload)
            _reset_errors(self, payload)
            return
        # elif method == Method.DELETE_STEPS:
        #     _delete_steps()
        #     return
        raise ValueError(f'Invalid method: {method}')

    def _process_request(self, method: Method, payload: bytes):
        if (method == Method.DONE or method == Method.PENDING or method == Method.CANCEL or method == Method.RESET
                or method == Method.ERROR or method == Method.UPLOAD_STEP or method == Method.UPLOAD_STEPS
                or method == Method.RESET_ERRORS):
            self.execution_queue.put((method, payload))
        if method == Method.GET_STEPS:
            scopes, kwargs = buelon.helpers.json_parser.loads(payload)
            # steps = get_steps(scopes, **kwargs)
            steps = self.get_steps(scopes, **kwargs)
            return buelon.helpers.json_parser.dumps(steps)
        elif method == Method.FETCH_ROWS:
            step_id = buelon.helpers.json_parser.loads(payload)['step_id']
            rows = get_step_status(self, step_id)
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
            # result = _get_step_count(kwargs['types'])
            result = _get_step_count(self, kwargs['types'])
            return buelon.helpers.json_parser.dumps(result)
        # elif method == Method.RESET_ERRORS:
        #     _reset_errors(payload)
        elif method == Method.DELETE_STEPS:
            # _delete_steps()
            _delete_steps(self)
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
            return buelon.helpers.json_parser.dumps(_fetch_errors(self, count, exclude))
        return b'ok'

    def get_steps(self, scopes, limit=100, chunk_size=100, status=buelon.core.step.StepStatus.pending.value,
                  include_working=True, reverse=False):
        """
            Get the steps in the scope, ordered by priority, velocity, then scope position.

            Args:
                scopes (list): A list of scopes to filter the steps.
                limit (int): The maximum number of steps to retrieve. Defaults to 50.
                chunk_size (int): The number of steps to fetch in each chunk. Defaults to 100.
                status (int | buelon.core.step.StepStatus.value): The status of the steps to retrieve. Defaults to pending.
                include_working (bool): Whether to include working steps. Defaults to True.
                reverse (bool): Whether to reverse the order of the steps. Defaults to False.

            Returns:
                list: A list of steps ordered by priority, velocity, and scope position.
            """
        global tag_usage

        # # Fetch velocities for each tag
        # velocity_sql = 'SELECT tag, velocity FROM tags'  # f'SELECT tag, velocity FROM tags WHERE scope IN ({",".join("?" * len(scopes))})'
        # cur.execute(velocity_sql)  # (velocity_sql, (*scopes,))
        # tag_velocities = dict(cur.fetchall())

        # # Initialize tag_usage for new tags
        # for tag in tag_velocities:
        #     if tag not in tag_usage:
        #         tag_usage[tag] = 0

        # case_statement = ' '.join([f"WHEN ? THEN {i}" for i in range(len(scopes))])
        # offset = 0
        # steps = []

        expiration_time = time.time() - (60 * 60 * .2)  # (60 * 60 * 2)
        status = f'{buelon.core.step.StepStatus(status).value}'
        working_clause = ''
        if include_working:
            working_clause = (f' or (epoch < {expiration_time} '
                              f'     and status = \'{buelon.core.step.StepStatus.working.value}\') ')

        order_by = 'priority desc, epoch'
        if reverse:
            order_by = 'priority asc, epoch'

        # ... (keep the existing logic)

        # sql = (f'SELECT id, priority, scope, velocity, tag '
        #        f'FROM steps '
        #        f'WHERE scope IN ({",".join("?" * len(scopes))}) '
        #        f'AND ('
        #        f'   status = \'{status}\' '
        #        f'   {working_clause}'
        #        f')'
        #        f'ORDER BY  '  # ' CASE scope {case_statement} END, '
        #        f'   {order_by} '  # , COALESCE(velocity, 1.0/0.0)
        #        f'LIMIT ? OFFSET ?')
        #
        # cur.execute(sql, (*scopes,  # *scopes,
        #                   chunk_size, offset))

        # Use a single query to fetch all necessary data
        query = f"""
        SELECT s.id -- s.id, s.priority, s.scope, s.velocity, s.tag, t.velocity as tag_velocity
        FROM steps s
        -- LEFT JOIN tags t ON s.tag = t.tag
        WHERE s.scope IN ({','.join('?' for _ in scopes)})
        AND (s.status = ? {working_clause})
        ORDER BY {order_by}
        LIMIT ?
        """

        params = (*scopes, status, limit)
        rows = self.execute_db_query(query, params)

        steps = []
        # tag_usage = {}

        # for row in rows:
        #     step_id, priority, scope, velocity, tag, tag_velocity = row
        #     if tag_velocity is None or tag_usage[tag] < tag_velocity:
        #         steps.append(step_id)
        #         tag_usage[tag] += 1
        #         if len(steps) >= limit:
        #             break

        for row in rows:
            steps.append(row[0])

        # Update status to working for selected steps
        if steps:
            update_query = f"""
            UPDATE steps 
            SET status = ?, epoch = ?
            WHERE id IN ({','.join('?' for _ in steps)})
            """
            update_params = (f'{buelon.core.step.StepStatus.working.value}', time.time(), *steps)
            self.execute_db_query(update_query, update_params)

        return steps


class HubClient:
    def __init__(self, host=None, port=None, max_reconnect_attempts=2):
        self.host = host or os.environ.get('PIPE_WORKER_HOST', 'localhost')
        self.port = port or int(os.environ.get('PIPE_WORKER_PORT', 65432))
        self.conn = None
        self.max_reconnect_attempts = max_reconnect_attempts

        if USING_POSTGRES:
            self.load_pg()

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
            # self.conn.settimeout(60)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def send_request(self, method, data: bytes, timeout: int | float = 5, increments: int | float | Callable = 5, attempts: int = 4):
        try:
            # socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
                conn.settimeout(timeout)
                conn.connect((self.host, self.port))
                request = method.value.encode() + PIPELINE_SPLIT_TOKEN + data
                send(conn, request)
                try:
                    response = timeout_func(receive, (conn,), timeout_duration=timeout, default='__fail__')
                except ValueError as e:
                    if 'signal only works in main thread of the main interpreter' not in str(e):
                        raise
                    import warnings
                    warnings.warn('HubClient used outside the main thread cannot use signal.py to timeout connections.')
                    socket.setdefaulttimeout(timeout)
                    conn.settimeout(timeout)
                    response = receive(conn,)
                if response == '__fail__':
                    raise TimeoutError(f"Request timed out after {timeout} seconds")
        except (TimeoutError, socket.timeout, ConnectionRefusedError) as e:
            if attempts <= 0:
                raise
            print(f"{type(e).__name__} error: {e}")
            time.sleep(1)  # Backoff before retrying
            timeout = timeout + increments if not callable(increments) else increments(timeout, attempts)
            return self.send_request(method, data, timeout, increments, attempts - 1)

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

    def get_steps(self, scopes, **kwargs):
        if USING_POSTGRES:
            return self.postgres_get_steps(scopes, **kwargs)
        return buelon.helpers.json_parser.loads(self.send_request(Method.GET_STEPS, buelon.helpers.json_parser.dumps([scopes, kwargs])))

    def get_rows(self, step_id):
        if USING_POSTGRES:
            return self.postgres_get_rows(step_id)
        return buelon.helpers.json_parser.loads(self.send_request(Method.FETCH_ROWS, buelon.helpers.json_parser.dumps({'step_id': step_id})))

    def done(self, step_id):
        if USING_POSTGRES:
            return self.postgres_done(step_id)
        return self.send_request(Method.DONE, step_id.encode())

    def dones(self, steps: list[buelon.core.step.Step]):
        if USING_POSTGRES:
            return self.postgres_dones(steps)
        for step in steps:
            self.done(step.id)

    def pending(self, step_id):
        if USING_POSTGRES:
            return self.postgres_pending(step_id)
        return self.send_request(Method.PENDING, step_id.encode())

    def pendings(self, steps: list[buelon.core.step.Step]):
        if USING_POSTGRES:
            return self.postgres_pendings(steps)
        for step in steps:
            self.pending(step.id)

    def cancel(self, step_id):
        if USING_POSTGRES:
            return self.postgres_cancel(step_id)
        return self.send_request(Method.CANCEL, step_id.encode())

    def cancels(self, steps: list[buelon.core.step.Step]):
        if USING_POSTGRES:
            return self.postgres_cancels(steps)
        for step in steps:
            self.cancel(step.id)

    def reset(self, step_id):
        if USING_POSTGRES:
            return self.postgres_reset(step_id)
        return self.send_request(Method.RESET, step_id.encode())

    def resets(self, steps: list[buelon.core.step.Step]):
        if USING_POSTGRES:
            return self.postgres_resets(steps)
        for step in steps:
            self.reset(step.id)

    def error(self, step_id, msg, trace):
        if USING_POSTGRES:
            return self.postgres_error(step_id, msg, trace)
        return self.send_request(Method.ERROR, buelon.helpers.json_parser.dumps({'step_id': step_id, 'msg': msg, 'trace': trace}))

    def upload_step(self, step_json, status_value):
        if USING_POSTGRES:
            return self.postgres_upload_step(step_json, status_value)
        return self.send_request(Method.UPLOAD_STEP, buelon.helpers.json_parser.dumps([step_json, status_value]))

    def upload_steps(self, step_jsons, status_values):
        if USING_POSTGRES:
            return self.postgres_upload_steps(step_jsons, status_values)
        return self.send_request(Method.UPLOAD_STEPS, buelon.helpers.json_parser.dumps([step_jsons, status_values]))

    def get_step_count(self, types: str | None = None):
        if USING_POSTGRES:
            return self.postgres_get_step_count(types)
        return buelon.helpers.json_parser.loads(
            self.send_request(
                Method.STEP_COUNT,
                buelon.helpers.json_parser.dumps({'types': types}),
                timeout=60,
                attempts=0
            )
        )

    def reset_errors(self, include_workers: bool):
        if USING_POSTGRES:
            return self.postgres_reset_errors(include_workers)
        return self.send_request(Method.RESET_ERRORS, b'true' if include_workers else b'false', timeout=60 * 10, increments=30)

    def delete_steps(self):
        if USING_POSTGRES:
            return self.postgres_delete_steps()
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
        if USING_POSTGRES:
            return self.postgres_fetch_errors(count, exclude)
        return buelon.helpers.json_parser.loads(
            self.send_request(
                Method.FETCH_ERRORS,
                buelon.helpers.json_parser.dumps({
                    'count': count,
                    'exclude': exclude
                })#f'{count}'.encode()
            )
        )

    def load_pg(self):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        q = '''create table if not exists buelon_jobs (
            id text,
            priority int,
            scope text,
            timeout real,
            retries int,
            velocity real,
            tag text,
            status text,
            epoch real,
            msg text,
            trace text,
            state text,
            unique(id, state)
        )
        PARTITION BY LIST (state);'''
        qs = [
            q,
            'CREATE TABLE IF NOT EXISTS tags (tag text primary key, velocity real);',
            'create index if not exists idx_buelon_jobs_scope on buelon_jobs using hash (scope);',
            'create index if not exists idx_buelon_jobs_status on buelon_jobs using hash (status);',
            'create index if not exists idx_buelon_jobs_priority on buelon_jobs (priority);',
            'create index if not exists idx_buelon_jobs_epoch on buelon_jobs (epoch);',
            'CREATE INDEX if not exists idx_buelon_jobs_active_scope_status_priority_epoch ON buelon_jobs (scope, status, priority, epoch);',
            'CREATE TABLE IF NOT EXISTS "buelon_jobs_waiting" PARTITION OF "buelon_jobs" FOR VALUES IN (\'waiting\');',
            'CREATE TABLE IF NOT EXISTS "buelon_jobs_active" PARTITION OF "buelon_jobs" FOR VALUES IN (\'active\');',
            'CREATE TABLE IF NOT EXISTS "buelon_jobs_archived" PARTITION OF "buelon_jobs" FOR VALUES IN (\'archived\');',
        ]
        try:
            for q in qs:
                self.db.query(q)
            self.db.upload_table('buelon_status_map', [
                {'value': e.value, 'name': e.name}
                for e in buelon.core.step.StepStatus
            ], id_column='value')
        except buelon.helpers.postgres.psycopg2.errors.InsufficientPrivilege:
            print('Insufficient privileges to create table.')

    def postgres_get_steps(
            self,
            scopes: list[str],
            limit=100,
            chunk_size=100,
            status=buelon.core.step.StepStatus.pending.value,
            include_working=True,
            reverse=False,
            min_table_size: int = 2000
    ) -> list[str]:
        """
        Get the steps in the scope, ordered by priority, velocity, then scope position.

        Args:
            scopes (list): A list of scopes to filter the steps.
            limit (int): The maximum number of steps to retrieve. Defaults to 50.
            chunk_size (int): The number of steps to fetch in each chunk. Defaults to 100.
            status (int | buelon.core.step.StepStatus.value): The status of the steps to retrieve. Defaults to pending.
            include_working (bool): Whether to include working steps. Defaults to True.
            reverse (bool): Whether to reverse the order of the steps. Defaults to False.

        Returns:
            list: A list of steps ordered by priority, velocity, and scope position.
        """
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        # expiration_time = time.time() - (60 * 60 * .2)  # (60 * 60 * 2)
        #
        # working_clause = ''
        # if include_working:
        #     working_clause = (f' or (epoch < {expiration_time} '
        #                       f'     and status = \'{buelon.core.step.StepStatus.working.value}\') ')
        #
        # order_by = 'priority desc, epoch'
        # if reverse:
        #     order_by = 'priority asc, epoch'
        #
        # if len(scopes) == 1:
        #     scope_section = f"s.scope = '{scopes[0]}'"
        # else:
        #     scope_section = f"""s.scope IN ({','.join(f"'{s}'" for s in scopes)})"""
        #
        # # Use a single query to fetch all necessary data
        # query = f"""
        # drop table if exists current_jobs;
        # create temp table current_jobs as
        # SELECT s.id -- s.id, s.priority, s.scope, s.velocity, s.tag, t.velocity as tag_velocity
        # FROM buelon_jobs_active s
        # -- LEFT JOIN tags t ON s.tag = t.tag
        # WHERE {scope_section}
        # AND (s.status = '{buelon.core.step.StepStatus.pending.value}' {working_clause})
        # ORDER BY {order_by}
        # LIMIT {limit};
        #
        # UPDATE buelon_jobs_active
        # SET status = '{buelon.core.step.StepStatus.working.value}', epoch = {time.time()}
        # WHERE id IN (select id from current_jobs);
        #
        # select *
        # from current_jobs;
        # """

        expiration_time = time.time() - (60 * 60 * 0.2)  # (60 * 60 * 2)

        working_clause = ''
        if include_working:
            # working_clause = f"OR (epoch < {expiration_time} AND status = '{buelon.core.step.StepStatus.working.value}')"
            working_clause = f"OR (epoch < coalesce(timeout, {expiration_time}) AND status = '{buelon.core.step.StepStatus.working.value}')"

        order_by = 'priority DESC, epoch' if not reverse else 'priority ASC, epoch'

        # scope_section = "s.scope = %s" if len(scopes) == 1 else "s.scope = ANY(%s)"
        if len(scopes) == 1:
            scope_section = f"s.scope = '{scopes[0]}'"
        else:
            # scope_section = f"""s.scope IN ({','.join(f"'{s}'" for s in scopes)})"""
            # same as above
            scope_section = f"""s.scope = ANY(array[{','.join(f"'{s}'" for s in scopes)}])"""

        query = f"""
        WITH updated_jobs AS (
            UPDATE buelon_jobs_active s
            SET status = '{buelon.core.step.StepStatus.working.value}', epoch = {time.time()}
            WHERE s.id IN (
                SELECT s.id
                FROM buelon_jobs_active s
                WHERE {scope_section}
                AND (s.status = '{buelon.core.step.StepStatus.pending.value}' {working_clause})
                ORDER BY {order_by}
                LIMIT {limit}
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
        )
        SELECT * FROM updated_jobs;
        """

        rows = self.db.download_table(sql=query)

        def check_active_state(min_table_size, scope_section, working_clause, order_by):
            db = buelon.helpers.postgres.get_postgres_from_env()
            row = db.download_table(sql=f'select count(*) as amount, min(priority) as priority from buelon_jobs_active where status != \'{buelon.core.step.StepStatus.working.value}\';')[0]
            amount = row['amount']
            priority = row['priority']  # waiting
            row = db.download_table(sql=f'select max(priority) as priority from buelon_jobs_waiting;')[0]
            waiting_priority = row['priority']
            if amount < min_table_size or priority < waiting_priority:
                query = f"""
                UPDATE buelon_jobs s
                SET state = 'active'
                WHERE s.id IN (
                    SELECT s.id
                    FROM buelon_jobs s
                    WHERE {scope_section}
                    AND (s.status = '{buelon.core.step.StepStatus.pending.value}' {working_clause})
                    ORDER BY {order_by}
                    LIMIT {min_table_size}
                    FOR UPDATE SKIP LOCKED
                );"""
                db.query(query)

        thread = threading.Thread(
            target=check_active_state,
            args=(min_table_size, scope_section, working_clause, order_by)
        )
        thread.start()

        return [row['id'] for row in rows]

    def postgres_get_rows(self, step_id):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        if ',' in step_id:
            return [
                row
                for s in step_id.split(',')
                for row in self.postgres_get_rows(s.strip())
            ]

        table = self.db.download_table(sql=f"SELECT * FROM buelon_jobs WHERE id = '{step_id}';")

        for row in table:
            row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

        return table

    def postgres_done(self, step_id):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        _step = get_step(step_id)
        status = buelon.core.step.StepStatus.success.value
        self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'archived\' WHERE id = '{_step.id}'")

        if _step.children:
            status = buelon.core.step.StepStatus.pending.value
            end = f" id = '{_step.children[0]}'" \
                if len(_step.children) == 1 else \
                f' id IN (' + ', '.join(f"'{_id}'" for _id in _step.children) + ')'
            # self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'active\' WHERE {end}")
            self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE {end}")

    def postgres_dones(self, steps: list[buelon.core.step.Step]):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        # with db.connect() as conn:
        #     cur = conn.cursor()
        #     buelon.helpers.postgres.psycopg2.extras.execute_batch(cur, sql, tuple(values))
        #     conn.commit()

        status = buelon.core.step.StepStatus.success.value
        where = 'id in (' + ', '.join(f"'{s.id}'" for s in steps) + ')'
        self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'archived\' WHERE {where}")
        children = [c for s in steps for c in s.children]
        if children:
            status = buelon.core.step.StepStatus.pending.value
            end = f" id = '{children[0]}'" \
                if len(children) == 1 else \
                f' id IN (' + ', '.join(f"'{_id}'" for _id in children) + ')'
            # self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'active\' WHERE {end}")
            self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE {end}")

    def postgres_pending(self, step_id):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        status = buelon.core.step.StepStatus.pending.value
        # self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'active\' WHERE id = '{step_id}';")
        self.db.query(
            f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE id = '{step_id}';")


    def postgres_pendings(self, steps: list[buelon.core.step.Step]):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        status = buelon.core.step.StepStatus.pending.value

        where = 'id in (' + ', '.join(f"'{s.id}'" for s in steps) + ')'
        self.db.query(
            f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE {where};")


    def postgres_cancel(self, step_id: str, __already: set | None = None):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        first_iteration = __already is None
        _step = get_step(step_id)
        __already = set() if first_iteration else __already

        status = buelon.core.step.StepStatus.cancel.value
        self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'archived\' WHERE id = '{_step.id}'")

        if _step.parents:
            for parent in _step.parents:
                if parent not in __already:
                    __already.add(parent)
                    self.postgres_cancel(parent, __already)

        if _step.children:
            for child in _step.children:
                if child not in __already:
                    __already.add(child)
                    self.postgres_cancel(child, __already)

    def postgres_cancels(self, steps: list[buelon.core.step.Step], __already: set | None = None):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        first_iteration = __already is None
        __already = set() if first_iteration else __already

        where = 'id in (' + ', '.join(f"'{s.id}'" for s in steps) + ')'
        status = buelon.core.step.StepStatus.cancel.value
        self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'archived\' WHERE {where}")

        cancels = [p for s in steps for p in s.parents if p not in __already]
        cancels += [c for s in steps for c in s.children if c not in __already]

        if cancels:
            __already |= set(cancels)
            self.postgres_cancels(list(bulk_get_step(cancels).values()), __already)

    def postgres_reset(self, step_id, __already: set | None = None):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()
        _step = get_step(step_id)
        __already = set() if not __already else __already
        status = buelon.core.step.StepStatus.queued.value if _step.parents else buelon.core.step.StepStatus.pending.value
        state = 'waiting' if _step.parents else 'active'

        # self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'{state}\' WHERE id = '{_step.id}'")
        self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE id = '{_step.id}'")

        if _step.children:
            for child in _step.children:
                if child not in __already:
                    __already.add(child)
                    self.postgres_reset(child, __already)

        if _step.parents:
            for parent in _step.parents:
                if parent not in __already:
                    __already.add(parent)
                    self.postgres_reset(parent, __already)

    def postgres_resets(self, steps: list[buelon.core.step.Step], __already: set | None = None):
        if not hasattr(self, 'db') or not self.db:
            self.db = buelon.helpers.postgres.get_postgres_from_env()

        first_iteration = __already is None
        __already = set() if first_iteration else __already

        pendings = [s.id for s in steps if not s.parents]
        queued = [s.id for s in steps if s.parents]
        if pendings:
            where = 'id in (' + ', '.join(f"'{s}'" for s in pendings) + ')'
            status = buelon.core.step.StepStatus.pending.value
            self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE {where}")
        if queued:
            where = 'id in (' + ', '.join(f"'{s}'" for s in queued) + ')'
            status = buelon.core.step.StepStatus.queued.value
            self.db.query(f"UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, state=\'waiting\' WHERE {where}")

        resets = [p for s in steps for p in s.parents if p not in __already]
        resets += [c for s in steps for c in s.children if c not in __already]

        if resets:
            __already |= set(resets)
            self.postgres_resets(list(bulk_get_step(resets).values()), __already)

    def postgres_error(self, step_id, msg, trace):
        db = buelon.helpers.postgres.get_postgres_from_env()

        status = buelon.core.step.StepStatus.error.value
        # sql_update_step = f"""UPDATE buelon_jobs_active SET status = \'{status}\', epoch = {time.time()},
        #      msg = $msg${msg}$msg$, trace = $trace${trace}$trace$ WHERE id = '{step_id}'"""
        sql_update_step = f"""UPDATE buelon_jobs SET status = \'{status}\', epoch = {time.time()}, 
             msg = $msg${msg}$msg$, trace = $trace${trace}$trace$, state=\'waiting\' WHERE id = '{step_id}'"""

        db.query(sql_update_step)

    def postgres_upload_step(self, step_json, status_value):
        return self.postgres_upload_steps([step_json], [status_value])

    def postgres_upload_steps(self, step_jsons, status_values):
        db = buelon.helpers.postgres.get_postgres_from_env()

        sql = ('INSERT INTO buelon_jobs_waiting (id, priority, scope, timeout, retries, velocity, tag, status, epoch, msg, trace, state) '
               'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')

        values = []

        for step_json, status_value in zip(step_jsons, status_values):
            status = buelon.core.step.StepStatus(status_value)
            _step: buelon.core.step.Step = buelon.core.step.Step().from_json(step_json)
            state = 'waiting'  # 'active' if status == buelon.core.step.StepStatus.pending else 'waiting'
            params = (_step.id, _step.priority, _step.scope, _step.timeout, _step.retries, _step.velocity, _step.tag, f'{status.value}', time.time(), '', '', state)
            values.append(params)

        with db.connect() as conn:
            cur = conn.cursor()

            buelon.helpers.postgres.psycopg2.extras.execute_batch(cur, sql, tuple(values))

            conn.commit()

    def postgres_get_step_count(self, types: str | None = None):
        db = buelon.helpers.postgres.get_postgres_from_env()

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
            buelon_jobs
        {where}
        group by 
            status;
        '''
        table = db.download_table(sql=query)
        for row in table:
            row['status'] = buelon.core.step.StepStatus(int(row['status'])).name

        return table

    def postgres_reset_errors(self, include_workers: bool=False):
        db = buelon.helpers.postgres.get_postgres_from_env()
        suffix = '' if not include_workers else f' or status = \'{buelon.core.step.StepStatus.working.value}\''
        query = f'''
            update buelon_jobs
            set status = \'{buelon.core.step.StepStatus.pending.value}\'
            where status = \'{buelon.core.step.StepStatus.error.value}\'
            {suffix};'''
        db.query(query)

    def postgres_delete_steps(self):
        db = buelon.helpers.postgres.get_postgres_from_env()
        db.query('delete from buelon_jobs;')

    def postgres_fetch_errors(self, count: int, exclude: list[str] | str | None = None) -> dict:
        try:
            count = int(count)
            exclude = exclude

            if not isinstance(exclude, (type(None), str, list)):
                raise ValueError('exclude must be a string or a list')

            if isinstance(exclude, list) and not all(isinstance(x, str) for x in exclude):
                raise ValueError('exclude must be a list of strings')
        except ValueError:
            count = 5
            exclude = None
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

        sql = f'SELECT id, msg, trace FROM buelon_jobs WHERE status = \'{buelon.core.step.StepStatus.error.value}\' {exclude_query}  LIMIT {count}'
        error_size_query = f'SELECT COUNT(*)  FROM buelon_jobs WHERE status = \'{buelon.core.step.StepStatus.error.value}\' {exclude_query}'

        db = buelon.helpers.postgres.get_postgres_from_env()

        table = db.download_table(sql=sql)
        error_size = db.query(error_size_query)[0][0]

        for row in table:
            row['step'] = get_step(row['id']).to_json()

        return {
            'total': error_size,
            'count': len(table),
            'table': table
        }

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
