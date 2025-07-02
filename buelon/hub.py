
import os
import uuid
import time
import socket
import asyncio
import datetime
import traceback
import threading
import contextlib
from typing import Any

import orjson
import asyncio_pool

from buelon.settings import settings
import buelon


# region global variables

boo_db = buelon.helpers.postgres.get_postgres_from_env()


ALL_STEPS: dict[str, list[str, buelon.step.Step]] = {}
STEPS: dict[str, dict[int, list[buelon.core.step.Step]]] = {}
queued: dict[str, buelon.step.Step] = {}
errors: dict[str, buelon.step.Step] = {}
done: dict[str, buelon.step.Step] = {}

holds: dict[str, list[buelon.step.Step]] = {}

db: dict[str, Any] = {}  # : dict[str, bytes] = {}

step_count = 10
END_TOKEN = b'[-_-]'
SPLIT_TOKEN = b'|(--)|'
SPLIT_TOKEN2 = b'|{**}|'
LENGTH_OF_END_TOKEN = len(END_TOKEN)

preset_priorities = list(range(100, -1, -1)) # 100 - 0

# endregion

# region handling steps

def get_steps(scopes: list[str], limit: int = 100):
    result = []
    skip = set()

    while len(result) < limit:
        got_data = False

        for s in scopes:
            r = []

            if s not in skip and s in STEPS:
                for i in preset_priorities:
                    if i in STEPS[s] and STEPS[s][i]:
                        r.extend(STEPS[s][i][:step_count])
                        STEPS[s][i] = STEPS[s][i][step_count:]
                        break

            if not r:
                skip.add(s)
            else:
                got_data = True

            result.extend(r)

            if len(result) >= limit:
                break

        if not got_data:
            break

    return result


def get_steps_v2(scopes: list[str], limit: int = 100, reverse: bool = False, single_step: str | None = None):
    if single_step:
        s = pop_step_from_id(single_step)

        if s:
            return [s]

        return []

    result = []
    _preset_priorities = preset_priorities[::-1] if reverse else preset_priorities

    if reverse:
        scopes = scopes[::-1]

    def get_scope_and_priority():
        for i in _preset_priorities:
            for s in scopes:
                if s in STEPS and i in STEPS[s] and STEPS[s][i]:
                    yield s, i

    for scope, priority in get_scope_and_priority():
        sl = max(0, limit - len(result))
        result.extend(STEPS[scope][priority][:sl])
        STEPS[scope][priority] = STEPS[scope][priority][sl:]

        if len(result) >= limit:
            break

    return result


def add_step_to_steps(step: buelon.core.step.Step, jobs: list[buelon.core.step.Step]):
    jobs.append(step)


def upload_step(job: buelon.core.step.Step):
    if job.scope not in STEPS:
        STEPS[job.scope] = {}

    if job.priority not in STEPS[job.scope]:
        STEPS[job.scope][job.priority] = []

    add_step_to_steps(job, STEPS[job.scope][job.priority])


def upload_steps(jobs: list[buelon.core.step.Step]):
    for job in jobs:
        upload_step(job)


def handle_step(step:  buelon.core.step.Step, status: buelon.core.step.StepStatus):
    if status == buelon.core.step.StepStatus.pending:
        ALL_STEPS[step.id] = [status.value, step]
        upload_step(step)
    elif status == buelon.core.step.StepStatus.cancel:
        for step_id in get_all_ids(step):
            remove_id(step_id)
    elif status == buelon.core.step.StepStatus.error:
        ALL_STEPS[step.id] = [status.value, step]
        errors[step.id] = step
    elif status == buelon.core.step.StepStatus.reset:
        for step in get_all_steps(step):
            remove_id(step.id, True)
            if step.parents:
                ALL_STEPS[step.id] = [status.queued.value, step]
                queued[step.id] = step
            else:
                ALL_STEPS[step.id] = [status.pending.value, step]
                upload_step(step)
    elif status == buelon.core.step.StepStatus.success:
        ALL_STEPS[step.id] = [status.value, step]
        done[step.id] = step
        if step.children:
            for step_id in step.children:
                if step_id in queued:
                    ALL_STEPS[step.id] = [status.pending.value, ALL_STEPS[step.id][1]]
                    upload_step(queued[step_id])
                    del queued[step_id]
        else:
            ids = get_all_ids(step)
            if all([i in done for i in ids]):
                for step_id in ids:
                    remove_id(step_id)

# endregion

# region util

def step_from_id(step_id: str):
    if step_id in ALL_STEPS:
        return ALL_STEPS[step_id][1]
    if step_id in queued:
        return queued[step_id]
    elif step_id in errors:
        return errors[step_id]
    elif step_id in done:
        return done[step_id]
    for hold in holds.values():
        for s in hold:
            if s.id == step_id:
                return s


def pop_step_from_id(step_id: str):
    if step_id in queued:
        # return queued[step_id]
        s = queued[step_id]
        del queued[step_id]
        return s
    elif step_id in errors:
        # return errors[step_id]
        s = errors[step_id]
        del errors[step_id]
        return s
    elif step_id in done:
        # return done[step_id]
        s = done[step_id]
        del done[step_id]
        return s
    for hold in holds.values():
        for s in hold:
            if s.id == step_id:
                # # removing s would cause errors
                # hold.remove(s)
                return s
    if step_id in ALL_STEPS:
        s = ALL_STEPS[step_id][1]
        # del ALL_STEPS[step_id]
        return s


def remove_id(step_id: str, skip_all_ids: bool = False):
    if step_id in queued:
        del queued[step_id]
    if step_id in errors:
        del errors[step_id]
    if step_id in done:
        del done[step_id]

    if step_id in db:
        del db[step_id]

    if step_id in ALL_STEPS and not skip_all_ids:
        del ALL_STEPS[step_id]


def get_all_ids(step:  buelon.core.step.Step, already: set | None = None):
    already = already or set()

    if step.id in already:
        return already

    already.add(step.id)

    for child in step.children:
        get_all_ids(step_from_id(child), already)

    for parent in step.parents:
        get_all_ids(step_from_id(parent), already)

    return already


def get_all_steps(step:  buelon.core.step.Step, already: dict | None = None):
    already = already or {}

    if step.id in already:
        return already

    already[step.id] = step

    for child in step.children:
        get_all_steps(step_from_id(child), already)

    for parent in step.parents:
        get_all_steps(step_from_id(parent), already)

    return already


def steps_to_bytes(steps:  list[buelon.core.step.Step]) -> bytes:
    return orjson.dumps([step.to_json() for step in steps])


def bytes_to_steps(data: bytes) -> list[buelon.core.step.Step]:
    return [buelon.core.step.Step().from_json(step) for step in orjson.loads(data)]


def all_steps_to_bytes() -> bytes:
    return orjson.dumps([[step[0], step[1].to_json()] for step in ALL_STEPS.values()])


def bytes_to_all_steps(data: bytes) -> None:
    global ALL_STEPS
    # ALL_STEPS = {step[1].id: [step[0], step[1]] for step in orjson.loads(data)}
    for row in orjson.loads(data):
        row[1] = buelon.core.step.Step().from_json(row[1])
        ALL_STEPS[row[1].id] = row

# endregion

# region socket communication

def receive(conn: socket.socket) -> bytes:
    data = b''
    last_data = None
    while not data.endswith(END_TOKEN):
        v = conn.recv(1024)
        data += v

        if not v and last_data == b'' and not data.endswith(END_TOKEN):
            try:
                data = data.decode()
            except: pass
            raise ValueError(f'Invalid value received: `{data}`')

        last_data = v
    return data[:-LENGTH_OF_END_TOKEN]


def send(conn: socket.socket, data: bytes) -> None:
    conn.sendall(data+END_TOKEN)

# endregion

# region client

@contextlib.contextmanager
def make_promise(scopes: list[str], reverse: bool = False, single_step: str | None = None):
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'get', orjson.dumps({'scopes': scopes, 'reverse': reverse, 'single_step': single_step})]))
        data = receive(s)
        steps, args = data.split(SPLIT_TOKEN)
        # print(args)

        def commit(steps: list[buelon.core.step.Step], statuses: list[buelon.core.step.StepStatus], results: list[Any]):
            send(s, SPLIT_TOKEN.join([
                steps_to_bytes(steps),
                orjson.dumps([status.value for status in statuses]),
                orjson.dumps(results)
            ]))
            receive(s)

        yield bytes_to_steps(steps), orjson.loads(args), commit
    finally:
        s.close()


def upload_file_to_server(file_path: str):
    with open(file_path) as f:
        code = f.read()

    upload_code_to_server(code)


def upload_code_to_server(code: str):
    chunk = []

    for step in buelon.core.pipe_interpreter.generate_steps_from_code(code):
        chunk.append(step)
        if len(chunk) >= 500:
            upload_steps_to_server(chunk)
            chunk = []

    if chunk:
        upload_steps_to_server(chunk)


def upload_steps_to_server(steps: list[buelon.core.step.Step]):
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'upload', steps_to_bytes(steps)]))
        receive(s)


def display_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'display', b'nothing']))
        data = receive(s)
    print(data.decode('utf-8'))


def display_errors_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'errors', b'nothing']))
        data = receive(s)
    # print(data.decode('utf-8'))
    steps, _errors = data.split(SPLIT_TOKEN)
    steps = bytes_to_steps(steps)
    _errors = orjson.loads(_errors)

    output = []

    for step, e in zip(steps, _errors):
        output.append(f'name: {step.name} | job id: {step.id}')
        # print(step.name, '|', step.id)
        if isinstance(e, dict):
            # print(f'Error: {e.get("error")}')
            # print(f'Traceback:\n{e.get("trace")}')
            output[-1] += f'\n\nError: {e.get("error")}'
            output[-1] += f'\nTraceback:\n{e.get("trace")}'

    print('\n\n----****----\n\n'.join(output))


def reset_errors_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'reset-errors', b'nothing']))
        receive(s)


def cancel_errors_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'cancel-errors', b'nothing']))
        receive(s)


def get_all_info_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'get-all-info', b'nothing']))
        data = receive(s)

    the_steps, all_done, all_queued, all_errors, all_db = data.split(SPLIT_TOKEN)
    return [the_steps, all_done, all_errors, all_queued, all_db]


def save_from_server():
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'save', b'nothing']))
        receive(s)

# endregion

# region server


def display_text():
    steps_len = sum([len(lst) for val in STEPS.values() for lst in val.values()])
    holds_len = sum([len(lst) for lst in holds.values()])
    text = (f'done: {len(done):,}'
            f', queued: {len(queued):,}'
            f', errors: {len(errors):,}'
            f', steps: {steps_len:,}'
            f', holds: {holds_len:,}'
            f', total: {steps_len + holds_len + len(done) + len(queued) + len(errors):,}')
    return text


last_display = time.time()
time_to_next_display = 2.0
def display():
    global last_display, time_to_next_display
    if time.time() - last_display < time_to_next_display:
        return
    steps_len = sum([len(lst) for val in STEPS.values() for lst in val.values()])
    print(f'done: {len(done):,}, queued: {len(queued):,}, errors: {len(errors):,}, steps: {steps_len}, total: {steps_len + len(done) + len(queued) + len(errors):,}')
    last_display = time.time()


def temp_get_all_ids(step:  buelon.core.step.Step, already: set | None = None, has_none: dict | None = None):
    already = already or set()
    has_none = has_none or {}

    if step.id in already:
        return already

    already.add(step.id)

    for child in step.children:
        s = step_from_id(child)
        if s:
            get_all_ids(s, already)
        else:
            has_none['has_none'] = True

    for parent in step.parents:
        s = step_from_id(parent)
        if s:
            get_all_ids(s, already)
        else:
            has_none['has_none'] = True

    return has_none, already


def temp_handle_step_args(step: buelon.core.step.Step):
    args = []
    for parent in step.parents:
        if parent not in db:
            handle_step(step, buelon.core.step.StepStatus.reset)
            # has_none, tmp_ids = temp_get_all_ids(step)
            # if has_none.get('has_none', False):
            #     for step_id in tmp_ids:
            #         remove_id(step_id)
            # else:
            #     handle_step(step, buelon.core.step.StepStatus.reset)
            return False, None
        args.append(db[parent])
    return True, args


def get_args(steps):
    # # old
    # args = [[db[parent] for parent in step.parents] for step in steps]

    # new
    args = []
    new_steps = []
    for step in steps:
        res, arg = temp_handle_step_args(step)

        if res:
            args.append(arg)
            new_steps.append(step)

    return new_steps, args


def hold_promise(s):
    global errors
    # WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    # WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #     s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    #     s.connect((WORKER_HOST, WORKER_PORT))
    with s:
        method, data = receive(s).split(SPLIT_TOKEN)
        print(f'method: {method}')
        if method == b'get':
            uid = f'{uuid.uuid1()}'
            try:
                holds[uid] = get_steps_v2(**orjson.loads(data))
                steps = holds[uid]
                try:
                    # args = [[db[parent] for parent in step.parents] for step in steps]
                    steps, args = get_args(steps)
                    send(s, SPLIT_TOKEN.join([steps_to_bytes(steps), orjson.dumps(args)]))
                    steps, statuses, results = receive(s).split(SPLIT_TOKEN)
                    steps = bytes_to_steps(steps)
                    statuses = [buelon.core.step.StepStatus(status) for status in orjson.loads(statuses)]
                    results = orjson.loads(results)
                    # print('uploading', len(steps), 'steps')
                    for step, status, result in zip(steps, statuses, results):
                        db[step.id] = result
                        handle_step(step, status)
                        # if s == buelon.core.step.StepStatus.success:
                    display()
                    send(s, b'ok')
                except Exception as e:
                    upload_steps(steps)
                    raise
                finally:
                    if uid in holds:
                        del holds[uid]
            except Exception as e:
                s.close()
                traceback.print_exc()
                row = {'uid': uid, 'error': str(e), 'trace': traceback.format_exc(), 'utc': datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc)}
                boo_db.upload_table('boo_errors', [row], id_column='uid')
        elif method == b'upload':
            steps = bytes_to_steps(data)
            for step in steps:
                if step.parents:
                    ALL_STEPS[step.id] = [buelon.core.step.StepStatus.queued.value, step]
                    queued[step.id] = step
                else:
                    ALL_STEPS[step.id] = [buelon.core.step.StepStatus.pending.value, step]
                    upload_step(step)
            send(s, b'ok')
        elif method == b'display':
            text = display_text()
            send(s, text.encode('utf-8'))
        elif method == b'errors':
            # print('errors:', orjson.loads(data))
            res = []
            for step_id in errors:
                if isinstance(db.get(step_id), dict) and 'error' in db[step_id] and 'trace' in db[step_id]:
                    res.append(db[step_id])
                else:
                    res.append({'error': 'Unknown error', 'trace': ''})
            send(s, SPLIT_TOKEN.join([
                steps_to_bytes(list(errors.values())),
                orjson.dumps(res)
            ]))
        elif method == b'reset-errors':
            _steps = list(errors.values())
            errors = {}
            upload_steps(_steps)
            send(s, b'ok')
        elif method == b'cancel-errors':
            # for step in list(errors.values()):
            #     for step_id in get_all_ids(step):
            #         remove_id(step_id)
            # # # Remove all steps
            for _, lst in ALL_STEPS.items():
                __, s = lst
                remove_id(s.id)
            # for sid in ALL_STEPS:
            #     remove_id(sid)
            send(s, b'ok')
        elif method == b'get-all-info':
            b = SPLIT_TOKEN.join([
                steps_to_bytes([step for scope in STEPS.values() for steps in scope.values() for step in steps]),
                steps_to_bytes(list(done.values())),
                steps_to_bytes(list(queued.values())),
                steps_to_bytes(list(errors.values())),
                orjson.dumps(db)
            ])
            send(s, b)
        elif method == b'save':
            auto_save()
            send(s, b'ok')


class Server:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

            while True:
                client_socket, addr = server.accept()
                print(f"Connection from {addr}")
                client_thread = threading.Thread(target=hold_promise, args=(client_socket,), daemon=True)
                client_thread.start()
        finally:
            server.shutdown(socket.SHUT_RDWR)
            server.close()
            exit()

# endregion

# region worker

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def worker():
    asyncio.run(_worker())


async def _worker():
    cleaner_task = asyncio.create_task(buelon.worker_v1.cleaner())

    for i in range(100):
        print('work', i)
        await work()

    cleaner_task.cancel()


async def work(single_step: str | None = None):
    async def run(step, arg):
        step: buelon.core.step.Step
        print('handling', step.name)
        try:
            r: buelon.core.step.Result = step.run(*arg)
        except Exception as e:
            print(e)
            traceback.print_exc()
            return step, buelon.core.step.StepStatus.error, {'error': str(e), 'trace': traceback.format_exc()}
        return step, r.status, r.data

    with make_promise(settings.worker.scopes.split(','), settings.worker.reverse, single_step) as (steps, args, commit):
        if not steps:
            await asyncio.sleep(5)
            return commit([], [], [])
        steps: list[buelon.core.step.Step]
        statuses: list[buelon.core.step.StepStatus] = []
        results: list[Any] = []
        # for step, arg in zip(steps, args):
        #     step: buelon.core.step.Step
        #     # print(step, arg)
        #     r: buelon.core.step.Result = step.run(*arg)
        #     statuses.append(r.status)
        #     results.append(r.data)
        lst = list(zip(steps, args))
        # for chunk in chunks(lst, 10):
        #     for step, status, result in await asyncio.gather(*[run(step, arg) for step, arg in chunk]):
        #         statuses.append(status)
        #         results.append(result)

        async def _run(data):
            step, arg = data
            return await run(step, arg)

        pool = asyncio_pool.AioPool(size=10)

        for step, status, result in (await pool.map(_run, lst)):  # await asyncio.gather(*[run(step, arg) for step, arg in zip(steps, args)]):
            statuses.append(status)
            results.append(result)

        commit(steps, statuses, results)

# endregion

# region auto save

auto_saving = True


def auto_save_task():
    global auto_saving
    while auto_saving:
        auto_save()
        time.sleep(60 * 10)


def auto_save():
    dir = '.auto_save'
    os.makedirs(dir, exist_ok=True)
    if not auto_saving:
        return
    with open(os.path.join(dir, 'all_steps'), 'wb') as f:
        b = all_steps_to_bytes()
        f.write(b)
    with open(os.path.join(dir, 'steps'), 'wb') as f:
        b = steps_to_bytes([step for scope in STEPS.values() for steps in scope.values() for step in steps])
        f.write(b)
    with open(os.path.join(dir, 'done'), 'wb') as f:
        b = steps_to_bytes(list(done.values()))
        f.write(b)
    with open(os.path.join(dir, 'queued'), 'wb') as f:
        b = steps_to_bytes(list(queued.values()))
        f.write(b)
    with open(os.path.join(dir, 'errors'), 'wb') as f:
        b = steps_to_bytes(list(errors.values()))
        f.write(b)
    with open(os.path.join(dir, 'holds'), 'wb') as f:
        b = steps_to_bytes([step for steps in holds.values() for step in steps])
        f.write(b)
    with open(os.path.join(dir, 'db'), 'wb') as f:
        b = orjson.dumps(db)
        f.write(b)


def auto_load():
    dir = '.auto_save'
    if not os.path.exists(dir):
        return
    if os.path.exists(os.path.join(dir, 'db')):
        with open(os.path.join(dir, 'db'), 'rb') as f:
            b = f.read()
            _db = orjson.loads(b)
            db.update(_db)
    if os.path.exists(os.path.join(dir, 'all_steps')):
        with open(os.path.join(dir, 'all_steps'), 'rb') as f:
            b = f.read()
            bytes_to_all_steps(b)
        for status, step in ALL_STEPS.values():
            if status == buelon.core.step.StepStatus.queued.value:
                queued[step.id] = step
            else:
                handle_step(step, buelon.core.step.StepStatus(status))
        if ALL_STEPS:
            return
    if os.path.exists(os.path.join(dir, 'steps')):
        with open(os.path.join(dir, 'steps'), 'rb') as f:
            b = f.read()
            _steps = bytes_to_steps(b)
            upload_steps(_steps)
            for step in _steps:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.pending.value, step]
    if os.path.exists(os.path.join(dir, 'done')):
        with open(os.path.join(dir, 'done'), 'rb') as f:
            b = f.read()
            _done = bytes_to_steps(b)
            for step in _done:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.success.value, step]
                handle_step(step, buelon.core.step.StepStatus.success)
    if os.path.exists(os.path.join(dir, 'queued')):
        with open(os.path.join(dir, 'queued'), 'rb') as f:
            b = f.read()
            _queued = bytes_to_steps(b)
            for step in _queued:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.queued.value, step]
                queued[step.id] = step
    if os.path.exists(os.path.join(dir, 'errors')):
        with open(os.path.join(dir, 'errors'), 'rb') as f:
            b = f.read()
            _errors = bytes_to_steps(b)
            for step in _errors:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.error.value, step]
                handle_step(step, buelon.core.step.StepStatus.error)
    if os.path.exists(os.path.join(dir, 'holds')):
        with open(os.path.join(dir, 'holds'), 'rb') as f:
            b = f.read()
            _holds = bytes_to_steps(b)
            upload_steps(_holds)
            for step in _holds:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.queued.value, step]



# endregion

# region run

def run_server():
    global auto_saving

    auto_load()
    auto_save_worker = threading.Thread(target=auto_save_task, daemon=True)
    auto_save_worker.start()
    try:
        server = Server(settings.hub.host, settings.hub.port)  # ('0.0.0.0', 65432)
        server.start()
    finally:
        auto_saving = False
        # auto_save_worker.join()


def run_worker():
    worker()

# endregion


if __name__ == '__main__':
    run_server()


