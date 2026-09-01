import collections
import os
import uuid
import time
import json
import socket
import asyncio
import datetime
import traceback
import threading
import contextlib
from dataclasses import dataclass
from typing import Any

import orjson
import asyncio_pool

from buelon.settings import settings
import buelon


# region structs

# @dataclass
# class Hold:
#     client_id: str
#     jobs: list[buelon.step.Job]

# endregion


# region global variables

# Guards every mutation and every iteration of the hub state below.
#
# `bisocket.Server` spawns one thread per connected client and invokes the request
# handler (`bi_handle_messages`) on that thread. Requests serialize *within* a client
# but run in parallel *across* clients, so every connected worker touches these dicts
# concurrently. Without this lock, two workers calling `hold` at the same time can be
# handed the same job (`get_steps_v2` does a non-atomic read-slice-then-rebind), and
# iteration in the reporting/save paths can raise `dictionary changed size`.
#
# Re-entrant because the state helpers call each other (`handle_step` -> `remove_id`,
# `get_args` -> `handle_step`, ...) and each takes the lock in its own right.
#
# Hold it only for in-memory state work. bz2 (de)compression, json encoding and socket
# I/O must stay outside it -- compressing a full job batch is slow enough to serialize
# every other worker's hold if it happens under the lock.
lock = threading.RLock()
boo_db = buelon.helpers.postgres.get_postgres_from_env()


ALL_STEPS: dict[str, list[str, buelon.step.Job]] = {}
STEPS: dict[str, dict[int, list[buelon.core.step.Job]]] = {}
queued: dict[str, buelon.step.Job] = {}
errors: dict[str, buelon.step.Job] = {}
done: dict[str, buelon.step.Job] = {}

holds: dict[str, list[buelon.step.Job]] = {}
holds_v2: dict[str, dict[str, buelon.step.Job]] = {}

workers: dict[str, dict] = {}

# Client ids whose *send* connection is currently open.
#
# A bisocket client opens two connections that share one client_id: `send` (requests
# in) and `receive` (responses out). bisocket fires `on_open`/`on_close` for the send
# connection only, but fires `on_finally` for BOTH. `OnFinallyInfo` carries no socket
# discriminator, so without this set the hub cannot tell "the worker is gone" from
# "the worker's response channel dropped while it is still running jobs" -- and would
# requeue in-flight jobs out from under a live worker.
#
# Guarded by `lock`.
send_open: set[str] = set()

db: dict[str, Any] = {}  # : dict[str, bytes] = {}

step_count = 10
END_TOKEN = b'[-_-]'
SPLIT_TOKEN = b'|(--)|'
SPLIT_TOKEN2 = b'|{**}|'
LENGTH_OF_END_TOKEN = len(END_TOKEN)

preset_priorities = list(range(100, -1, -1)) # 100 - 0

# Back-off for an idle worker -- BUGS.md #6.
#
# `hold` is a plain request/response, not a long poll: an empty queue answers instantly.
# Without a back-off `see_if_more` re-asks the moment the reply lands, so a worker with
# nothing to do hammers the hub in a tight network loop for the whole of `max_time`
# (20 minutes by default). Each empty hold doubles the wait, up to the ceiling; the
# first hold after any job arrives resets it. The ceiling is also the worst-case delay
# before a newly uploaded job is picked up by an already-idle worker, which is why it is
# seconds rather than minutes.
WORKER_IDLE_BACKOFF_START = 0.1
WORKER_IDLE_BACKOFF_MAX = 2.0

# `BiWorkerClient.get_response` used to poll `self.messages` in a sleep loop, and #6
# had to give that loop a floor: `handle_finished_jobs` asks for `wait_time=0.0`, which
# made the wait an `await asyncio.sleep(0)` loop that spun the event loop flat out for a
# whole round trip and starved the job coroutines sharing its thread. #7 replaced the
# loop with a per-request `asyncio.Event`, so there is no poll interval left to floor --
# the waiter is woken by the reply itself.

# Default bound on a hub round trip -- BUGS.md #7.
#
# Every request used to wait forever. A dead hub or a lost socket hung `bue status`,
# `bue errors` and every `web.py` endpoint for good, and web.py's `try/except ->
# reconnect` blocks could never fire because nothing was ever raised. Exceeding the
# bound now raises `HubTimeout`.
#
# `hold` gets its own, larger bound: it is the one request whose reply carries a whole
# job batch (compressed hub-side), so it is legitimately the slowest.
RESPONSE_TIMEOUT = 60.0
HOLD_RESPONSE_TIMEOUT = 300.0

# `upload` gets its own, larger bound too -- BUGS.md #8. Its reply is a bare `b'ok'`, but
# the hub has to decompress and register up to 500 jobs under `lock` before sending it,
# behind however many other workers are queued ahead of it.
UPLOAD_RESPONSE_TIMEOUT = 300.0

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
    with lock:
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


def add_step_to_steps(step: buelon.core.step.Job, jobs: list[buelon.core.step.Job]):
    jobs.append(step)


def upload_step(job: buelon.core.step.Job):
    with lock:
        if job.scope not in STEPS:
            STEPS[job.scope] = {}

        if job.priority not in STEPS[job.scope]:
            STEPS[job.scope][job.priority] = []

        add_step_to_steps(job, STEPS[job.scope][job.priority])


def upload_steps(jobs: list[buelon.core.step.Job]):
    with lock:
        for job in jobs:
            upload_step(job)


def handle_step(step:  buelon.core.step.Job, status: buelon.core.step.StepStatus):
    with lock:
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
            for step in get_all_steps(step).values():
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
                        # Promote the *child* out of `queued` and into the dispatch
                        # queue. The status write is the child's, not the parent's --
                        # the parent's `success` entry set above must survive. See
                        # BUGS.md #9.
                        child = queued.pop(step_id)
                        ALL_STEPS[step_id] = [status.pending.value, child]
                        upload_step(child)
            else:
                ids = get_all_ids(step)
                if all([i in done for i in ids]):
                    for step_id in ids:
                        remove_id(step_id)

# endregion

# region util

def step_from_id(step_id: str) -> buelon.step.Job | None:
    with lock:
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
        for scope in STEPS.values():
            for steps in scope.values():
                for step in steps:
                    if step.id == step_id:
                        return step


def pop_step_from_id(step_id: str):
    """Hand out a single job by id (the `bue run-job` / web "run job" path).

    "Pop" means: take it out of everywhere it could be dispatched from, and out of the
    terminal buckets it is sitting in -- but *not* out of `ALL_STEPS`. `ALL_STEPS` is the
    id index every DAG walk resolves through (`step_from_id` -> `get_all_ids` /
    `get_all_steps`, which silently truncate on a `None`), so dropping the entry would
    report the running job as `'unknown'` and cut the walk for all of its relatives.
    Normal dispatch (`get_steps_v2`) has the same rule. See BUGS.md #5.
    """
    with lock:
        # Already checked out by a worker -- do not hand it out a second time.
        # `bi_on_hold` registers into `holds_v2` under this same lock immediately after
        # `get_steps_v2` returns, so a repeated single-job hold sees it here and gets
        # nothing back. That is what stops `run-job` re-executing the job in a loop.
        if any(step_id in client_holds for client_holds in holds_v2.values()):
            return None

        s = None

        if step_id in queued:
            s = queued[step_id]
            del queued[step_id]
        elif step_id in errors:
            s = errors[step_id]
            del errors[step_id]
        elif step_id in done:
            s = done[step_id]
            del done[step_id]
        else:
            for hold in holds.values():
                for held in hold:
                    if held.id == step_id:
                        # # removing s would cause errors
                        # hold.remove(s)
                        s = held
                        break
                if s is not None:
                    break

            if s is None and step_id in ALL_STEPS:
                s = ALL_STEPS[step_id][1]

        if s is not None:
            # It may also be sitting in a dispatch queue (a pending job reaches us
            # through the ALL_STEPS fallback above, which does not touch STEPS). Drop it
            # so a normal worker cannot pick up the same job in parallel. The scan is
            # linear in the queue size, which is fine here: the single-job path is an
            # operator action, not the hot path.
            remove_ids_from_steps({step_id})

        return s


def remove_id(step_id: str, skip_all_ids: bool = False):
    with lock:
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


def get_all_ids(step: buelon.core.step.Job, already: set | None = None):
    with lock:
        already = already or set()

        if not step or step.id in already:
            return already

        already.add(step.id)

        for child in step.children:
            get_all_ids(step_from_id(child), already)

        for parent in step.parents:
            get_all_ids(step_from_id(parent), already)

        return already


def get_all_steps(step: buelon.core.step.Job, already: dict | None = None):
    with lock:
        already = already or {}

        if not step or step.id in already:
            return already

        already[step.id] = step

        for child in step.children:
            get_all_steps(step_from_id(child), already)

        for parent in step.parents:
            get_all_steps(step_from_id(parent), already)

        return already


def steps_to_bytes(steps:  list[buelon.core.step.Job]) -> bytes:
    return orjson.dumps([step.to_json() for step in steps])


def bytes_to_steps(data: bytes) -> list[buelon.core.step.Job]:
    return [buelon.core.step.Job().from_json(step) for step in orjson.loads(data)]


def steps_to_compressed_message(steps:  list[buelon.core.step.Job]) -> str:
    import bz2, base64
    b = steps_to_bytes(steps)
    return base64.b64encode(bz2.compress(b)).decode('utf-8')


def compressed_message_to_steps(data: str) -> list[buelon.core.step.Job]:
    import bz2, base64
    b = base64.b64decode(data)
    return bytes_to_steps(bz2.decompress(b))


def all_steps_to_bytes() -> bytes:
    return orjson.dumps([[step[0], step[1].to_json()] for step in ALL_STEPS.values()])


def bytes_to_all_steps(data: bytes) -> None:
    global ALL_STEPS
    # ALL_STEPS = {step[1].id: [step[0], step[1]] for step in orjson.loads(data)}
    for row in orjson.loads(data):
        row[1] = buelon.core.step.Job().from_json(row[1])
        ALL_STEPS[row[1].id] = row

# endregion

# region socket communication

def receive(conn: socket.socket) -> bytes:
    data = b''
    while not data.endswith(END_TOKEN):
        v = conn.recv(1024)
        if not v:
            # If the connection is closed, we'll break out of the loop
            break
        data += v

    if not data.endswith(END_TOKEN):
        # If we broke out of the loop and don't have the end token,
        # it means the connection was closed prematurely.
        try:
            decoded_data = data.decode()
        except UnicodeDecodeError:
            decoded_data = repr(data)
        raise ValueError(f'Invalid value received: `{decoded_data}`')

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

        def commit(steps: list[buelon.core.step.Job], statuses: list[buelon.core.step.StepStatus], results: list[Any]):
            send(s, SPLIT_TOKEN.join([
                steps_to_bytes(steps),
                orjson.dumps([status.value for status in statuses]),
                orjson.dumps(results)
            ]))
            receive(s)

        yield bytes_to_steps(steps), orjson.loads(args), commit
    finally:
        s.close()


def upload_file_to_server(file_path: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    with open(file_path) as f:
        code = f.read()

    return upload_code_to_server(code, return_jobs=return_jobs)


def upload_code_to_server(code: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    return bi_test_upload('code', code, return_jobs)
    return test_upload('code', code, return_jobs)
    chunk = []
    all_jobs = []

    for step in buelon.core.pipe_interpreter.generate_steps_from_code(code):
        chunk.append(step)
        if len(chunk) >= 500:
            upload_steps_to_server(chunk)
            chunk = []

        if return_jobs:
            all_jobs.append(step)

    if chunk:
        upload_steps_to_server(chunk)

    if return_jobs:
        return all_jobs


def upload_steps_to_server(steps: list[buelon.core.step.Job]):
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'upload', steps_to_bytes(steps)]))
        receive(s)


def display_from_server(prefix: str = '', suffix: str = '', return_value: bool = False):
    async def d():
        # async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
        #     r = await client.display()
        #     return (prefix + r + suffix) if return_value else print(r)
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            r = await client.display()
            return (prefix + r + suffix) if return_value else print(r)
    return asyncio.run(d())

    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'display', b'nothing']))
        data = receive(s)
    
    r = prefix + data.decode('utf-8') + suffix
    
    if return_value:
        return r
    
    print(r)


def display_errors_from_server():
    # WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    # WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #     s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    #     s.connect((WORKER_HOST, WORKER_PORT))
    #     send(s, SPLIT_TOKEN.join([b'errors', b'nothing']))
    #     data = receive(s)
    # # print(data.decode('utf-8'))
    # steps, _errors = data.split(SPLIT_TOKEN)
    # steps = bytes_to_steps(steps)
    # _errors = orjson.loads(_errors)

    async def cor():
        # async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
        #     steps, errors = await client.errors()
        #     return compressed_message_to_steps(steps), errors
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            steps, errors = await client.errors()
            return compressed_message_to_steps(steps), errors

    steps, _errors = asyncio.run(cor())

    #

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
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            await client.reset_errors()
        # async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
        #     await client.reset_errors()

    return asyncio.run(cor())
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'reset-errors', b'nothing']))
        receive(s)


def cancel_errors_from_server():
    """Cancel every pipeline containing an error. Returns {'jobs', 'pipelines'} or None."""
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.cancel_errors()

    return asyncio.run(cor())


def delete_all_from_server():
    """Wipe ALL job state on the hub. Returns {'jobs'} or None."""
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.delete_all()

    return asyncio.run(cor())
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'cancel-errors', b'nothing']))
        receive(s)


def get_all_info_from_server():
    async def cor():
        # async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
        #     return await client.get_all_info()
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_all_info()

    return asyncio.run(cor())
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'get-all-info', b'nothing']))
        data = receive(s)

    the_steps, all_done, all_queued, all_errors, all_db = data.split(SPLIT_TOKEN)
    return [the_steps, all_done, all_errors, all_queued, all_db]


def _job_status(job_id: str):
    step_id = job_id
    with lock:
        if step_id not in ALL_STEPS:
            return 'unknown'
        status = ALL_STEPS[step_id][0]

    if not isinstance(status, str):
        if isinstance(status, int):
            status = buelon.core.step.StepStatus(status).name
        elif isinstance(status, buelon.core.step.StepStatus):
            status = status.name
        else:
            status = f'{status}'
            _map = {k: v.value for k, v in dict(buelon.core.step.StepStatus.__members__).items()}
            if status in _map:
                status = 'unknown'
    return status


def check_job_status(job_id: str) -> str:
    async def cor():
        async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_job_status(job_id)

    return asyncio.run(cor())
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'job-status', job_id.encode()]))
        data = receive(s)
    return data.decode('utf-8')


def check_job_status_bulk(job_ids: list[str]) -> dict[str, str]:
    async def cor():
        # async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
        #     return await client.get_job_status_bulk(job_ids)
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_job_status_bulk(job_ids)

    return asyncio.run(cor())
    WORKER_HOST = settings.worker.host  # = os.environ.get('PIPE_WORKER_HOST', 'localhost')
    WORKER_PORT = settings.worker.port  # = int(os.environ.get('PIPE_WORKER_PORT', 65432))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        send(s, SPLIT_TOKEN.join([b'job-status-bulk', orjson.dumps(job_ids)]))
        data = orjson.loads(receive(s))
        r = dict(zip(job_ids, data))
    return r


def save_from_server():
    async def cor():
        async with WorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.save()

    return asyncio.run(cor())
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
    with lock:
        steps_len = sum([len(lst) for val in STEPS.values() for lst in val.values()])
        # `holds` is the dead websocket path's dict; the live bi path checks jobs out into
        # `holds_v2[client_id][job_id]` (BUGS.md #10).
        holds_len = sum([len(client_holds) for client_holds in holds_v2.values()])

        done_len, queue_len, error_len = len(done), len(queued), len(errors)

    total = steps_len + holds_len + done_len + queue_len + error_len
    remaining = total - done_len

    text = (f'done: {done_len:,}'
            f', queued: {queue_len:,}'
            f', errors: {error_len:,}'
            f', jobs: {steps_len:,}'
            f', holds: {holds_len:,}'
            f', remaining: {remaining:,}'
            f', total: {total:,}')

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


def temp_get_all_ids(step:  buelon.core.step.Job, already: set | None = None, has_none: dict | None = None):
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


def temp_handle_step_args(step: buelon.core.step.Job):
    with lock:
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
    with lock:
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
        elif method == b'job-status':
            status = _job_status(data.decode('utf-8'))
            send(s, status.encode('utf-8'))
        elif method == b'job-status-bulk':
            job_id_ids = orjson.loads(data)  # .decode('utf-8').split(',')
            statuses = [_job_status(job_id) for job_id in job_id_ids]
            r = orjson.dumps(statuses)
            send(s, r)
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
        step: buelon.core.step.Job
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
        steps: list[buelon.core.step.Job]
        statuses: list[buelon.core.step.StepStatus] = []
        results: list[Any] = []
        # for step, arg in zip(steps, args):
        #     step: buelon.core.step.Job
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

    # Snapshot under the lock, serialize and write outside it.
    with lock:
        snapshot = {
            'all_steps': [[status, step] for status, step in ALL_STEPS.values()],
            'steps': [step for scope in STEPS.values() for steps in scope.values() for step in steps],
            'done': list(done.values()),
            'queued': list(queued.values()),
            'errors': list(errors.values()),
            'holds': [step for steps in holds.values() for step in steps],
            'db': dict(db),
        }

    with open(os.path.join(dir, 'all_steps'), 'wb') as f:
        f.write(orjson.dumps([[status, step.to_json()] for status, step in snapshot['all_steps']]))
    for name in ('steps', 'done', 'queued', 'errors', 'holds'):
        with open(os.path.join(dir, name), 'wb') as f:
            f.write(steps_to_bytes(snapshot[name]))
    with open(os.path.join(dir, 'db'), 'wb') as f:
        f.write(orjson.dumps(snapshot['db']))


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
    return asyncio.run(test_server())

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


def run_worker(stop_on_no_jobs: bool = False):
    return asyncio.run(bi_test_worker(stop_on_no_jobs=stop_on_no_jobs))
    return asyncio.run(test_worker())
    worker()

# endregion

# region test
from websockets.asyncio.server import serve, Connection
# from websockets.sync.client import connect, ClientConnection
from websockets.asyncio.client import connect, ClientConnection


def compress_method(method: str, data: any) -> str:
    import bz2, base64
    compressed = bz2.compress(orjson.dumps([method, data]), compresslevel=9)
    return base64.b64encode(compressed).decode('utf-8')


def decompress_method(compressed: str) -> tuple[str, any]:
    import bz2, base64
    decoded = base64.b64decode(compressed)
    decompressed = bz2.decompress(decoded)
    return orjson.loads(decompressed)


async def on_hold(websocket: Connection, data, websocket_holds: list[str]):
    uid = f'{uuid.uuid1()}'
    steps = get_steps_v2(**data)  # json.loads(data))  # (**orjson.loads(data))

    if steps:
        holds[uid] = steps
        websocket_holds.append(uid)

    try:
        steps, args = get_args(steps)
        await websocket.send(json.dumps([
            uid,
            steps_to_compressed_message(steps),
            args
        ]))
    except:
        upload_steps(steps)


async def on_release(websocket: Connection, data, websocket_holds: list[str]):
    uid, steps, statuses, results = data

    finished = (len(steps) == len(holds[uid]))  # len(holds.get(uid, [])))

    if finished and uid in holds:
        del holds[uid]

    # if finished and uid in websocket_holds:
    #     websocket_holds.remove(uid)

    steps = compressed_message_to_steps(steps)
    statuses = [buelon.core.step.StepStatus(status) for status in statuses]
    # results = results

    for step, status, result in zip(steps, statuses, results):
        db[step.id] = result
        handle_step(step, status)

    if uid in holds:
        these_step_ids = [step.id for step in steps]
        to_remove = []

        for s in holds[uid]:
            if s.id in these_step_ids:
                to_remove.append(s)

        for s in to_remove:
            holds[uid].remove(s)

        if not holds[uid]:
            del holds[uid]

    await websocket.send('ok')


async def on_upload(websocket: Connection, data):
    steps = compressed_message_to_steps(data)

    print(f'uploading {len(steps):,} jobs')

    for step in steps:
        if step.parents:
            ALL_STEPS[step.id] = [buelon.core.step.StepStatus.queued.value, step]
            queued[step.id] = step
        else:
            ALL_STEPS[step.id] = [buelon.core.step.StepStatus.pending.value, step]
            upload_step(step)

    await websocket.send('ok')


async def on_errors(websocket: Connection, data):
    res = []
    for step_id in errors:
        if isinstance(db.get(step_id), dict) and 'error' in db[step_id] and 'trace' in db[step_id]:
            res.append(db[step_id])
        else:
            res.append({'error': 'Unknown error', 'trace': ''})

    await websocket.send(json.dumps([
        steps_to_compressed_message(list(errors.values())),
        res
    ]))


class WorkerClient:
    def __init__(self, *args, **kwargs):  # (self, host: str, port: int, scopes: list[str]):
        self.host = settings.worker.host  # host
        self.port = settings.worker.port  # port
        self.scopes = settings.worker.scopes.split(',') + ['test']  # scopes
        self.websocket: ClientConnection = None

    async def __aenter__(self):
        self.websocket = await connect(
            f'ws://{self.host}:{self.port}',
            ping_interval=60 * 5,  # Send ping every 30 seconds (default: 20)
            ping_timeout=60 * 5,  # Wait 20 seconds for pong (default: 20)
            close_timeout=60 * 5  # Wait 10 seconds for close (default: 10)
        ).__aenter__()
        await self.update_worker_info()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.websocket.__aexit__(exc_type, exc_val, exc_tb)

    async def hold(self, limit: int = 100, reverse: bool = False, single_job: str | None = None) -> list[str, list[buelon.step.Job], list[any]]:
        data = {'scopes': self.scopes, 'limit': limit, 'reverse': settings.worker.reverse, 'single_step': single_job}
        await self.websocket.send(compress_method('hold', data))

        uid, jobs, args = json.loads(await self.websocket.recv())

        return [uid, compressed_message_to_steps(jobs), args]

    async def release(self, uid: str, jobs: list[buelon.step.Job], statuses: list[buelon.step.StepStatus], results: list[any]):
        data = [uid, steps_to_compressed_message(jobs), [status.value for status in statuses], results]
        await self.websocket.send(compress_method('release', data))
        await self.websocket.recv()

    async def update_worker_info(self):
        await self.websocket.send(compress_method('worker-info', settings.worker.info))

    async def get_web_info(self, workers_info: bool = False):
        await self.websocket.send(compress_method('web-info', workers_info))
        data = json.loads(await self.websocket.recv())

        for worker_id, worker in data['workers'].items():
            if 'jobs' not in worker:
                worker['jobs'] = []

        return data

    async def get_job_parents_and_results(self, job_id: str):
        await self.websocket.send(compress_method('job-parents-and-results', job_id))
        return json.loads(await self.websocket.recv())

    async def upload(self, jobs: list[buelon.step.Job]):
        await self.websocket.send(compress_method('upload', steps_to_compressed_message(jobs)))
        await self.websocket.recv()

    async def display(self) -> str:
        await self.websocket.send(compress_method('display', ''))
        return await self.websocket.recv()

    async def get_job_status(self, job_id: str) -> str:
        await self.websocket.send(compress_method('job-status', job_id))
        return await self.websocket.recv()

    async def get_job_status_bulk(self, job_ids: list[str]) -> list[str]:
        await self.websocket.send(compress_method('job-status-bulk', job_ids))
        return json.loads(await self.websocket.recv())

    async def errors(self):
        await self.websocket.send(compress_method('errors', ''))
        return json.loads(await self.websocket.recv())

    async def reset_errors(self):
        await self.websocket.send(compress_method('reset-errors', ''))
        await self.websocket.recv()

    async def cancel_errors(self):
        await self.websocket.send(compress_method('cancel-errors', ''))
        await self.websocket.recv()

    async def get_all_info(self):
        await self.websocket.send(compress_method('get-all-info', ''))

        _steps, _done, _queued, _errors, _db = json.loads(await self.websocket.recv())
        _steps, _done, _queued, _errors = [compressed_message_to_steps(lst) for lst in (_steps, _done, _queued, _errors)]

        return _steps, _done, _queued, _errors, _db

    async def save(self):
        await self.websocket.send(compress_method('save', ''))
        await self.websocket.recv()


def get_web_info(workers_info: bool = False):
    info = {}

    steps_len = sum([len(lst) for val in STEPS.values() for lst in val.values()])
    holds_len = sum([len(lst) for lst in holds.values()])

    done_len, queue_len, error_len = len(done), len(queued), len(errors)
    total = steps_len + holds_len + done_len + queue_len + error_len
    remaining = total - done_len

    # text = (f'done: {done_len:,}'
    #         f', queued: {queue_len:,}'
    #         f', errors: {error_len:,}'
    #         f', jobs: {steps_len:,}'
    #         f', holds: {holds_len:,}'
    #         f', remaining: {remaining:,}'
    #         f', total: {total:,}')
    #
    # info['text'] = text
    info['counts'] = {
        'done': done_len, 'queued': queue_len, 'errors': error_len, 'jobs': steps_len, 'holds': holds_len,
        'remaining': remaining, 'total': total
    }

    if workers_info:
        info['workers'] = get_all_worker_info()

    return info


def get_all_worker_info(bi: bool = True):
    _workers = json.loads(json.dumps(workers))
    _workers = json.loads(json.dumps(workers))

    for client_id, worker_info in _workers.items():
        if worker_info.get('holds'):
            if not bi:
                _holds: list[buelon.core.step.Job] = [s for uid in worker_info['holds'] for s in holds.get(uid, [])]
            else:
                _holds: list[buelon.core.step.Job] = [s for uid in worker_info['holds'] for s in holds.get(uid, [])]
            _holds[:] = [s.to_json() for s in _holds]
            _holds: list[dict]
            worker_info['jobs'] = _holds
            worker_info['holds'] = len(worker_info['holds'])

    try:
        return _workers
    except:
        return {}


def get_job_parents_and_results(job_id: str, already: set | None = None):
    with lock:
        already = already or set()  # <-- prevent infinite dependencies, should never happend though

        if job_id in already:
            return None

        already.add(job_id)
        job: buelon.core.step.Job = step_from_id(job_id)

        if job:
            return {
                'job': job.to_json(),
                'result': db.get(job_id),
                'parents': {
                    parent_id: get_job_parents_and_results(parent_id, already=already)
                    for parent_id in job.parents
                }
            }


async def handle_messages(websocket: Connection):
    global errors, holds
    client_id = f'{id(websocket)}'

    websocket_holds = []
    worker_info = {'holds': websocket_holds}
    workers[client_id] = worker_info
    # client_id = f'{id(websocket)}'
    # holds[client_id] = {}

    try:
        async for message in websocket:
            method, data = decompress_method(message)
            data: str
            if method == 'hold':
                await on_hold(websocket, data, websocket_holds)
            elif method == 'release':
                await on_release(websocket, data, websocket_holds)
            elif method == 'worker-info':
                if isinstance(data, dict):
                    worker_info.update(data)
            elif method == 'web-info':
                info = get_web_info(bool(data))
                await websocket.send(json.dumps(info))
            elif method == 'job-parents-and-results':
                res = get_job_parents_and_results(data)
                await websocket.send(json.dumps(res))
            elif method == 'upload':
                await on_upload(websocket, data)
            elif method == 'display':
                text = display_text()
                await websocket.send(text)
            elif method == 'job-status':
                status = _job_status(data)
                await websocket.send(status)
            elif method == 'job-status-bulk':
                statuses = {job_id: _job_status(job_id) for job_id in data}
                await websocket.send(json.dumps(statuses))
            elif method == 'errors':
                await on_errors(websocket, data)
            elif method == 'reset-errors':
                _steps = list(errors.values())
                errors = {}
                upload_steps(_steps)
                await websocket.send('ok')
            elif method == 'cancel-errors':
                # for step in list(errors.values()):
                #     for step_id in get_all_ids(step):
                #         remove_id(step_id)
                # # # Remove all steps
                for _, lst in ALL_STEPS.items():
                    __, s = lst
                    remove_id(s.id)
                # # for sid in ALL_STEPS:
                # #     remove_id(sid)
                # send(s, b'ok')
                await websocket.send('ok')
            elif method == 'get-all-info':
                await websocket.send(json.dumps([
                    steps_to_compressed_message([step for scope in STEPS.values() for steps in scope.values() for step in steps]),
                    steps_to_compressed_message(list(done.values())),
                    steps_to_compressed_message(list(queued.values())),
                    steps_to_compressed_message(list(errors.values())),
                    db
                ]))
            elif method == 'save':
                auto_save()
                await websocket.send('ok')

            for uid in holds:
                if not holds[uid]:
                    del holds[uid]

            for uid in websocket_holds:
                if uid not in holds:
                    websocket_holds.remove(uid)
    finally:
        for uid in websocket_holds:
            if uid in holds:
                if holds[uid]:
                    upload_steps(holds[uid])
                del holds[uid]

        del workers[client_id]


async def test_server():
    async with serve(
        handle_messages,
        settings.hub.host,
        settings.hub.port,
        ping_interval=60 * 5,  # Send ping every 30 seconds (default: 20)
        ping_timeout=60 * 5,  # Wait 20 seconds for pong (default: 20)
        close_timeout=60 * 5  # Wait 10 seconds for close (default: 10)
    ) as server:
        await server.serve_forever()


class WorkerJob:
    def __init__(self, mut, hold_id: str, step: buelon.core.step.Job, arg):
        self.mut = mut
        self.hold_id = hold_id
        self.step = step
        self.arg = arg

        self.status = None
        self.result = None
        self.finished = False
        self.thread = None
        self.task = None

        self.start = None

    async def arun(self):
        async def __run():
            try:
                await self._arun()
            finally:
                self.finished = True

        self.task = asyncio.create_task(__run())
        self.start = time.time()

    def run(self):
        def __run():
            try:
                self._run()
            finally:
                self.finished = True

        self.thread = threading.Thread(target=__run, daemon=True)
        self.thread.start()
        self.start = time.time()

    def _run(self):
        print('handling', self.step.name)
        try:
            r: buelon.core.step.Result = self.step.run(*self.arg, mut=self.mut)
            self.status, self.result = r.status, r.data
        except Exception as e:
            print(e)
            traceback.print_exc()
            self.status, self.result = buelon.core.step.StepStatus.error, {'error': str(e), 'trace': traceback.format_exc(), 'worker_name': f'{settings.worker.info.get("name", "Unknown")}'}
        self.start = None

    async def _arun(self):
        print('handling', self.step.name)
        try:
            r: buelon.core.step.Result = await self.step.arun(*self.arg, mut=self.mut)
            self.status, self.result = r.status, r.data
        except Exception as e:
            print(e)
            traceback.print_exc()
            self.status, self.result = buelon.core.step.StepStatus.error, {'error': str(e), 'trace': traceback.format_exc()}
        self.start = None

    @property
    def runtime(self):
        start = self.start
        if isinstance(start, (int, float)):
            return time.time() - start
        return 0

    @property
    def done(self):
        if self.finished and self.thread:
            self.thread.join()
            self.thread = None

        return self.finished

    async def adone(self):
        if self.finished and self.task:
            await self.task
            self.task = None

        return self.finished


class WorkerJobQueue:
    def __init__(self):
        self.jobs: list[WorkerJob] = []

    def finished_jobs(self):
        finished_jobs = []
        result = collections.defaultdict(list)

        for job in self.jobs:
            if job.done:
                finished_jobs.append(job)

        for job in finished_jobs:
            self.jobs.remove(job)
            result[job.hold_id].append(job)

        return result

    async def afinished_jobs(self):
        finished_jobs = []
        result = collections.defaultdict(list)

        for job in self.jobs:
            if await job.adone():
                finished_jobs.append(job)

        for job in finished_jobs:
            self.jobs.remove(job)
            result[job.hold_id].append(job)

        return result

    def put(self, job: WorkerJob):
        self.jobs.append(job)
        job.run()

    async def aput(self, job: WorkerJob):
        self.jobs.append(job)
        await job.arun()

    def qsize(self):
        return len(self.jobs)

    def max_runtime(self):
        return max([job.runtime for job in self.jobs]) if self.jobs else 0


async def test_worker(jobs_at_a_time: int = 25, single_step: str | None = None, iterations: int = 10_000):
    mut = {}
    job_queue = WorkerJobQueue()
    time_since_last_hold = 0
    time_to_send_anyway = 5
    waited = 0
    last_hold = 0
    max_time_to_handle_more = 60 * 10

    if single_step:
        iterations = 2

    async def hold_more():
        nonlocal time_since_last_hold, last_hold
        needed = jobs_at_a_time - job_queue.qsize()

        if needed == jobs_at_a_time or (needed > 0 and (time_to_send_anyway < (time.time() - time_since_last_hold))):
            limit = min(needed, jobs_at_a_time)  # int(jobs_at_a_time / 2))
            uid, jobs, args = await client.hold(limit=limit, reverse=settings.worker.reverse, single_job=single_step)
            uid: str
            jobs: list[buelon.step.Job]
            args: list[any]

            print(f'pulled {len(jobs):,} jobs')

            for job, arg in zip(jobs, args):
                # await job_queue.aput(WorkerJob(mut, uid, job, arg))
                job_queue.put(WorkerJob(mut, uid, job, arg))

            time_since_last_hold = time.time()
            last_hold = len(jobs)
        else:
            last_hold = 0

    async def handle_finished_jobs():
        # finished_jobs = await job_queue.afinished_jobs()
        finished_jobs = job_queue.finished_jobs()

        for uid, jobs in finished_jobs.items():
            steps = [job.step for job in jobs]
            statuses = [job.status for job in jobs]
            results = [job.result for job in jobs]

            print(f'finished {len(jobs):,} jobs')

            await client.release(uid, steps, statuses, results)

    async with WorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        i = 0
        while ((i := i + 1) < (iterations + 1)) or job_queue.qsize():
            if i < iterations or max_time_to_handle_more < job_queue.max_runtime():
                await hold_more()
            await handle_finished_jobs()

            if not job_queue.qsize() or not last_hold:
                # if waited:
                #     buelon.hub_v1.delete_last_line()
                print(f'waiting({i:02d})' + ('.' * waited))
                await asyncio.sleep(1.0 if not job_queue.qsize() else 0.05)
                waited = ((waited + 1) % 4) + 1
            else:
                waited = 0


def test_upload(upload_type: str, code_file: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    if upload_type == 'file':
        with open(code_file) as f:
            code = f.read()
    else:
        code = code_file

    return asyncio.run(_test_upload(code, return_jobs))


async def _test_upload(code: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    chunk = []
    jobs = []

    async with WorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        for step in buelon.core.pipe_interpreter.generate_steps_from_code(code):
            chunk.append(step)
            if len(chunk) >= 500:
                await client.upload(chunk)
                chunk.clear()

            if return_jobs:
                jobs.append(step)

        if chunk:
            await client.upload(chunk)

    if return_jobs:
        return jobs



# endregion

# region bi_test

from bisocket.main import Server as BiServer, Client as BiClient, BiMessage, ServerRequest, OnCloseInfo, OnOpenInfo, OnFinallyInfo


class HubTimeout(TimeoutError):
    """The hub did not answer a request within the allowed time -- BUGS.md #7.

    Subclasses `TimeoutError` so a caller that already handles connection trouble
    (`web.py`'s `try/except -> reconnect`) treats a silent hub the same way.
    """

    def __init__(self, request_id: str, timeout: float | int):
        self.request_id = request_id
        self.timeout = timeout
        super().__init__(f'no response from the hub for request {request_id} within {timeout}s')


class UploadRejected(RuntimeError):
    """The hub did not accept an uploaded chunk of jobs -- BUGS.md #8.

    Raised instead of returning normally, because the alternative is `bue upload`
    reporting success for a pipeline the hub never stored.
    """


# Sentinel for "caller did not choose a timeout", so `timeout=None` can keep its own
# meaning of "wait forever".
_DEFAULT_TIMEOUT = object()


class BiWorkerClient:
    def __init__(self, *args, **kwargs):  # (self, host: str, port: int, scopes: list[str]):
        self.host = settings.worker.host  # host
        self.port = settings.worker.port  # port
        self.scopes = settings.worker.scopes.split(',') + ['test']  # scopes
        self.client: BiClient = None
        self.messages: dict[str, BiMessage] = {}
        # request_id -> Event, set when that request's reply lands. Replaces the old
        # `while request_id not in self.messages: await asyncio.sleep(...)` poll, which
        # added up to `wait_time` of latency to every round trip and burned CPU while
        # waiting -- BUGS.md #7.
        self._waiters: dict[str, asyncio.Event] = {}
        # request_ids whose caller has given up. A reply that turns up afterwards is
        # dropped instead of sitting in `self.messages` for the life of the client.
        self._abandoned: set[str] = set()
        # Strong references to the fire-and-forget response readers started by
        # `release`/`upload`/`reset_errors`/`save`, so CPython cannot collect one
        # mid-flight -- BUGS.md #7.
        self._background: set[asyncio.Task] = set()
        # The loop `__aenter__` ran on. `on_receive` is handed to bisocket as a plain
        # function, and bisocket calls a non-coroutine callback via `asyncio.to_thread`,
        # i.e. off the loop -- so waking a waiter has to go through the loop.
        self._loop: asyncio.AbstractEventLoop | None = None

    async def __aenter__(self):
        # self.websocket = await connect(
        #     f'ws://{self.host}:{self.port}',
        #     ping_interval=60 * 5,  # Send ping every 30 seconds (default: 20)
        #     ping_timeout=60 * 5,  # Wait 20 seconds for pong (default: 20)
        #     close_timeout=60 * 5  # Wait 10 seconds for close (default: 10)
        # ).__aenter__()
        self._loop = asyncio.get_running_loop()
        # web.py reconnects by re-entering an existing client. Request ids from the old
        # connection can never be answered on the new one, so stop tracking them.
        self._abandoned.clear()
        self.client = await BiClient(self.host, self.port, self.on_receive).__aenter__()
        await self.update_worker_info()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in list(self._background):
            task.cancel()
        self._background.clear()
        await self.client.__aexit__(exc_type, exc_val, exc_tb)

    def on_receive(self, msg: BiMessage):
        if msg.request_id in self._abandoned:
            # Nobody is waiting for this any more -- see `get_response`.
            self._abandoned.discard(msg.request_id)
            return

        self.messages[msg.request_id] = msg
        self._wake(msg.request_id)

    def _wake(self, request_id: str) -> None:
        """Release whoever is waiting on `request_id`, from whichever thread we are on."""
        event = self._waiters.get(request_id)

        if event is None:
            return

        loop = self._loop

        if loop is None:
            event.set()
            return

        try:
            # Safe from the loop thread as well as from bisocket's worker thread.
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            pass  # loop already closed; the waiter is gone with it

    async def get_response(self, request_id: str, wait_time: float | int = 0.1,
                           timeout: float | int | None = _DEFAULT_TIMEOUT) -> BiMessage | None:
        """Wait for the hub's reply to `request_id`.

        Raises `HubTimeout` if it does not arrive within `timeout`. `timeout=None` still
        means "wait forever", but it now has to be asked for explicitly -- BUGS.md #7.

        `wait_time` is accepted and ignored: the wait is event-driven now, so there is no
        poll interval. It is kept because `hold` forwards a caller-supplied value.
        """
        if timeout is _DEFAULT_TIMEOUT:
            timeout = RESPONSE_TIMEOUT

        msg = self.messages.pop(request_id, None)

        if msg is not None:
            return msg

        event = self._waiters.setdefault(request_id, asyncio.Event())

        try:
            # The reply can land between the pop above and the register on the line
            # before this one; `on_receive` would then have found no waiter to wake.
            if request_id not in self.messages:
                if timeout is None:
                    await event.wait()
                else:
                    try:
                        await asyncio.wait_for(event.wait(), timeout)
                    except (asyncio.TimeoutError, TimeoutError):
                        if request_id not in self.messages:
                            # A late reply will have nobody left to claim it.
                            self._abandoned.add(request_id)
                            raise HubTimeout(request_id, timeout) from None
                        # The reply landed in the same breath as we gave up. Take it.
        finally:
            self._waiters.pop(request_id, None)

        return self.messages.pop(request_id, None)

    def _read_ack_in_background(self, request_id: str) -> asyncio.Task:
        """Consume a reply nobody is waiting for, without leaking it or the task.

        The task reference is held until it finishes -- an `asyncio.create_task` result
        that is dropped on the floor can be garbage collected mid-flight, which is how
        `self.messages` used to accumulate unclaimed replies.
        """
        async def read():
            try:
                await self.get_response(request_id)
            except HubTimeout:
                pass  # nothing to report it to; `get_response` has dropped the entry

        task = asyncio.ensure_future(read())
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def hold(self, limit: int = 100, reverse: bool = False, single_job: str | None = None, wait_time: float | int = 0.1, timeout: float | int | None = HOLD_RESPONSE_TIMEOUT) -> list[str, list[buelon.step.Job], list[any]]:
        data = {'scopes': self.scopes, 'limit': limit, 'reverse': settings.worker.reverse, 'single_step': single_job}
        request_id = await self.client.asend_obj('hold', data)

        msg = await self.get_response(request_id, wait_time=wait_time, timeout=timeout)

        uid, jobs, args = msg.get_obj()  # json.loads(await self.websocket.recv())

        return [uid, compressed_message_to_steps(jobs), args]

    async def release(self, uid: str, jobs: list[buelon.step.Job], statuses: list[buelon.step.StepStatus], results: list[any]):
        data = [uid, steps_to_compressed_message(jobs), [status.value for status in statuses], results]
        request_id = await self.client.asend_obj('release', data)
        # msg = await self.get_response(request_id)
        # await self.websocket.recv()
        self._read_ack_in_background(request_id)

    async def update_worker_info(self):
        await self.client.asend_obj('worker-info', settings.worker.info)

    async def get_web_info(self, workers_info: bool = False):
        # await self.websocket.send(compress_method('web-info', workers_info))
        request_id = await self.client.asend_obj('web-info', workers_info)
        # data = json.loads(await self.websocket.recv())
        data = (await self.get_response(request_id)).get_obj()

        for worker_id, worker in data['workers'].items():
            if 'jobs' not in worker:
                worker['jobs'] = []

        return data

    async def get_job_parents_and_results(self, job_id: str):
        # await self.websocket.send(compress_method('job-parents-and-results', job_id))
        request_id = await self.client.asend_obj('job-parents-and-results', job_id)
        # return json.loads(await self.websocket.recv())
        return (await self.get_response(request_id)).get_obj()

    async def upload(self, jobs: list[buelon.step.Job],
                     timeout: float | int | None = UPLOAD_RESPONSE_TIMEOUT):
        """Send a chunk of jobs and wait for the hub to confirm it stored them.

        The ack used to be handed to `_read_ack_in_background`, which `__aexit__`
        cancels -- so `bue upload` returned before the hub had processed anything, and a
        hub-side failure on any chunk was invisible. Waiting here also gives the chunk
        loop in `_bi_test_upload` its only backpressure. BUGS.md #8.

        Raises `HubTimeout` if the hub never answers, `UploadRejected` if it answers
        that the upload failed.
        """
        # await self.websocket.send(compress_method('upload', steps_to_compressed_message(jobs)))
        request_id = await self.client.asend_obj('upload', steps_to_compressed_message(jobs))
        # await self.websocket.recv()
        msg = await self.get_response(request_id, timeout=timeout)

        if msg is None:
            raise UploadRejected(
                f'no confirmation from the hub for an upload of {len(jobs):,} job(s)')

        if msg.is_error:
            err = msg.error
            raise UploadRejected(
                f'the hub failed to store {len(jobs):,} job(s): '
                f'{err.type}: {err.message}')

    async def display(self) -> str:
        # await self.websocket.send(compress_method('display', ''))
        request_id = await self.client.asend_obj('display', '')
        # return await self.websocket.recv()
        return (await self.get_response(request_id)).get_str()

    async def get_job_status(self, job_id: str) -> str:
        # await self.websocket.send(compress_method('job-status', job_id))
        request_id = await self.client.asend_obj('job-status', job_id)
        # return await self.websocket.recv()
        return (await self.get_response(request_id)).get_str()

    async def get_job_status_bulk(self, job_ids: list[str]) -> list[str]:
        # await self.websocket.send(compress_method('job-status-bulk', job_ids))
        request_id = await self.client.asend_obj('job-status-bulk', job_ids)
        # return json.loads(await self.websocket.recv())
        return (await self.get_response(request_id)).get_obj()

    async def errors(self):
        # await self.websocket.send(compress_method('errors', ''))
        request_id = await self.client.asend_obj('errors', '')
        # return json.loads(await self.websocket.recv())
        return (await self.get_response(request_id)).get_obj()

    async def reset_errors(self):
        # await self.websocket.send(compress_method('reset-errors', ''))
        request_id = await self.client.asend_obj('reset-errors', '')
        # await self.websocket.recv()
        self._read_ack_in_background(request_id)

    async def cancel_errors(self, timeout: float | int = 120):
        """Cancel every pipeline containing an error. Returns {'jobs', 'pipelines'}."""
        request_id = await self.client.asend_obj('cancel-errors', '')
        # `bue delete` reports "no response from the hub" rather than raising -- keeping
        # that means swallowing the timeout here.
        try:
            msg = await self.get_response(request_id, timeout=timeout)
        except HubTimeout:
            return None
        return msg.get_obj() if msg is not None else None

    async def delete_all(self, timeout: float | int = 120):
        """Wipe ALL job state on the hub. Returns {'jobs'}."""
        request_id = await self.client.asend_obj('delete-all', '')
        try:
            msg = await self.get_response(request_id, timeout=timeout)
        except HubTimeout:
            return None
        return msg.get_obj() if msg is not None else None

    async def get_all_info(self):
        # await self.websocket.send(compress_method('get-all-info', ''))
        request_id = await self.client.asend_obj('get-all-info', '')

        # _steps, _done, _queued, _errors, _db = json.loads(await self.websocket.recv())
        _steps, _done, _queued, _errors, _db = (await self.get_response(request_id)).get_obj()
        _steps, _done, _queued, _errors = [compressed_message_to_steps(lst) for lst in (_steps, _done, _queued, _errors)]

        return _steps, _done, _queued, _errors, _db

    async def save(self):
        # await self.websocket.send(compress_method('save', ''))
        request_id = await self.client.asend_obj('save', '')
        # await self.websocket.recv()
        self._read_ack_in_background(request_id)


def bi_on_hold(request: ServerRequest, data):
    uid = f'{uuid.uuid1()}'

    def release_back(_jobs):
        """Return jobs to the queue and drop them from this client's holds."""
        with lock:
            upload_steps(_jobs)
            client_holds = holds_v2.get(request.client_id)
            if client_holds is not None:
                for job in _jobs:
                    client_holds.pop(job.id, None)

    with lock:
        jobs = get_steps_v2(**data)

        try:
            # Register in holds_v2 only AFTER get_args has filtered the batch. get_args
            # drops jobs whose parent results are missing from `db` and resets them
            # (which re-queues them into STEPS); registering first left those dropped
            # jobs in holds_v2 forever, and every disconnect requeued them again.
            jobs, args = get_args(jobs)
        except:
            traceback.print_exc()
            # Nothing is registered yet, so this only puts the popped jobs back on the
            # queue. release_back's holds_v2 pop is a no-op here.
            release_back(jobs)
            return

        if jobs:
            client_holds = holds_v2.setdefault(request.client_id, {})
            for job in jobs:
                client_holds[job.id] = job

    try:
        request.send_data(json.dumps([
            uid,
            steps_to_compressed_message(jobs),
            args
        ]).encode())
    except:
        traceback.print_exc()
        release_back(jobs)


def bi_on_release(request: ServerRequest, data):
    uid, steps, statuses, results = data

    steps = compressed_message_to_steps(steps)
    statuses = [buelon.core.step.StepStatus(status) for status in statuses]

    with lock:
        for step, status, result in zip(steps, statuses, results):
            db[step.id] = result
            handle_step(step, status)

        client_holds = holds_v2.get(request.client_id)
        if client_holds is not None:
            for job in steps:
                client_holds.pop(job.id, None)


def bi_on_upload(request: ServerRequest, data):
    steps = compressed_message_to_steps(data)

    print(f'uploading {len(steps):,} jobs')

    with lock:
        for step in steps:
            if step.parents:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.queued.value, step]
                queued[step.id] = step
            else:
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.pending.value, step]
                upload_step(step)

    request.send_data(b'ok')


def bi_on_errors(request: ServerRequest, data):
    res = []

    with lock:
        for step_id in errors:
            if isinstance(db.get(step_id), dict) and 'error' in db[step_id] and 'trace' in db[step_id]:
                res.append(db[step_id])
            else:
                res.append({'error': 'Unknown error', 'trace': ''})

        errored_steps = list(errors.values())

    request.send_data(json.dumps([
        steps_to_compressed_message(errored_steps),
        res
    ]).encode())


def bi_get_web_info(request: ServerRequest, workers_info: bool = False):
    info = {}

    with lock:
        steps_len = sum([len(lst) for val in STEPS.values() for lst in val.values()])
        # `holds` is the dead websocket path's dict; the live bi path checks jobs out into
        # `holds_v2[client_id][job_id]` (BUGS.md #10).
        holds_len = sum([len(client_holds) for client_holds in holds_v2.values()])

        done_len, queue_len, error_len = len(done), len(queued), len(errors)

        total = steps_len + holds_len + done_len + queue_len + error_len
        remaining = total - done_len

        info['counts'] = {
            'done': done_len, 'queued': queue_len, 'errors': error_len, 'jobs': steps_len, 'holds': holds_len,
            'remaining': remaining, 'total': total
        }

        if workers_info:
            info['workers'] = bi_get_all_worker_info(request)

    return info


def bi_get_all_worker_info(request: ServerRequest):
    with lock:
        _workers = json.loads(json.dumps(workers))
        # _holds_v2: dict[str, dict[str, buelon.core.step.Job]] = json.loads(json.dumps(holds_v2))

        for client_id, worker_info in _workers.items():
            client_holds = holds_v2.get(client_id, {})
            if client_holds:
                _holds: list[buelon.core.step.Job] = list(client_holds.values())
                _holds[:] = [s.to_json() for s in _holds]
                _holds: list[dict]
                worker_info['jobs'] = _holds
                worker_info['holds'] = len(_holds)

        try:
            return _workers
        except:
            return {}


def remove_ids_from_steps(step_ids: set[str]) -> int:
    """Drop `step_ids` from the pending-dispatch queues. Returns how many were removed.

    `remove_id` deliberately does not do this -- there is no `job_id -> (scope, priority)`
    index yet, so it cannot find a job in `STEPS` cheaply (BUGS.md #4). The operator
    commands below have to actually stop a job from being handed out, so they scan the
    queues instead. That is linear in the queue size, which is fine for a rare admin
    command and would not be fine on the per-job path.
    """
    with lock:
        removed = 0

        for scope, priorities in list(STEPS.items()):
            for priority, jobs in list(priorities.items()):
                keep = [job for job in jobs if job.id not in step_ids]
                removed += len(jobs) - len(keep)
                priorities[priority] = keep

        return removed


def remove_ids_from_holds(step_ids: set[str]) -> int:
    """Drop `step_ids` from every client's hold set.

    Without this a worker disconnecting after a cancel would requeue the very jobs the
    operator just removed.
    """
    with lock:
        removed = 0

        for client_holds in holds_v2.values():
            for step_id in step_ids & set(client_holds):
                del client_holds[step_id]
                removed += 1

        return removed


def cancel_errored_jobs() -> tuple[int, int]:
    """Remove every job belonging to a pipeline that contains an error.

    Returns (jobs_removed, pipelines_cancelled).
    """
    with lock:
        pipelines = len(errors)

        # Collect the full id set BEFORE removing anything: get_all_ids walks the DAG
        # through ALL_STEPS, so removing as we go would truncate later walks.
        ids: set[str] = set()
        for step in list(errors.values()):
            ids |= get_all_ids(step)

        for step_id in ids:
            remove_id(step_id)

        remove_ids_from_steps(ids)
        remove_ids_from_holds(ids)

        return len(ids), pipelines


def delete_all_jobs() -> int:
    """Wipe all job state on the hub. Returns the number of jobs removed."""
    with lock:
        count = len(ALL_STEPS)

        ALL_STEPS.clear()
        STEPS.clear()
        queued.clear()
        done.clear()
        errors.clear()
        db.clear()

        # Leave the client registrations themselves alone -- those track live
        # connections -- but empty their hold sets so a later disconnect cannot
        # requeue jobs that no longer exist.
        for client_holds in holds_v2.values():
            client_holds.clear()

        return count


def bi_handle_messages(request: ServerRequest):
    global errors, holds
    client_id = request.client_id
    with lock:
        worker_info = workers.setdefault(client_id, {})

    method = request.method
    data = json.loads(request.data.decode())

    if method == 'hold':
        bi_on_hold(request, data)
    elif method == 'release':
        bi_on_release(request, data)
    elif method == 'worker-info':
        if isinstance(data, dict):
            with lock:
                worker_info.update(data)
    elif method == 'web-info':
        info = bi_get_web_info(request, bool(data))
        request.send_data(json.dumps(info).encode())
    elif method == 'job-parents-and-results':
        res = get_job_parents_and_results(data)
        request.send_data(json.dumps(res).encode())
    elif method == 'upload':
        bi_on_upload(request, data)
    elif method == 'display':
        text = display_text()
        request.send_data(text.encode())
    elif method == 'job-status':
        status = _job_status(data)
        request.send_data(status.encode())
    elif method == 'job-status-bulk':
        with lock:
            statuses = {job_id: _job_status(job_id) for job_id in data}
        request.send_data(json.dumps(statuses).encode())
    elif method == 'errors':
        bi_on_errors(request, data)
    elif method == 'reset-errors':
        with lock:
            _steps = list(errors.values())
            errors = {}
            upload_steps(_steps)
        request.send_data(b'ok')
    elif method == 'cancel-errors':
        n_jobs, n_pipelines = cancel_errored_jobs()
        print(f'cancel-errors: removed {n_jobs:,} job(s) across {n_pipelines:,} errored pipeline(s)')
        request.send_data(json.dumps({'jobs': n_jobs, 'pipelines': n_pipelines}).encode())
    elif method == 'delete-all':
        n_jobs = delete_all_jobs()
        print(f'delete-all: removed {n_jobs:,} job(s)')
        request.send_data(json.dumps({'jobs': n_jobs}).encode())
    elif method == 'get-all-info':
        # Snapshot under the lock; compress outside it.
        with lock:
            _steps = [step for scope in STEPS.values() for steps in scope.values() for step in steps]
            _done = list(done.values())
            _queued = list(queued.values())
            _errors = list(errors.values())
            _db = dict(db)
        request.send_data(json.dumps([
            steps_to_compressed_message(_steps),
            steps_to_compressed_message(_done),
            steps_to_compressed_message(_queued),
            steps_to_compressed_message(_errors),
            _db
        ]).encode())
    elif method == 'save':
        auto_save()
        request.send_data(b'ok')


def bi_release_client(client_id: str) -> int:
    """Requeue everything `client_id` still holds and drop its registration.

    Idempotent: calling it for an unknown or already-released client is a no-op, so it
    is safe from both `bi_on_close` and the `bi_on_finally` safety net.

    Returns the number of jobs put back on the queue.
    """
    with lock:
        # Deregister first. If upload_steps somehow raised afterwards we would still
        # rather leak the jobs than leave `send_open` holding a dead id, which would
        # make every later on_finally skip its cleanup for good.
        send_open.discard(client_id)

        held = holds_v2.pop(client_id, None)
        jobs = list(held.values()) if isinstance(held, dict) else []

        if isinstance(held, dict):
            held.clear()

        d = workers.pop(client_id, None)
        if isinstance(d, dict):
            d.clear()

        if jobs:
            # At-least-once: the worker may still be running these. The hub has no way
            # to know, and losing them is worse than running them twice.
            upload_steps(jobs)

    return len(jobs)


def bi_on_open(open_info: OnOpenInfo):
    # Send connection only -- bisocket does not call this for the receive socket.
    with lock:
        holds_v2[open_info.client_id] = {}
        workers[open_info.client_id] = {}
        send_open.add(open_info.client_id)


def bi_on_close(close_info: OnCloseInfo):
    # Send connection only, and bisocket calls it from a `finally`, so it always runs.
    # This is the authoritative teardown: no more requests can arrive for this client.
    n = bi_release_client(close_info.client_id)

    if n:
        print(f'client {close_info.client_id} closed, requeued {n:,} held job(s)')


def bi_on_finally(finally_info: OnFinallyInfo):
    # Fires for BOTH of a client's connections. If the send side is still open, this
    # is the receive socket going away on its own -- the worker is still connected and
    # running jobs, so releasing its holds here would hand them to a second worker
    # while the first is mid-flight. Leave it alone; `bi_on_close` will clean up when
    # the send side actually ends.
    client_id = finally_info.client_id

    if not client_id:
        return

    with lock:
        still_live = client_id in send_open

    if still_live:
        print(f'client {client_id} lost its receive socket; send socket still open, '
              f'keeping its held jobs')
        return

    # Send side already closed, or never opened: safety net only.
    bi_release_client(client_id)


def bi_test_server():
    server = BiServer(settings.hub.host, settings.hub.port, bi_handle_messages, on_open=bi_on_open, on_close=bi_on_close, on_finally=bi_on_finally)
    server.start()


class BiWorkerJob:
    def __init__(self, mut, hold_id: str, step: buelon.core.step.Job, arg):
        self.mut = mut
        self.hold_id = hold_id
        self.step = step
        self.arg = arg

        self.status = None
        self.result = None
        self.finished = False
        self.thread = None
        self.task = None

        self.start = None

    async def arun(self):
        async def __run():
            try:
                await self._arun()
            finally:
                self.finished = True

        self.task = asyncio.create_task(__run())
        self.start = time.time()

    def run(self):
        def __run():
            try:
                self._run()
            finally:
                self.finished = True

        self.thread = threading.Thread(target=__run, daemon=True)
        self.thread.start()
        self.start = time.time()

    def _run(self):
        print('handling', self.step.name)
        try:
            r: buelon.core.step.Result = self.step.run(*self.arg, mut=self.mut)
            self.status, self.result = r.status, r.data
        except Exception as e:
            print(e)
            traceback.print_exc()
            self.status, self.result = buelon.core.step.StepStatus.error, {'error': str(e), 'trace': traceback.format_exc(), 'worker_name': f'{settings.worker.info.get("name", "Unknown")}'}
        self.start = None

    async def _arun(self):
        print('handling', self.step.name)
        try:
            r: buelon.core.step.Result = await self.step.arun(*self.arg, mut=self.mut)
            self.status, self.result = r.status, r.data
        except Exception as e:
            print(e)
            traceback.print_exc()
            self.status, self.result = buelon.core.step.StepStatus.error, {'error': str(e), 'trace': traceback.format_exc()}
        self.start = None

    @property
    def runtime(self):
        start = self.start
        if isinstance(start, (int, float)):
            return time.time() - start
        return 0

    @property
    def done(self):
        if self.finished and self.thread:
            self.thread.join()
            self.thread = None

        return self.finished

    async def adone(self):
        if self.finished and self.task:
            await self.task
            self.task = None

        return self.finished


class BiWorkerJobQueue:
    def __init__(self):
        self.jobs: list[BiWorkerJob] = []

    def finished_jobs(self):
        finished_jobs = []
        result = collections.defaultdict(list)

        for job in self.jobs:
            if job.done:
                finished_jobs.append(job)

        for job in finished_jobs:
            self.jobs.remove(job)
            result[job.hold_id].append(job)

        return result

    async def afinished_jobs(self):
        finished_jobs = []
        result = collections.defaultdict(list)

        for job in self.jobs:
            if await job.adone():
                finished_jobs.append(job)

        for job in finished_jobs:
            self.jobs.remove(job)
            result[job.hold_id].append(job)

        return result

    def put(self, job: BiWorkerJob):
        self.jobs.append(job)
        job.run()

    async def aput(self, job: BiWorkerJob):
        self.jobs.append(job)
        await job.arun()

    def qsize(self):
        return len(self.jobs)

    def max_runtime(self):
        return max([job.runtime for job in self.jobs]) if self.jobs else 0


async def bi_test_worker(jobs_at_a_time: int = 25, single_step: str | None = None, max_time: float = 60 * 20, stop_on_no_jobs: bool = False):
    """Run jobs off the hub until `max_time` (or, with `single_step`, until that one job is done).

    `single_step` is the `bue run-job -j <id>` / web "run job" path and is **single shot**:
    exactly one `hold` is attempted, and the worker exits as soon as that job has been
    released. Without that it re-held the same id forever -- see BUGS.md #5.
    """
    mut = {}
    t = time.time()
    available = jobs_at_a_time
    available_lock = asyncio.Lock()
    job_queue = BiWorkerJobQueue()
    should_stop_n = 0
    single_job_mode = single_step is not None
    stop_now = False
    def should_stop():
        return stop_now or should_stop_n > 5

    def out_of_time():
        return (time.time() - t) >= max_time

    async def idle_sleep(seconds: float):
        """Sleep `seconds` in 0.1s slices so a stop is noticed promptly."""
        end = time.time() + seconds
        while time.time() < end and not should_stop() and not out_of_time():
            await asyncio.sleep(max(0.0, min(0.1, end - time.time())))

    # `available` is the number of free job slots. It used to be read outside
    # `available_lock` and only decremented inside it, so the lock protected nothing:
    # the read-hold-decrement was not atomic and a hold could be issued against a
    # figure that had already moved. Reserve up front instead, and hand back whatever
    # the hold did not fill -- BUGS.md #6.
    async def take_capacity(most: int) -> int:
        """Reserve up to `most` slots; returns how many were actually reserved."""
        nonlocal available
        async with available_lock:
            n = max(0, min(available, most))
            available -= n
            return n

    async def give_capacity(n: int) -> None:
        """Hand `n` slots back, clamped into [0, jobs_at_a_time]."""
        nonlocal available
        async with available_lock:
            available = max(0, min(jobs_at_a_time, available + n))

    async def see_if_more():
        nonlocal should_stop_n, stop_now
        idle_backoff = 0.0
        while not out_of_time() and not should_stop():
            limit = await take_capacity(jobs_at_a_time)
            if not limit:
                # Every slot is busy, so this is not an idle spin -- the queue coroutine
                # will free slots as jobs finish.
                idle_backoff = 0.0
                await asyncio.sleep(0.1)
                continue

            uid, jobs, args = await client.hold(limit=limit, reverse=settings.worker.reverse, single_job=single_step)
            print(f'pulled {len(jobs):,} jobs')
            if stop_on_no_jobs:
                if not jobs:
                    should_stop_n += 1
                else:
                    should_stop_n = 0

            for job, arg in zip(jobs, args):
                await job_queue.aput(BiWorkerJob(mut, uid, job, arg))
                # job_queue.put(BiWorkerJob(mut, uid, job, arg))

            # Slots we reserved but the hub could not fill go straight back.
            await give_capacity(limit - len(jobs))

            if single_job_mode:
                # One attempt, then stop pulling. If we got the job,
                # handle_finished_jobs stops the run once it is released; if we did
                # not (unknown id, or already held by another worker) there is
                # nothing to wait for.
                if not jobs:
                    stop_now = True
                return

            if jobs:
                idle_backoff = 0.0
            else:
                # Nothing on the hub. Back off rather than re-asking immediately.
                idle_backoff = min(WORKER_IDLE_BACKOFF_MAX,
                                   idle_backoff * 2 or WORKER_IDLE_BACKOFF_START)
                await idle_sleep(idle_backoff)

    async def handle_finished_jobs():
        nonlocal should_stop_n, stop_now
        while not out_of_time() and not should_stop():
            finished_jobs = await job_queue.afinished_jobs()
            # finished_jobs = job_queue.finished_jobs()

            for uid, jobs in finished_jobs.items():
                steps = [job.step for job in jobs]
                statuses = [job.status for job in jobs]
                results = [job.result for job in jobs]

                print(f'finished {len(jobs):,} jobs')

                await client.release(uid, steps, statuses, results)
                n_released = len(jobs)

                if single_job_mode:
                    # The one job we were asked to run is done. Do not re-hold it.
                    await give_capacity(n_released)
                    stop_now = True
                    continue

                # The released jobs' slots are still counted as in use, so this hold
                # spends already-reserved capacity -- no `take_capacity` here.
                uid, jobs, args = await client.hold(limit=n_released, reverse=settings.worker.reverse, single_job=single_step, wait_time=0.0)
                print(f'pulled {len(jobs):,} jobs')
                if stop_on_no_jobs:
                    if not jobs and n_released:
                        should_stop_n += 1
                    else:
                        should_stop_n = 0

                for job, arg in zip(jobs, args):
                    await job_queue.aput(BiWorkerJob(mut, uid, job, arg))
                    # job_queue.put(BiWorkerJob(mut, uid, job, arg))

                await give_capacity(n_released - len(jobs))

            if not finished_jobs:
                await asyncio.sleep(0.1)

    async with BiWorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        t1 = asyncio.create_task(see_if_more())
        t2 = asyncio.create_task(handle_finished_jobs())

        while (time.time() - t) < max_time and not should_stop():
            # await asyncio.sleep(5.0)
            print(f'left: {max_time - (time.time() - t):0.2f} seconds. Available: {available:,}, Job Queue: {job_queue.qsize():,}')
            # Sleep in slices rather than one 5s block so a finished single-job run --
            # or any other stop -- is noticed straight away instead of up to 5s later.
            for _ in range(50):
                await asyncio.sleep(0.1)
                if should_stop():
                    break
            if stop_on_no_jobs:
                if not job_queue.qsize():
                    should_stop_n += 1
                else:
                    should_stop_n = 0
            if should_stop():
                break

        print('finishing up see_if_more')
        await t1
        print('finished see_if_more, now for handle_finished_jobs')
        await t2


async def v1_bi_test_worker(jobs_at_a_time: int = 25, single_step: str | None = None, iterations: int = 10_000, max_time: float = 60 * 20, stop_on_no_jobs: bool = False):
    mut = {}
    t = time.time()
    available = jobs_at_a_time
    available_lock = asyncio.Lock()
    job_queue = BiWorkerJobQueue()
    should_stop = False

    async def see_if_more():
        nonlocal available, should_stop
        while (time.time() - t) < max_time and not should_stop:
            if not available:
                await asyncio.sleep(0.1)
            else:
                uid, jobs, args = await client.hold(limit=available, reverse=settings.worker.reverse, single_job=single_step)
                print(f'pulled {len(jobs):,} jobs')
                if stop_on_no_jobs and (not jobs and available):
                    should_stop = True

                for job, arg in zip(jobs, args):
                    await job_queue.aput(BiWorkerJob(mut, uid, job, arg))
                    # job_queue.put(BiWorkerJob(mut, uid, job, arg))

                async with available_lock:
                    available -= len(jobs)

    async def handle_finished_jobs():
        nonlocal available, should_stop
        while (time.time() - t) < max_time and not should_stop:
            finished_jobs = await job_queue.afinished_jobs()
            # finished_jobs = job_queue.finished_jobs()

            for uid, jobs in finished_jobs.items():
                steps = [job.step for job in jobs]
                statuses = [job.status for job in jobs]
                results = [job.result for job in jobs]

                print(f'finished {len(jobs):,} jobs')

                await client.release(uid, steps, statuses, results)
                n_released = len(jobs)
                uid, jobs, args = await client.hold(limit=n_released, reverse=settings.worker.reverse, single_job=single_step, wait_time=0.0)
                print(f'pulled {len(jobs):,} jobs')
                if stop_on_no_jobs and (not jobs and n_released):
                    should_stop = True

                for job, arg in zip(jobs, args):
                    await job_queue.aput(BiWorkerJob(mut, uid, job, arg))
                    # job_queue.put(BiWorkerJob(mut, uid, job, arg))

                async with available_lock:
                    available += n_released - len(jobs)

            if not finished_jobs:
                await asyncio.sleep(0.1)

    async with BiWorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        t1 = asyncio.create_task(see_if_more())
        t2 = asyncio.create_task(handle_finished_jobs())

        while (time.time() - t) < max_time and not should_stop:
            # await asyncio.sleep(5.0)
            print(f'left: {max_time - (time.time() - t):0.2f} seconds. Available: {available:,}, Job Queue: {job_queue.qsize():,}')
            await asyncio.sleep(5.0)
            if stop_on_no_jobs and not job_queue.qsize():
                should_stop = True
                break
            if should_stop:
                break

        print('finishing up see_if_more')
        await t1
        print('finished see_if_more, now for handle_finished_jobs')
        await t2


    # mut = {}
    # job_queue = BiWorkerJobQueue()
    # time_since_last_hold = 0
    # time_to_send_anyway = 5
    # waited = 0
    # last_hold = 0
    # max_time_to_handle_more = 60 * 10
    #
    # if single_step:
    #     iterations = 2
    #
    # async def hold_more():
    #     nonlocal time_since_last_hold, last_hold
    #     needed = jobs_at_a_time - job_queue.qsize()
    #
    #     if needed == jobs_at_a_time or (needed > 0 and (time_to_send_anyway < (time.time() - time_since_last_hold))):
    #         limit = min(needed, jobs_at_a_time)  # int(jobs_at_a_time / 2))
    #         uid, jobs, args = await client.hold(limit=limit, reverse=settings.worker.reverse, single_job=single_step)
    #         uid: str
    #         jobs: list[buelon.step.Job]
    #         args: list[any]
    #
    #         print(f'pulled {len(jobs):,} jobs')
    #
    #         for job, arg in zip(jobs, args):
    #             # await job_queue.aput(WorkerJob(mut, uid, job, arg))
    #             job_queue.put(BiWorkerJob(mut, uid, job, arg))
    #
    #         time_since_last_hold = time.time()
    #         last_hold = len(jobs)
    #     else:
    #         last_hold = 0
    #
    # async def handle_finished_jobs():
    #     # finished_jobs = await job_queue.afinished_jobs()
    #     finished_jobs = job_queue.finished_jobs()
    #
    #     for uid, jobs in finished_jobs.items():
    #         steps = [job.step for job in jobs]
    #         statuses = [job.status for job in jobs]
    #         results = [job.result for job in jobs]
    #
    #         print(f'finished {len(jobs):,} jobs')
    #
    #         await client.release(uid, steps, statuses, results)
    #
    # async with BiWorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
    #     i = 0
    #     while ((i := i + 1) < (iterations + 1)) or job_queue.qsize():
    #         if i < iterations or max_time_to_handle_more < job_queue.max_runtime():
    #             await hold_more()
    #
    #         await handle_finished_jobs()
    #
    #         if not job_queue.qsize() or not last_hold:
    #             # if waited:
    #             #     buelon.hub_v1.delete_last_line()
    #             print(f'waiting({i:02d})' + ('.' * waited))
    #             await asyncio.sleep(1.0 if not job_queue.qsize() else 0.05)
    #             waited = ((waited + 1) % 4) + 1
    #         else:
    #             waited = 0


def bi_test_upload(upload_type: str, code_file: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    if upload_type == 'file':
        with open(code_file) as f:
            code = f.read()
    else:
        code = code_file

    return asyncio.run(_bi_test_upload(code, return_jobs))


async def _bi_test_upload(code: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    chunk = []
    jobs = []

    async with BiWorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        for step in buelon.core.pipe_interpreter.generate_steps_from_code(code):
            chunk.append(step)
            if len(chunk) >= 500:
                await client.upload(chunk)
                chunk.clear()

            if return_jobs:
                jobs.append(step)

        if chunk:
            await client.upload(chunk)

    if return_jobs:
        return jobs



# endregion


if __name__ == '__main__':
    run_server()


