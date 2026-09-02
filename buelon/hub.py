import collections
import os
import uuid
import time
import json
import base64
import asyncio
import traceback
import threading
import contextlib
from dataclasses import dataclass
from typing import Any

import orjson

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


ALL_STEPS: dict[str, list[str, buelon.step.Job]] = {}
STEPS: dict[str, dict[int, list[buelon.core.step.Job]]] = {}
queued: dict[str, buelon.step.Job] = {}
errors: dict[str, buelon.step.Job] = {}
done: dict[str, buelon.step.Job] = {}

holds: dict[str, list[buelon.step.Job]] = {}
holds_v2: dict[str, dict[str, buelon.step.Job]] = {}

workers: dict[str, dict] = {}

db: dict[str, Any] = {}  # : dict[str, bytes] = {}


# The conventional priority range, high to low. `get_steps_v2` no longer consults it
# -- it sorts on the priority number itself, so any integer dispatches (BUGS.md #45).
# Kept as the documented default range.
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

def get_steps_v2(scopes: list[str], limit: int = 100, reverse: bool = False, single_step: str | None = None):
    with lock:
        if single_step:
            s = pop_step_from_id(single_step)

            if s:
                return [s]

            return []

        result = []

        if reverse:
            scopes = scopes[::-1]

        # `STEPS` prunes itself -- an emptied priority list and then an emptied scope
        # are deleted, here and in `remove_ids_from_steps` -- so the (scope, priority)
        # pairs that actually hold jobs are few. Walking those and sorting them beats
        # scanning the full 101-priorities x N-scopes grid on every `hold`, which is
        # the common case now that idle workers back off and re-ask (BUGS.md #6, #26).
        scope_order = {s: n for n, s in enumerate(scopes)}

        def get_scope_and_priority():
            # Every priority present is dispatchable. This used to look each one up in
            # a 0-100 allow-list built from `preset_priorities` and silently drop the
            # misses, so `!priority 500` or `!priority -1` produced a job that sat in
            # `STEPS` -- counted by `bue status` -- and was never offered to anyone.
            # Sorting on the number itself needs no allow-list. BUGS.md #45.
            pairs = [
                (s, i)
                for s in scope_order
                for i in STEPS.get(s, {})
                if STEPS[s][i]
            ]
            # Priority first, scope second. Highest priority leads, or lowest under
            # `reverse` -- the same order the old nested loops produced.
            pairs.sort(key=lambda pair: (pair[1] if reverse else -pair[1],
                                         scope_order[pair[0]]))
            yield from pairs

        for scope, priority in get_scope_and_priority():
            sl = max(0, limit - len(result))
            result.extend(STEPS[scope][priority][:sl])
            remaining = STEPS[scope][priority][sl:]

            if remaining:
                STEPS[scope][priority] = remaining
            else:
                del STEPS[scope][priority]
                if not STEPS[scope]:
                    del STEPS[scope]

            if len(result) >= limit:
                break

        return result


def add_step_to_steps(step: buelon.core.step.Job, jobs: list[buelon.core.step.Job]):
    jobs.append(step)


def job_int_field(job: buelon.core.step.Job, field: str, default: int = 0) -> int:
    """Read an integer job field, repairing the job in place if it is not one.

    BUGS.md #42. The parser used to hand back the *string* `'0'` for a file-level
    `!priority` / `!retries`, and a string priority is uniquely nasty: `upload_step`
    happily keys `STEPS[scope]['0']`, `get_steps_v2` only walks the int priorities in
    `preset_priorities`, and the job is counted by `bue status` forever while no
    worker is ever offered it. Nothing errors, so there is nothing to notice.

    That is fixed at the source, but the hub also accepts jobs from clients it does
    not control -- an older `bue upload`, or a snapshot written before the fix -- so
    normalise here too rather than trusting the wire. Repairing in place keeps the
    job self-consistent for the snapshot and the web UI, not just for the dict key.
    """
    value = getattr(job, field, default)

    if isinstance(value, int) and not isinstance(value, bool):
        return value

    try:
        coerced = int(value)
    except (TypeError, ValueError):
        print(f'job {getattr(job, "name", "?")!r} ({getattr(job, "id", "?")}) has a '
              f'non-numeric {field} {value!r}; treating it as {default}')
        coerced = default

    setattr(job, field, coerced)
    return coerced


def upload_step(job: buelon.core.step.Job):
    with lock:
        priority = job_int_field(job, 'priority', 0)

        if job.scope not in STEPS:
            STEPS[job.scope] = {}

        if priority not in STEPS[job.scope]:
            STEPS[job.scope][priority] = []

        add_step_to_steps(job, STEPS[job.scope][priority])


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
            # `!retries` used to be parsed and then never read by anything -- BUGS.md
            # #14. A failed job goes back on the dispatch queue until it has burned
            # through its budget; only then does it land in `errors`.
            #
            # The counter lives on the job itself (`Job.attempts`) rather than in a
            # hub-side dict, so it survives being requeued into STEPS, dispatched, and
            # released back by a worker -- the object handed to us here is the worker's
            # deserialized copy, and `attempts` rides along in its `__dict__`.
            step.attempts = (getattr(step, 'attempts', 0) or 0) + 1
            # A string here used to raise `TypeError` on the comparison below -- `'0'`
            # is truthy, so the old `or 0` did not catch it. BUGS.md #42.
            retries = job_int_field(step, 'retries', 0)

            if step.attempts <= retries:
                print(f'job {step.id} ({step.name}) failed, retrying '
                      f'(attempt {step.attempts} of {retries + 1})')
                ALL_STEPS[step.id] = [buelon.core.step.StepStatus.pending.value, step]
                upload_step(step)
            else:
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
        else:
            # `working`, `queued` and `unknown` have no branch of their own. Reaching the
            # end of the chain with nothing written used to destroy the job: `get_steps_v2`
            # has already taken it out of `STEPS` and `bi_on_release` pops it out of
            # `holds_v2`, so no dict referenced it any more and `_job_status` reported
            # `'unknown'`. `create_return_value` lets user code return an arbitrary
            # `StepStatus` in a tuple, so this is reachable from a `.bue` script. Record it
            # as an error instead -- visible in `bue errors`, and cancellable. BUGS.md #13.
            print(f'unhandled job status {status.name!r} for job {step.id} '
                  f'({step.name}) -- recording as an error')
            ALL_STEPS[step.id] = [buelon.core.step.StepStatus.error.value, step]
            errors[step.id] = step
            existing = db.get(step.id)
            if not (isinstance(existing, dict) and 'error' in existing and 'trace' in existing):
                db[step.id] = {
                    'error': f'Unhandled job status {status.name!r} returned by job '
                             f'{step.name!r} ({step.id})',
                    'trace': '',
                }

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


def _unsendable_results_to_errors(
    jobs: list[buelon.core.step.Job],
    statuses: list[buelon.core.step.StepStatus],
    results: list[Any],
) -> tuple[list[buelon.core.step.StepStatus], list[Any]]:
    """Replace any result `json` cannot encode with an error naming the job.

    BUGS.md #43. A job's return value crosses the wire as part of a `release`,
    which bisocket encodes with `json.dumps`. One job returning a `uuid.UUID`
    used to raise `TypeError` for the *whole batch*, and because the caller never
    recovered, every job in it stayed checked out on the hub forever with nothing
    logged anywhere.

    `json.dumps` is deliberate here rather than `orjson`: it has to be the encoder
    bisocket actually uses, or this would pass values the send still rejects.
    Encoding is per job, so one bad result cannot take its batch down with it.
    """
    new_statuses, new_results = [], []

    for job, status, result in zip(jobs, statuses, results):
        try:
            json.dumps(result)
        except (TypeError, ValueError) as e:
            msg = (f'job {job.name!r} ({job.id}) returned a value that cannot be '
                   f'sent to the hub: {e}')
            print(msg)
            new_statuses.append(buelon.core.step.StepStatus.error)
            # 'error'/'trace' are the keys `bi_on_errors` looks for, so this shows
            # up under `bue errors` like any other failure.
            new_results.append({
                'error': msg,
                'trace': '',
                'worker_name': f'{settings.worker.info.get("name", "Unknown")}',
            })
        else:
            new_statuses.append(status)
            new_results.append(result)

    return new_statuses, new_results


def all_steps_to_bytes() -> bytes:
    return orjson.dumps([[step[0], step[1].to_json()] for step in ALL_STEPS.values()])


def bytes_to_all_steps(data: bytes) -> None:
    global ALL_STEPS
    # ALL_STEPS = {step[1].id: [step[0], step[1]] for step in orjson.loads(data)}
    for row in orjson.loads(data):
        row[1] = buelon.core.step.Job().from_json(row[1])
        ALL_STEPS[row[1].id] = row

# endregion

# region client

def upload_file_to_server(file_path: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    with open(file_path) as f:
        code = f.read()

    return upload_code_to_server(code, return_jobs=return_jobs)


def upload_code_to_server(code: str, return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    return bi_test_upload('code', code, return_jobs)


def submit_bootstrap_code(code: str, scope: str) -> str:
    """Wrap a `.bue` script in a one-job `.bue` that uploads it from a worker.

    `bue upload` runs the script on the machine you type it on: a `.bue` is a program
    that builds a job graph, and a `for` loop's source pipe genuinely executes to say
    how many jobs to make. `bue submit` moves that work onto the cluster by uploading
    a single bootstrap job whose body re-uploads the real script -- so the build, and
    the loop source with it, run on a worker in `scope`.

    The payload is base64 so it cannot break out of the enclosing block: inline code
    in a `.bue` is delimited by backticks, and a submitted script may well contain
    them (any inline `sqlite3` job does). Base64 has no backticks, no newlines and no
    quotes, so it embeds verbatim.
    """
    payload = base64.b64encode(code.encode()).decode()

    return (
        f'!scope {scope}\n'
        f'\n'
        f'submit:\n'
        f'    python\n'
        f'    main\n'
        f'    `\n'
        f'import base64\n'
        f'import buelon.hub\n'
        f'\n'
        f'def main(*args):\n'
        f'    buelon.hub.upload_code_to_server(\n'
        f'        base64.b64decode("{payload}").decode()\n'
        f'    )\n'
        f'`\n'
        f'\n'
        f'submit_pipe = | submit\n'
        f'submit_pipe()\n'
    )


def submit_code_to_server(code: str, scope: str | None = None,
                          return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    """Build `code` on a worker instead of locally. See `submit_bootstrap_code`."""
    if scope is None:
        scope = settings.worker.scopes.split(',')[-1].strip()

    return upload_code_to_server(submit_bootstrap_code(code, scope), return_jobs=return_jobs)


def submit_file_to_server(file_path: str, scope: str | None = None,
                          return_jobs: bool = False) -> None | list[buelon.core.step.Job]:
    with open(file_path) as f:
        code = f.read()

    return submit_code_to_server(code, scope=scope, return_jobs=return_jobs)


def display_from_server(prefix: str = '', suffix: str = '', return_value: bool = False):
    async def d():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            r = await client.display()
            return (prefix + r + suffix) if return_value else print(r)
    return asyncio.run(d())


def display_errors_from_server():
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            steps, errors = await client.errors()
            return compressed_message_to_steps(steps), errors

    steps, _errors = asyncio.run(cor())

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

    return asyncio.run(cor())


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


def get_all_info_from_server():
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_all_info()

    return asyncio.run(cor())


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
    return status


def check_job_status(job_id: str) -> str:
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_job_status(job_id)

    return asyncio.run(cor())


def check_job_status_bulk(job_ids: list[str]) -> dict[str, str]:
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.get_job_status_bulk(job_ids)

    return asyncio.run(cor())


def save_from_server():
    async def cor():
        async with BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            return await client.save()

    return asyncio.run(cor())

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


# endregion

# region auto save

# --------------------------------------------------------------------------------
# Hub persistence -- BUGS.md #15.
#
# All hub state is in-memory (see the dict table in BUGS.md). `bi_test_server` never
# called `auto_load()` and never started `auto_save_task`, so the snapshot files were
# written by nothing and read by nothing: a hub restart lost every pipeline.
#
# The shape chosen (see the write-up in BUGS.md #15) is the cheapest one that closes
# the real risks: **one file, written atomically, restored literally.**
#
#   one file      seven separate files could tear against each other -- `steps` saved
#                 while `db` did not, and the reloaded hub would dispatch jobs whose
#                 parent results had vanished.
#   atomic        temp file + `os.replace`, so a crash mid-write leaves the previous
#                 snapshot intact instead of a truncated one.
#   literal       the old `auto_load` replayed each entry through `handle_step`, which
#                 is a *state machine*, not a loader. Replaying `error` burned a retry
#                 on every restart; replaying `success` promoted children out of
#                 `queued` that the loop had not put there yet (so they were stranded)
#                 and ran the terminal DAG cleanup. Restoring the dicts directly has
#                 none of those failure modes.
#
# The one deliberate departure from a literal restore is `holds_v2`: jobs checked out
# by a worker go back on the dispatch queue, exactly as `bi_release_client` would have
# done had the worker disconnected cleanly. Nothing survives a hub restart holding a
# job, so the alternative is stranding them as `pending` in `ALL_STEPS` with no dict
# pointing at them.
# --------------------------------------------------------------------------------

AUTO_SAVE_PATH: str = os.environ.get('BUELON_AUTO_SAVE_PATH', '.auto_save')
AUTO_SAVE_INTERVAL: float = float(os.environ.get('BUELON_AUTO_SAVE_INTERVAL', 60 * 10))
AUTO_SAVE_ENABLED: bool = os.environ.get('BUELON_AUTO_SAVE', 'true').strip().lower() not in (
    'false', '0', 'no', 'off')

SNAPSHOT_NAME = 'snapshot'
SNAPSHOT_VERSION = 1

# Names of the pre-#15 per-dict files. Still read by `auto_load` so an existing
# `.auto_save/` directory is not silently ignored; never written any more.
LEGACY_SNAPSHOT_FILES = ('all_steps', 'steps', 'done', 'queued', 'errors', 'holds', 'db')

auto_saving = True

# Set by `bi_test_server` so shutdown does not have to wait out a full interval.
_auto_save_stop = threading.Event()


def snapshot_path() -> str:
    return os.path.join(AUTO_SAVE_PATH, SNAPSHOT_NAME)


def auto_save_task():
    """Daemon loop: snapshot every `AUTO_SAVE_INTERVAL` seconds until told to stop."""
    while auto_saving and AUTO_SAVE_ENABLED:
        auto_save()
        if _auto_save_stop.wait(AUTO_SAVE_INTERVAL):
            return


def _atomic_write(path: str, data: bytes) -> None:
    """Write `data` to `path` so a crash can never leave a half-written file there."""
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f'.{os.path.basename(path)}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    try:
        with open(tmp, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise


def _dumps_snapshot(payload: dict) -> bytes:
    """`orjson.dumps` the snapshot, dropping `db` entries that will not serialize.

    `db` holds whatever user code returned, so one job returning a `set` (or any other
    type orjson does not know) would otherwise cost the entire snapshot -- job graph
    included. Losing the offending result is bad; losing the whole pipeline is worse.
    """
    try:
        return orjson.dumps(payload)
    except TypeError:
        pass

    kept, dropped = {}, []
    for job_id, value in payload['db'].items():
        try:
            orjson.dumps(value)
        except TypeError:
            dropped.append(job_id)
        else:
            kept[job_id] = value

    print(f'auto_save: {len(dropped)} job result(s) are not JSON-serializable and were '
          f'left out of the snapshot: {", ".join(sorted(dropped)[:10])}'
          f'{" ..." if len(dropped) > 10 else ""}')
    return orjson.dumps({**payload, 'db': kept})


def auto_save(force: bool = False):
    """Write a single atomic snapshot of all hub state.

    `force` writes even after `auto_saving` has been cleared, so shutdown can take one
    last snapshot without the loop racing it.
    """
    if not AUTO_SAVE_ENABLED:
        return
    if not auto_saving and not force:
        return

    # Snapshot under the lock, serialize and write outside it (invariant #2). Every
    # `to_json()` is shallow-copied because it hands back the job's live `__dict__`.
    with lock:
        held_ids = {job_id for client in holds_v2.values() for job_id in client}
        payload = {
            'version': SNAPSHOT_VERSION,
            'saved_at': time.time(),
            'all_steps': [[status, dict(job.to_json())] for status, job in ALL_STEPS.values()],
            # scope/priority/order are part of the dispatch queue's meaning, so they are
            # saved rather than rebuilt from each job's own fields.
            'steps': [
                [scope, priority, [job.id for job in jobs]]
                for scope, priorities in STEPS.items()
                for priority, jobs in priorities.items()
                if jobs
            ],
            'queued': list(queued),
            'done': list(done),
            'errors': list(errors),
            # Checked-out jobs are requeued on load; see the note at the top of the region.
            'holds': sorted(held_ids),
            # Jobs reachable only from one of the id lists above (a job can be dropped
            # from ALL_STEPS by `remove_id` while still sitting in STEPS -- BUGS.md #4).
            'orphans': [
                dict(job.to_json())
                for job in _snapshot_orphans()
            ],
            'db': dict(db),
        }

    _atomic_write(snapshot_path(), _dumps_snapshot(payload))


def _snapshot_orphans() -> list[buelon.core.step.Job]:
    """Jobs referenced by a state dict but missing from `ALL_STEPS`.

    `remove_id` deletes from `ALL_STEPS` without touching `STEPS` (BUGS.md #4), so the
    id lists in the snapshot are not guaranteed to resolve. Saving the job bodies keeps
    the reload faithful instead of quietly dropping them.
    """
    with lock:
        out = []
        for job in ([j for pr in STEPS.values() for jobs in pr.values() for j in jobs]
                    + list(queued.values()) + list(done.values()) + list(errors.values())
                    + [j for client in holds_v2.values() for j in client.values()]):
            if job.id not in ALL_STEPS:
                out.append(job)
        return out


def _restore_snapshot(payload: dict) -> None:
    """Rebuild every state dict from `payload`. No `handle_step` replay -- see above."""
    jobs: dict[str, buelon.core.step.Job] = {}

    with lock:
        for status, job_json in payload.get('all_steps', []):
            job = buelon.core.step.Job().from_json(job_json)
            jobs[job.id] = job
            ALL_STEPS[job.id] = [status, job]

        for job_json in payload.get('orphans', []):
            job = buelon.core.step.Job().from_json(job_json)
            jobs.setdefault(job.id, job)

        for name, target in (('queued', queued), ('done', done), ('errors', errors)):
            for job_id in payload.get(name, []):
                if job_id in jobs:
                    target[job_id] = jobs[job_id]

        for scope, priority, job_ids in payload.get('steps', []):
            bucket = STEPS.setdefault(scope, {}).setdefault(int(priority), [])
            bucket.extend(jobs[job_id] for job_id in job_ids if job_id in jobs)

        # Jobs a worker was holding when the hub went down. Put them back exactly where
        # `bi_release_client` would have.
        for job_id in payload.get('holds', []):
            if job_id in jobs:
                upload_step(jobs[job_id])

        db.update(payload.get('db', {}))


def _load_legacy_snapshot(directory: str) -> bool:
    """Read a pre-#15 seven-file `.auto_save/` directory. Returns True if it found one.

    Restores each file into its own dict directly. The original did this by feeding
    every entry back through `handle_step`, which mutated the state it was meant to be
    reproducing -- see the note at the top of the region.
    """
    def read(name):
        path = os.path.join(directory, name)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            return f.read()

    if not any(os.path.exists(os.path.join(directory, n)) for n in LEGACY_SNAPSHOT_FILES):
        return False

    with lock:
        raw = read('db')
        if raw:
            db.update(orjson.loads(raw))

        jobs: dict[str, buelon.core.step.Job] = {}
        raw = read('all_steps')
        if raw:
            for status, job_json in orjson.loads(raw):
                job = buelon.core.step.Job().from_json(job_json)
                jobs[job.id] = job
                ALL_STEPS[job.id] = [status, job]

            for status, job in ALL_STEPS.values():
                if status == buelon.core.step.StepStatus.queued.value:
                    queued[job.id] = job
                elif status == buelon.core.step.StepStatus.success.value:
                    done[job.id] = job
                elif status == buelon.core.step.StepStatus.error.value:
                    errors[job.id] = job
                else:
                    upload_step(job)
            return True

        # No `all_steps` file: fall back to the per-dict files.
        for name, status, target in (
            ('steps', buelon.core.step.StepStatus.pending, None),
            ('done', buelon.core.step.StepStatus.success, done),
            ('queued', buelon.core.step.StepStatus.queued, queued),
            ('errors', buelon.core.step.StepStatus.error, errors),
            # v1 `holds` -- always empty under the bi hub, but an old file may have one.
            # A held job was mid-dispatch, so it is restored as pending, not queued.
            ('holds', buelon.core.step.StepStatus.pending, None),
        ):
            raw = read(name)
            if not raw:
                continue
            for job in bytes_to_steps(raw):
                job = jobs.setdefault(job.id, job)
                ALL_STEPS[job.id] = [status.value, job]
                if target is None:
                    upload_step(job)
                else:
                    target[job.id] = job
        return True


def _install_sigterm_shutdown():
    """Make SIGTERM unwind instead of killing the process. Returns an undo callable.

    Returns None (and installs nothing) if the signal cannot be claimed: not the main
    thread, no SIGTERM on this platform, or something has already handled it.
    """
    import signal

    if threading.current_thread() is not threading.main_thread():
        return None
    if not hasattr(signal, 'SIGTERM'):
        return None
    try:
        previous = signal.getsignal(signal.SIGTERM)
        if previous is not signal.SIG_DFL:
            return None

        def on_sigterm(signum, frame):
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, on_sigterm)
    except (ValueError, OSError):
        return None

    def restore():
        with contextlib.suppress(ValueError, OSError):
            signal.signal(signal.SIGTERM, previous)

    return restore


def auto_load():
    """Restore hub state written by `auto_save`. A missing or unreadable snapshot is
    not fatal -- the hub starts empty, which is what it did before #15."""
    path = snapshot_path()
    try:
        if os.path.exists(path):
            with open(path, 'rb') as f:
                payload = orjson.loads(f.read())
            _restore_snapshot(payload)
        elif not _load_legacy_snapshot(AUTO_SAVE_PATH):
            return
    except Exception:
        print(f'auto_load: could not restore {path!r}; starting with empty state')
        traceback.print_exc()
        return

    with lock:
        n_steps = sum(len(jobs) for pr in STEPS.values() for jobs in pr.values())
        print(f'auto_load: restored {len(ALL_STEPS):,} job(s) -- {n_steps:,} queued for '
              f'dispatch, {len(queued):,} waiting on a parent, {len(done):,} done, '
              f'{len(errors):,} errored, {len(db):,} result(s)')


# endregion

# region run

def run_server():
    return bi_test_server()


def run_worker(stop_on_no_jobs: bool = False):
    return asyncio.run(bi_test_worker(stop_on_no_jobs=stop_on_no_jobs))

# endregion

# region job inspection

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


# endregion

# region bi_test

from bisocket.main import Server as BiServer, Client as BiClient, BiMessage, ServerRequest, OnCloseInfo, OnOpenInfo, OnFinallyInfo, CONNECTION_RECEIVE


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
        """Hand a batch of finished jobs back to the hub.

        `results` is whatever user code returned, and it has to survive
        `json.dumps` -- that is how bisocket encodes an object payload. A job
        returning a `uuid.UUID`, a `set` or a `datetime` used to make this raise
        `TypeError` and strand the whole batch on the hub forever (BUGS.md #43),
        so a failed encode is retried once with the offending results replaced by
        an error the user can actually read in `bue errors`.

        Retrying is safe: `asend_obj` evaluates `json.dumps(data)` to build its
        argument *before* `asend` is entered, so a raise from the encoder means no
        lock was taken and no byte reached the socket.
        """
        def payload(_statuses, _results):
            return [uid, steps_to_compressed_message(jobs),
                    [status.value for status in _statuses], _results]

        try:
            request_id = await self.client.asend_obj('release', payload(statuses, results))
        except (TypeError, ValueError):
            statuses, results = _unsendable_results_to_errors(jobs, statuses, results)
            request_id = await self.client.asend_obj('release', payload(statuses, results))

        self._read_ack_in_background(request_id)

    async def update_worker_info(self):
        await self.client.asend_obj('worker-info', settings.worker.info)

    async def get_web_info(self, workers_info: bool = False):
        request_id = await self.client.asend_obj('web-info', workers_info)
        data = (await self.get_response(request_id)).get_obj()

        # The hub only includes 'workers' when `workers_info` is true, so this must not
        # assume the key is there -- `get_web_info(False)` used to raise KeyError on any
        # caller that just wanted the counts (BUGS.md #28).
        for worker_id, worker in data.get('workers', {}).items():
            if 'jobs' not in worker:
                worker['jobs'] = []

        return data

    async def get_job_parents_and_results(self, job_id: str):
        request_id = await self.client.asend_obj('job-parents-and-results', job_id)
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
        request_id = await self.client.asend_obj('upload', steps_to_compressed_message(jobs))
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
        request_id = await self.client.asend_obj('display', '')
        # return await self.websocket.recv()
        return (await self.get_response(request_id)).get_str()

    async def get_job_status(self, job_id: str) -> str:
        request_id = await self.client.asend_obj('job-status', job_id)
        # return await self.websocket.recv()
        return (await self.get_response(request_id)).get_str()

    async def get_job_status_bulk(self, job_ids: list[str]) -> list[str]:
        request_id = await self.client.asend_obj('job-status-bulk', job_ids)
        return (await self.get_response(request_id)).get_obj()

    async def errors(self):
        request_id = await self.client.asend_obj('errors', '')
        return (await self.get_response(request_id)).get_obj()

    async def reset_errors(self):
        request_id = await self.client.asend_obj('reset-errors', '')
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
        request_id = await self.client.asend_obj('get-all-info', '')

        _steps, _done, _queued, _errors, _db = (await self.get_response(request_id)).get_obj()
        _steps, _done, _queued, _errors = [compressed_message_to_steps(lst) for lst in (_steps, _done, _queued, _errors)]

        return _steps, _done, _queued, _errors, _db

    async def save(self):
        request_id = await self.client.asend_obj('save', '')
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

        for client_id, worker_info in _workers.items():
            client_holds = holds_v2.get(client_id, {})
            if client_holds:
                _holds: list[dict] = [job.to_json() for job in client_holds.values()]
                worker_info['jobs'] = _holds
                worker_info['holds'] = len(_holds)

        return _workers


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

                # Emptied queues are dropped rather than left behind, so the dispatch
                # scan in `get_steps_v2` only ever sees pairs that hold jobs.
                if keep:
                    priorities[priority] = keep
                else:
                    del priorities[priority]

            if not priorities:
                del STEPS[scope]

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
    # Send connection only -- bisocket calls `on_open_receive`, which the hub does not
    # register, for the receive socket.
    with lock:
        holds_v2[open_info.client_id] = {}
        workers[open_info.client_id] = {}


def bi_on_close(close_info: OnCloseInfo):
    # Send connection only, and bisocket calls it from a `finally`, so it always runs.
    # This is the authoritative teardown: no more requests can arrive for this client.
    n = bi_release_client(close_info.client_id)

    if n:
        print(f'client {close_info.client_id} closed, requeued {n:,} held job(s)')


def bi_on_finally(finally_info: OnFinallyInfo):
    # Fires for BOTH of a client's connections (see BUGS.md #2). The receive socket
    # going away on its own does not mean the worker is gone -- it is still connected
    # and running jobs, so releasing its holds here would hand them to a second worker
    # while the first is mid-flight. Leave it alone; `bi_on_close` will clean up when
    # the send side actually ends.
    #
    # `connection_type` needs bisocket >= 0.0.9. `None` means the handshake failed
    # before the socket said which one it was -- nothing was ever held on it, so the
    # idempotent cleanup is the safe reading.
    client_id = finally_info.client_id

    if not client_id:
        return

    if getattr(finally_info, 'connection_type', None) == CONNECTION_RECEIVE:
        print(f'client {client_id} lost its receive socket; keeping its held jobs '
              f'until the send socket closes')
        return

    # Send connection, or an unidentified one: safety net behind `bi_on_close`.
    bi_release_client(client_id)


def bi_test_server():
    global auto_saving

    # Restore before the socket opens, so a worker cannot hold a job out of a
    # half-populated queue. BUGS.md #15 -- nothing used to call either of these, which
    # made a hub restart a total loss of every pipeline.
    auto_load()

    server = BiServer(settings.hub.host, settings.hub.port, bi_handle_messages, on_open=bi_on_open, on_close=bi_on_close, on_finally=bi_on_finally)

    saver = None
    if AUTO_SAVE_ENABLED:
        saver = threading.Thread(target=auto_save_task, daemon=True, name='buelon-auto-save')
        saver.start()

    # `docker stop` and systemd both stop a service with SIGTERM, whose default action
    # kills the process outright -- no `finally`, so no shutdown snapshot, and up to
    # `AUTO_SAVE_INTERVAL` of work lost on every ordinary restart. Turning it into a
    # `SystemExit` lets the block below run and exits just as quietly. Installed only
    # if nobody else has claimed the signal.
    _restore_sigterm = _install_sigterm_shutdown()

    try:
        server.start()
    finally:
        # Stop the loop first so it cannot race the shutdown snapshot, then take one
        # last one -- a clean `bue hub` restart should lose nothing at all.
        auto_saving = False
        _auto_save_stop.set()
        if _restore_sigterm is not None:
            _restore_sigterm()
        if saver is not None:
            saver.join(timeout=5)
        try:
            auto_save(force=True)
        except Exception:
            traceback.print_exc()


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
        # NOTE: `!timeout` is NOT enforced on this path. It runs the job on a bare
        # thread, and a Python thread cannot be interrupted from outside, so there is
        # nothing to cancel. The live worker uses `aput`/`_arun`, which can. BUGS.md #14.
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
        # `!timeout` was parsed and then read by nothing -- BUGS.md #14. A hung job held
        # its worker slot for the whole 20-minute `max_time`. `<= 0` (the `Job` class
        # default) still means "no timeout".
        #
        # Best-effort by construction: `wait_for` cancels the *await*, which really does
        # stop a coroutine job, but a plain `def` job runs under `asyncio.to_thread` and
        # that thread keeps going after we stop waiting for it. Either way the slot is
        # freed and the job is reported as an error rather than occupying the worker.
        timeout = getattr(self.step, 'timeout', 0.0) or 0.0
        try:
            coro = self.step.arun(*self.arg, mut=self.mut)
            if timeout > 0:
                r: buelon.core.step.Result = await asyncio.wait_for(coro, timeout)
            else:
                r: buelon.core.step.Result = await coro
            self.status, self.result = r.status, r.data
        except asyncio.TimeoutError:
            # Must precede the generic handler: from 3.11 on `asyncio.TimeoutError` is
            # the builtin `TimeoutError`, an ordinary `Exception` subclass.
            msg = (f'job {self.step.name!r} ({self.step.id}) exceeded its '
                   f'!timeout of {timeout:g} seconds')
            print(msg)
            self.status, self.result = buelon.core.step.StepStatus.error, {
                'error': msg,
                'trace': '',
                'worker_name': f'{settings.worker.info.get("name", "Unknown")}',
            }
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


def _log_worker_task_exception(task: asyncio.Task) -> None:
    """Print the traceback of a worker task that died, when it dies."""
    if task.cancelled():
        return

    exc = task.exception()

    if exc is not None:
        print(f'worker task {task.get_name()!r} stopped with an exception:')
        traceback.print_exception(type(exc), exc, exc.__traceback__)


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
                n_released = len(jobs)

                print(f'finished {n_released:,} jobs')

                # Anything raised in here used to kill this coroutine outright. The
                # worker kept polling for new work but never handed another finished
                # job back, and the slots these jobs occupied were never returned --
                # `available` walked down to zero one dead batch at a time, with no
                # traceback until the 20-minute `await t2` at shutdown. BUGS.md #43.
                try:
                    await client.release(uid, steps, statuses, results)

                    if single_job_mode:
                        # The one job we were asked to run is done. Do not re-hold it.
                        await give_capacity(n_released)
                        stop_now = True
                        continue

                    # The released jobs' slots are still counted as in use, so this
                    # hold spends already-reserved capacity -- no `take_capacity`.
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
                except Exception:
                    # Hand the slots back and keep going. If the release itself is
                    # what failed, the hub still has the jobs held; it requeues them
                    # when this client disconnects.
                    traceback.print_exc()
                    await give_capacity(n_released)
                    if single_job_mode:
                        stop_now = True

            if not finished_jobs:
                await asyncio.sleep(0.1)

    async with BiWorkerClient(settings.worker.host, settings.worker.port, ['test'] + settings.worker.scopes.split(',')) as client:
        t1 = asyncio.create_task(see_if_more(), name='see_if_more')
        t2 = asyncio.create_task(handle_finished_jobs(), name='handle_finished_jobs')
        # Neither task is awaited until the run ends, so a crash in either was
        # invisible for up to `max_time` -- the worker just quietly stopped doing
        # half its job. Say so the moment it happens. BUGS.md #43.
        t1.add_done_callback(_log_worker_task_exception)
        t2.add_done_callback(_log_worker_task_exception)

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


