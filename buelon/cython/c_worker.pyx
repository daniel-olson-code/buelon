import os
import asyncio
import traceback
import sys
import enum
import contextlib
from typing import List, Any

import asyncio_pool
import unsync

import buelon.core.step
import buelon.hub
import buelon.bucket
import buelon.helpers.json_parser

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass

WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS', 'true')
try:
    N_WORKER_PROCESSES: int = int(os.environ['N_WORKER_PROCESSES'])
except (KeyError, ValueError):
    N_WORKER_PROCESSES = 15

bucket_client = buelon.bucket.Client()
hub_client: buelon.hub.HubClient = buelon.hub.HubClient(WORKER_HOST, WORKER_PORT)

JOB_CMD = f'{sys.executable} -c "import buelon.worker;buelon.worker.job()"'


class HandleStatus(enum.Enum):
    success = 'success'
    pending = 'pending'
    almost = 'almost'
    none = 'none'


@contextlib.contextmanager
def new_client_if_subprocess():
    global hub_client
    if PIPE_WORKER_SUBPROCESS_JOBS == 'true':
        with buelon.hub.HubClient(WORKER_HOST, WORKER_PORT) as client:
            yield client
    else:
        yield hub_client


def job(step_id: str | None = None) -> None:
    if step_id:
        _step = buelon.hub.get_step(step_id)
    else:
        _step = buelon.hub.get_step(os.environ['STEP_ID'])

    if _step is None:
        with new_client_if_subprocess() as client:
            client.reset(step_id if step_id else os.environ['STEP_ID'])
            return

    print('handling', _step.name)
    try:
        args = [buelon.hub.get_data(_id) for _id in _step.parents]
        r: buelon.core.step.Result = _step.run(*args)
        buelon.hub.set_data(_step.id, r.data)

        with new_client_if_subprocess() as client:
            if r.status == buelon.core.step.StepStatus.success:
                client.done(_step.id)
            elif r.status == buelon.core.step.StepStatus.pending:
                client.pending(_step.id)
            elif r.status == buelon.core.step.StepStatus.reset:
                client.reset(_step.id)
            elif r.status == buelon.core.step.StepStatus.cancel:
                client.cancel(_step.id)
            else:
                raise Exception('Invalid step status')
    except Exception as e:
        print(' - Error - ')
        print(str(e))
        traceback.print_exc()
        with new_client_if_subprocess() as client:
            client.error(
                _step.id,
                str(e),
                f'{traceback.format_exc()}'
            )


async def run(step_id: str | None = None) -> None:
    if PIPE_WORKER_SUBPROCESS_JOBS != 'true':
        return job(step_id)
    env = {**os.environ, 'STEP_ID': step_id}
    p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
    await p.wait()


async def work():
    _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', 'default')
    scopes: List = _scopes.split(',')
    print('scopes', scopes)

    last_loop_had_steps = True

    async with asyncio_pool.AioPool(size=N_WORKER_PROCESSES) as pool:
        while True:
            steps = hub_client.get_steps(scopes)

            if not steps:
                if last_loop_had_steps:
                    last_loop_had_steps = False
                    print('waiting..')
                await asyncio.sleep(1.)
                continue

            last_loop_had_steps = True

            for s in steps:
                await pool.spawn(run(s))


def main():
    asyncio.run(_main())


async def _main():
    global hub_client
    with hub_client:
        await work()


if __name__ == '__main__':
    main()
