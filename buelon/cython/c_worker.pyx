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
PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS', 'false')

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
        yield bucket_client
    else:
        yield hub_client


def job(step_id: str | None = None) -> None:
    if step_id:
        _step = buelon.hub.get_step(step_id)
    else:
        _step = buelon.hub.get_step(os.environ['STEP_ID'])
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

    async with asyncio_pool.AioPool(size=50) as pool:
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


#
#
# """Worker module for handling pipeline steps.
#
# This module contains functions and classes for managing and executing pipeline steps,
# communicating with a pipeline server, and handling various step statuses.
#
# Typical usage example:
#
#   `python3 worker.py`
#
#   # or
#
#   import worker
#   worker.main()
#
# """
#
# import os
# import asyncio
# import traceback
# import socket
# import sys
# import enum
# from typing import List, Any
#
# import asyncio_pool
#
# import buelon.core.step
# import buelon.hub
# import buelon.bucket
# import buelon.helpers.json_parser
#
# try:
#     import dotenv
#
#     dotenv.load_dotenv()
# except ModuleNotFoundError:
#     pass
#
# WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
# WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
# PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS') == 'true'
#
# bucket_client = buelon.bucket.Client()
#
# JOB_CMD = f'{sys.executable} -c "import worker;worker.job()"'
#
#
# class HandleStatus(enum.Enum):
#     """Enumeration of possible handling statuses."""
#     success = 'success'
#     pending = 'pending'
#     almost = 'almost'
#     none = 'none'
#
#
# def job(step_id: str | None = None) -> None:
#     """Execute a job for a given step.
#
#     Args:
#         step_id: The ID of the step to execute. If None, it's read from the environment.
#
#     Raises:
#         Exception: If an invalid step status is encountered.
#     """
#     if step_id:
#         _step = buelon.hub.get_step(step_id)
#     else:
#         _step = buelon.hub.get_step(os.environ['STEP_ID'])
#     print('handling', _step.name)
#     try:
#         args = [buelon.hub.get_data(_id) for _id in _step.parents]
#         r: buelon.core.step.Result = _step.run(*args)
#         buelon.hub.set_data(_step.id, r.data)
#
#         if r.status == buelon.core.step.StepStatus.success:
#             request_done(_step.id)
#         elif r.status == buelon.core.step.StepStatus.pending:
#             request_pending(_step.id)
#         elif r.status == buelon.core.step.StepStatus.reset:
#             request_reset(_step.id)
#         elif r.status == buelon.core.step.StepStatus.cancel:
#             request_cancel(_step.id)
#         else:
#             raise Exception('Invalid step status')
#     except Exception as e:
#         print(' - Error - ')
#         print(str(e))
#         traceback.print_exc()
#         request_error(
#             _step.id,
#             str(e),
#             f'{traceback.format_exc()}'
#         )
#
#
# async def run(step_id: str | None = None) -> None:
#     """Run a job asynchronously.
#
#     Args:
#         step_id: The ID of the step to run. If None, it's read from the environment.
#     """
#     if PIPE_WORKER_SUBPROCESS_JOBS != 'true':
#         return job(step_id)
#     env = {**os.environ, 'STEP_ID': step_id}
#     p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
#     await p.wait()
#
#
# async def work():
#     """Main worker loop that continuously processes steps."""
#     _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', 'default')
#     scopes: List = _scopes.split(',')
#     print('scopes', scopes)
#
#     last_loop_had_steps = True  # used to print 'waiting..' only once when no steps
#
#     async with asyncio_pool.AioPool(size=50) as pool:
#         while True:
#             steps = request_steps(scopes)
#
#             if not steps:
#                 if last_loop_had_steps:
#                     last_loop_had_steps = False
#                     print('waiting..')
#                 await asyncio.sleep(1.)
#                 continue
#
#             last_loop_had_steps = True
#
#             for s in steps:
#                 await pool.spawn(run(s))
#
#
# def receive(conn) -> bytes:
#     """Receive data from a socket connection.
#
#     Args:
#         conn: The socket connection.
#
#     Returns:
#         The received data as bytes.
#     """
#     data = b''
#     while not data.endswith(b'[-_-]'):
#         v = conn.recv(1024)
#         data += v
#     return data[:-5]
#
#
# def send(conn, data: bytes) -> None:
#     """Send data through a socket connection.
#
#     Args:
#         conn: The socket connection.
#         data: The data to send.
#     """
#     conn.sendall(data + b'[-_-]')
#
#
# def request_steps(scopes: list[str]) -> Any:
#     """Request steps from the buelon.hub server.
#
#     Args:
#         scopes: A list of scopes to request steps for.
#
#     Returns:
#         The steps received from the server.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'get-steps' + buelon.hub.PIPELINE_SPLIT_TOKEN + buelon.helpers.json_parser.dumps(scopes)
#         send(s, data)
#         data = receive(s)
#         return buelon.helpers.json_parser.loads(data)
#
#
# def request_done(step_id: str) -> None:
#     """Send a 'done' request to the buelon.hub server.
#
#     Args:
#         step_id: The ID of the completed step.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'done' + buelon.hub.PIPELINE_SPLIT_TOKEN + step_id.encode()
#         send(s, data)
#
#
# def request_pending(step_id: str) -> None:
#     """Send a 'pending' request to the buelon.hub server.
#
#     Args:
#         step_id: The ID of the pending step.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'pending' + buelon.hub.PIPELINE_SPLIT_TOKEN + step_id.encode()
#         send(s, data)
#
#
# def request_cancel(step_id: str) -> None:
#     """Send a 'cancel' request to the buelon.hub server.
#
#     Args:
#         step_id: The ID of the step to cancel.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'cancel' + buelon.hub.PIPELINE_SPLIT_TOKEN + step_id.encode()
#         send(s, data)
#
#
# def request_reset(step_id: str) -> None:
#     """Send a 'reset' request to the buelon.hub server.
#
#     Args:
#         step_id: The ID of the step to reset.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'reset' + buelon.hub.PIPELINE_SPLIT_TOKEN + step_id.encode()
#         send(s, data)
#
#
# def request_error(step_id: str, msg: str, trace: str) -> None:
#     """Send an 'error' request to the buelon.hub server.
#
#     Args:
#         step_id: The ID of the step that encountered an error.
#         msg: The error message.
#         trace: The error traceback.
#     """
#     global WORKER_HOST, WORKER_PORT
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         s.connect((WORKER_HOST, WORKER_PORT))
#         data = b'error' + buelon.hub.PIPELINE_SPLIT_TOKEN + buelon.helpers.json_parser.dumps(
#             {'step_id': step_id, 'msg': msg, 'trace': trace})
#         send(s, data)
#
#
# def main():
#     """Main function to start the worker."""
#     asyncio.run(work())
#
#
# try:
#     from cython.c_worker import *
# except (ImportError, ModuleNotFoundError):
#     pass
#
#
# if __name__ == '__main__':
#     main()
#
