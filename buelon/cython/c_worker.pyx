import os
import asyncio
import traceback
import sys
import enum
import contextlib
import gc

import asyncio_pool

import buelon.core.step
import buelon.hub
import buelon.bucket
import buelon.helpers.json_parser

import time

try:
    import dotenv
    dotenv.load_dotenv('.env')
except ModuleNotFoundError:
    pass

DEFAULT_SCOPES = 'production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small,default'

WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS', 'true')
try:
    N_WORKER_PROCESSES: int = int(os.environ['N_WORKER_PROCESSES'])
except (KeyError, ValueError):
    N_WORKER_PROCESSES = 15
REVERSE_PRIORITY = os.environ.get('REVERSE_PRIORITY', 'false') == 'true'
try:
    WORKER_RESTART_INTERVAL = int(os.environ.get('WORKER_RESTART_INTERVAL', 60 * 60 * 2))
except ValueError:
    WORKER_RESTART_INTERVAL = 60 * 60 * 2

try:
    WORKER_JOB_TIMEOUT = int(os.environ.get('WORKER_JOB_TIMEOUT', 60 * 60 * 2))
except:
    WORKER_JOB_TIMEOUT = 60 * 60 * 2

bucket_client = buelon.bucket.Client()
hub_client: buelon.hub.HubClient = buelon.hub.HubClient(WORKER_HOST, WORKER_PORT)

JOB_CMD = f'{sys.executable} -c "import buelon.worker;buelon.worker.job()"'

TEMP_FILE_LIFETIME = 60 * 60 * 3


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
    global bucket_client
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
            client: buelon.hub.HubClient
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
    finally:
        if PIPE_WORKER_SUBPROCESS_JOBS == 'true':
            del bucket_client
            del buelon.hub.bucket_client

async def run(step_id: str | None = None) -> str:
    if PIPE_WORKER_SUBPROCESS_JOBS != 'true':
        job(step_id)
        return 'done'
    env = {**os.environ, 'STEP_ID': step_id}
    p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
    await p.wait()
    return 'done'


async def get_steps(scopes: list[str]):
    await asyncio.sleep(0)
    return hub_client.get_steps(scopes, reverse=REVERSE_PRIORITY)


async def work():
    start_time = time.time()

    _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', DEFAULT_SCOPES)
    scopes: list[str] = _scopes.split(',')
    print('scopes', scopes)

    last_loop_had_steps = True

    steps = []
    while True:
        try:
            if os.environ.get('STOP_WORKER', 'false') == 'true':
                return

            if not steps:
                try:
                    steps = await asyncio.wait_for(get_steps(scopes), timeout=30)
                except asyncio.TimeoutError:
                    print("Timeout while getting steps from hub")
                    await asyncio.sleep(5)
                    continue
                except Exception as e:
                    print('Error getting steps:', e)
                    await asyncio.sleep(5)
                    continue

            if not steps:
                if last_loop_had_steps:
                    last_loop_had_steps = False
                    print('waiting..')
                await asyncio.sleep(1.)
                continue

            last_loop_had_steps = True

            async with asyncio_pool.AioPool(size=N_WORKER_PROCESSES) as pool:
                futures = []
                fut_steps = await pool.spawn(asyncio.wait_for(get_steps(scopes), timeout=35))
                for s in steps:
                    fut = await pool.spawn(asyncio.wait_for(run(s), timeout=WORKER_JOB_TIMEOUT))
                    futures.append((fut, s))

            for fut, step in futures:
                try:
                    fut.result()
                except asyncio.TimeoutError:
                    print(f"Job timed out for step: {step}")
                    await hub_client.error(step, "Job timed out", "")
                except Exception as e:
                    print(f'Error running step: {e}', step)
                    await hub_client.error(step, str(e), traceback.format_exc())

            try:
                steps = fut_steps.result()
            except asyncio.TimeoutError:
                print("Timeout while getting next batch of steps")
                steps = []
            except Exception as e:
                print('Error getting next batch of steps:', e)
                steps = []

        except Exception as e:
            print(f"Unexpected error in work loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)

        # Force garbage collection
        gc.collect()

        # Check if we need to restart the worker
        if time.time() - start_time > WORKER_RESTART_INTERVAL:
            print("Restarting worker...")
            return


# async def work():
#     _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', DEFAULT_SCOPES)
#     scopes: list[str] = _scopes.split(',')
#     print('scopes', scopes)
#
#     last_loop_had_steps = True
#
#     steps = []
#     while True:
#         futures = []
#         async with asyncio_pool.AioPool(size=N_WORKER_PROCESSES) as pool:
#             try:
#                 if not steps:
#                     steps = await get_steps(scopes)  # hub_client.get_steps(scopes)
#             except Exception as e:
#                 steps = []
#                 print('Error getting steps:', e)
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
#             if os.environ.get('STOP_WORKER', 'false') == 'true':
#                 return
#
#             fut_steps = await pool.spawn(get_steps(scopes))
#
#             for s in steps:
#                 fut = await pool.spawn(run(s))
#                 futures.append((fut, s))
#
#         for fut, step in futures:
#             try:
#                 fut.result()  # check for exceptions
#             except Exception as e:
#                 print('Error running step:', e, step)
#
#         try:
#             print('getting steps')
#             steps = fut_steps.result()
#         except Exception as e:
#             print('Error getting steps:', e)
#             steps = []


def is_hanging_script(path: str, extension: str = '.py'):
    """
    Checks if a temporary script created by the worker that was not properly cleaned up

    Args:
        path: file path
        extension: file extension

    Returns: True if the file is a hanging script
    """
    # example: temp_ace431278698111efab2de73d545b8b66.py
    file_name = os.path.basename(path)
    # temp_bue_
    return (file_name.startswith('temp_')
            and file_name.endswith(extension)
            # and len(file_name) == 41
            and (time.time() - os.path.getmtime(path)) > TEMP_FILE_LIFETIME)


def file_age(path: str):
    return time.time() - os.path.getmtime(path)


async def cleaner():
    """
    Cleans up hanging scripts created by the worker that were not properly cleaned up
    """
    while True:
        for root, dirs, files in os.walk('.'):
            for file in files:
                if is_hanging_script(os.path.join(root, file)):
                    os.remove(os.path.join(root, file))
                await asyncio.sleep(0.1)
            break
        if os.path.exists('./__pycache__'):
            for root, dirs, files in os.walk('./__pycache__'):
                for file in files:
                    if is_hanging_script(os.path.join(root, file), '.pyc'):
                        os.remove(os.path.join(root, file))
                break
        await asyncio.sleep(60 * 10)  # Run every 10 minutes


def main():
    """
    Main function to run the worker
    """
    asyncio.run(_main())


async def _main():
    """
    Main coroutine to run the worker
    """
    global hub_client
    cleaner_task = asyncio.create_task(cleaner())
    with hub_client:
        await work()
    cleaner_task.cancel()





if __name__ == '__main__':
    main()


