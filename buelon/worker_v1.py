import os
import asyncio
import traceback
import sys
import enum
import contextlib
import gc
import queue
import collections
import threading
import tempfile
import json
import inspect
from typing import Any

import asyncio_pool

import buelon.core.step
import buelon.hub_v1
import buelon.bucket_v1
import buelon.helpers.json_parser
import buelon.helpers.persistqueue

import time

try:
    import dotenv
    dotenv.load_dotenv(os.environ.get('ENV_PATH', '.env'))
except ModuleNotFoundError:
    pass

DEFAULT_SCOPES = 'production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small,default'

WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS', 'false') == 'true'
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
hub_client: buelon.hub_v1.HubClient = buelon.hub_v1.HubClient(WORKER_HOST, WORKER_PORT)

JOB_CMD = f'{sys.executable} -c "import buelon.worker;buelon.worker.job()"'

TEMP_FILE_LIFETIME = 60 * 60 * 3

transactions = buelon.helpers.persistqueue.JsonPersistentQueue(os.path.join('.bue', os.environ.get('WORKER_QUEUE', 'worker_queue.queue')))  # persistqueue.Queue(os.path.join('.bue', 'worker_queue.queue'))  # persistqueue.SQLiteQueue(os.path.join('.bue', 'worker_queue.db'), auto_commit=True)  # queue.Queue()


class HandleStatus(enum.Enum):
    success = 'success'
    pending = 'pending'
    almost = 'almost'
    none = 'none'


def try_for_int(v: Any, default_value: int = 0) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return default_value


@contextlib.contextmanager
def new_client_if_subprocess():
    global hub_client
    if PIPE_WORKER_SUBPROCESS_JOBS:
        with buelon.hub_v1.HubClient(WORKER_HOST, WORKER_PORT) as client:
            yield client
    else:
        yield hub_client


def transaction_worker():
    def convert_transaction(transaction: tuple[dict, dict] | tuple[buelon.core.step.Step, buelon.core.step.Result] | None):
        if transaction is None:
            return None

        _step, r = transaction

        if isinstance(_step, dict):
            _step = buelon.core.step.Step().from_json(_step)

        if isinstance(r, dict):
            r = buelon.core.step.Result().from_dict(r)

        return _step, r

    while True:
        transaction = convert_transaction(transactions.get())

        if transaction is None:
            break

        chunk = None
        _step, r = transaction

        if 100 < transactions.qsize() or True:
            print('** running bulk!!')
            chunk = []
            if isinstance(r, dict) and 'error' in r:
                with new_client_if_subprocess() as client:
                    client.error(_step.id, r['e'], r['trace'])
            else:
                chunk.append((_step, r))
            if transactions.qsize() > 1:
                for _ in range(max(1, min(1000, transactions.qsize()))):
                    transaction = convert_transaction(transactions.get())
                    if transaction is None:
                        transactions.put(None)
                        break
                    _step, r = transaction
                    if isinstance(r, dict) and 'error' in r:
                        with new_client_if_subprocess() as client:
                            client.error(_step.id, r['e'], r['trace'])
                    else:
                        chunk.append((_step, r))
            try:
                buelon.hub_v1.bulk_set_data({_step.id: r.data for _step, r in chunk})
                with new_client_if_subprocess() as client:
                    client: buelon.hub_v1.HubClient

                    dones = [s for s, r in chunk if r.status == buelon.core.step.StepStatus.success]
                    if dones:
                        client.dones(dones)
                    pendings = [s for s, r in chunk if r.status == buelon.core.step.StepStatus.pending]
                    if pendings:
                        client.pendings(pendings)
                    resets = [s for s, r in chunk if r.status == buelon.core.step.StepStatus.reset]
                    if resets:
                        client.resets(resets)
                    cancels = [s for s, r in chunk if r.status == buelon.core.step.StepStatus.cancel]
                    if cancels:
                        client.cancels(cancels)
            except:
                for _step, r in chunk:
                    try:
                        with new_client_if_subprocess() as client:
                            client: buelon.hub_v1.HubClient
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
                        del _step
                        del r
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
        else:
            if isinstance(r, dict) and 'error' in r:
                with new_client_if_subprocess() as client:
                    client.error(_step.id, r['e'], r['trace'])
                continue

            try:
                buelon.hub_v1.set_data(_step.id, r.data)

                with new_client_if_subprocess() as client:
                    client: buelon.hub_v1.HubClient
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
                del _step
                del r
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


def job(step_id: str | buelon.core.step.Step | None, datas: dict | None = None) -> None:
    global bucket_client
    os.environ['BUELON_JOB'] = 'true'
    if step_id:
        if not isinstance(step_id, buelon.core.step.Step):
            _step = buelon.hub_v1.get_step(step_id)
        else:
            _step = step_id
    else:
        _step = buelon.hub_v1.get_step(os.environ['STEP_ID'])

    if _step is None:
        with new_client_if_subprocess() as client:
            client.reset(step_id if step_id else os.environ['STEP_ID'])
            return

    print('handling', _step.name)
    try:
        args = []  # [buelon.hub_v1.get_data(_id) for _id in _step.parents]
        for _id in _step.parents:
            if isinstance(datas, dict) and datas.get(_id):
                args.append(datas[_id])
            else:
                args.append(buelon.hub_v1.get_data(_id))
        r: buelon.core.step.Result = _step.run(*args)
        if not PIPE_WORKER_SUBPROCESS_JOBS:
            # _step.to_json()
            transactions.put((_step.to_json(), r.to_dict()))
        else:
            buelon.hub_v1.set_data(_step.id, r.data)

            with new_client_if_subprocess() as client:
                client: buelon.hub_v1.HubClient
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
            del buelon.hub_v1.bucket_client


async def run(step_id: str | buelon.core.step.Step | None = None, data=None) -> str:
    if not PIPE_WORKER_SUBPROCESS_JOBS:
        await asyncio.sleep(0)
        await asyncio.to_thread(job, step_id, data)
        # job(step_id, data)
        return 'done'
    if isinstance(step_id, buelon.core.step.Step):
        step_id = step_id.id
    env = {**os.environ, 'STEP_ID': step_id}
    p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
    await p.wait()
    return 'done'


async def get_steps(scopes: list[str], **kwargs):
    await asyncio.sleep(0)
    return hub_client.get_steps(scopes, reverse=REVERSE_PRIORITY, **kwargs)


async def work(include_working=False):
    start_time = time.time()

    _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', DEFAULT_SCOPES)
    scopes: list[str] = _scopes.split(',')
    print('scopes', scopes)

    last_loop_had_steps = True

    steps = []
    try:
        while True:
            try:
                if os.environ.get('STOP_WORKER', 'false') == 'true':
                    return

                if not steps:
                    try:
                        steps = await asyncio.wait_for(get_steps(scopes, all_types=include_working), timeout=30)
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
                    else:
                        # one shot for testing
                        if os.environ.get('WORKER_ONE_SHOT') == 'true':
                            return
                    await asyncio.sleep(1.)
                    continue

                last_loop_had_steps = True

                async with asyncio_pool.AioPool(size=N_WORKER_PROCESSES) as pool:
                    futures = []
                    fut_steps = await pool.spawn(asyncio.wait_for(get_steps(scopes, all_types=include_working), timeout=35))

                    step_objs = buelon.hub_v1.bulk_get_step(steps)
                    try:
                        parents = [step_id for s in step_objs.values() for step_id in s.parents]
                    except AttributeError:  # 'NoneType' object has no attribute 'parents'
                        print('Some steps were not found')
                        new_steps = []
                        parents = []
                        for step_id, s in step_objs.items():
                            if s is None:
                                print(f"Step {step_id} not found")
                                await hub_client.reset(step_id)
                            else:
                                new_steps.append(step_id)
                                parents.extend(s.parents)
                        steps = new_steps
                    step_data = buelon.hub_v1.bulk_get_data(parents) if parents else {}
                    for s in steps:
                        fut = await pool.spawn(asyncio.wait_for(run(step_objs[s], step_data), timeout=try_for_int(step_objs[s].timeout, WORKER_JOB_TIMEOUT)))
                        futures.append((fut, s))
                    # for s in steps:
                    #     fut = await pool.spawn(asyncio.wait_for(run(s), timeout=WORKER_JOB_TIMEOUT))
                    #     futures.append((fut, s))

                for fut, step in futures:
                    try:
                        fut.result()
                    except asyncio.TimeoutError:
                        print(f"Job timed out for id: {step}")
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
            except KeyboardInterrupt:
                print('Gracefully quitting')
                break
            except Exception as e:
                print(f"Unexpected error in work loop: {e}")
                traceback.print_exc()
                await asyncio.sleep(5)

            # Force garbage collection
            gc.collect()

            # Check if we need to restart the worker
            if time.time() - start_time > WORKER_RESTART_INTERVAL:
                print("Restarting worker...")
                break

            if os.environ.get('WORKER_ONCE', 'false') == 'true':
                break

            if not PIPE_WORKER_SUBPROCESS_JOBS:
                if transactions.qsize() > 1000:
                    break
    except KeyboardInterrupt:
        print('Gracefully quitting')


async def test_job(step_id: str | buelon.core.step.Step | None, datas: dict | None = None, mut = None) -> None:
    global bucket_client
    os.environ['BUELON_JOB'] = 'true'
    if step_id:
        if not isinstance(step_id, buelon.core.step.Step):
            _step = buelon.hub_v1.get_step(step_id)
        else:
            _step = step_id
    else:
        _step = buelon.hub_v1.get_step(os.environ['STEP_ID'])

    if _step is None:
        with new_client_if_subprocess() as client:
            client.reset(step_id if step_id else os.environ['STEP_ID'])
            return

    print('handling', _step.name)
    try:
        args = []  # [buelon.hub_v1.get_data(_id) for _id in _step.parents]
        for _id in _step.parents:
            if isinstance(datas, dict) and datas.get(_id):
                args.append(datas[_id])
            else:
                args.append(buelon.hub_v1.get_data(_id))

        await asyncio.sleep(0.001)
        r: buelon.core.step.Result = await _step.run_async(*args, mut=mut)
        # return
        transactions.put((_step.to_json(), r.to_dict()))
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
            del buelon.hub_v1.bucket_client


async def test_run(step_id: str | buelon.core.step.Step | None = None, data=None, mut=None) -> str:
    await test_job(step_id, data, mut=mut)
    return 'done'
    # if not PIPE_WORKER_SUBPROCESS_JOBS:
    #     await asyncio.sleep(0)
    #     await test_job(step_id, data)  #  await asyncio.to_thread(job, step_id, data)
    #     # job(step_id, data)
    #     return 'done'
    # if isinstance(step_id, buelon.core.step.Step):
    #     step_id = step_id.id
    # env = {**os.environ, 'STEP_ID': step_id}
    # p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
    # await p.wait()
    # return 'done'


async def test_get_steps(scopes: list[str], all_types=False):
    steps = await asyncio.wait_for(get_steps(scopes, limit=100, all_types=all_types), timeout=30)

    if not steps:
        print('No steps found')
        return [], {}, {}

    step_objs = await buelon.hub_v1.async_bulk_get_step(steps)
    try:
        parents = [step_id for s in step_objs.values() for step_id in s.parents]
    except AttributeError:  # 'NoneType' object has no attribute 'parents'
        print('Some steps were not found')
        new_steps = []
        parents = []
        for step_id, s in step_objs.items():
            if s is None:
                print(f"Step {step_id} not found")
                await hub_client.reset(step_id)
            else:
                new_steps.append(step_id)
                parents.extend(s.parents)
        steps = new_steps
    step_data = (await buelon.hub_v1.async_bulk_get_data(parents)) if parents else {}

    return steps, step_objs, step_data


async def test_work(steps=None, mut=None, all_types=False):
    start_time = time.time()

    _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', DEFAULT_SCOPES)
    scopes: list[str] = _scopes.split(',') + ['dev']  # _scopes.split(',')
    print('scopes', scopes)
    steps, step_objs, step_data = (steps or await test_get_steps(scopes, all_types=all_types))
    i = 0
    async with asyncio_pool.AioPool(size=25) as pool:
        while steps and (i := i + 1) < 5:
            try:
                # steps, step_objs, step_data = await test_get_steps(scopes)
                next_steps = asyncio.create_task(test_get_steps(scopes, all_types=all_types))

                # cors = []
                for s in steps:
                    await pool.spawn(test_run(step_objs[s], step_data, mut=mut))
                    # cors.append(
                    #     # asyncio.wait_for(
                    #         test_run(step_objs[s], step_data, mut=mut),
                    #         # timeout=try_for_int(step_objs[s].timeout, WORKER_JOB_TIMEOUT)
                    #     # )
                    # )

                # print(f'awaiting {len(cors):,} Jobs')
                # await asyncio.gather(*cors)

                # Force garbage collection
                gc.collect()

                # return await next_steps
                steps, step_objs, step_data = await next_steps
            except KeyboardInterrupt:
                print('Gracefully quitting')


# This function will be the target for each thread
def run_async_in_thread(async_func, *args, **kwargs):
    # Create a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the async function to completion in this thread's event loop
    result = loop.run_until_complete(async_func(*args, **kwargs))

    # Clean up
    loop.close()
    return result


async def test_main(all_types=False):
    """
    Main coroutine to run the worker
    """
    global hub_client
    cleaner_task = asyncio.create_task(cleaner())
    transaction_thread = threading.Thread(target=transaction_worker, daemon=True)
    transaction_thread.start()

    mut = {}

    with hub_client:
        await test_work(mut=mut, all_types=all_types)

        # threads = []
        # for i in range(1):  # Run your function 5 times in separate threads
        #     thread = threading.Thread(target=run_async_in_thread, args=(test_work,), kwargs={'mut': mut})
        #     threads.append(thread)
        #     thread.start()
        #
        # for thread in threads:
        #     thread.join()

        # await asyncio.gather(*[test_work() for i in range(1)])
        # # jobs = ([], {}, {})
        # # i = 0
        # #
        # # while jobs[0] or i == 0:
        # #     jobs = await test_work(jobs)
        # #     i += 1
        # #     if isinstance(jobs, tuple) and len(jobs) and isinstance(jobs[0], list):
        # #         print(f'Returned {len(jobs[0]):,} jobs')

    cleaner_task.cancel()

    transactions.put(None)
    print('waiting on transactions', transactions.qsize())
    transaction_thread.join()
    print('done', transactions.qsize())
    # await asyncio.sleep(10.)


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


def is_old(path):
    return (time.time() - os.path.getmtime(path)) > TEMP_FILE_LIFETIME


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
            and is_old(path))#(time.time() - os.path.getmtime(path)) > TEMP_FILE_LIFETIME)


def file_age(path: str):
    return time.time() - os.path.getmtime(path)


async def clean():
    at_a_time = 50
    i = 0
    for root, dirs, files in os.walk('.'):
        for file in files:
            if is_hanging_script(os.path.join(root, file)):
                try:
                    os.remove(os.path.join(root, file))
                except FileNotFoundError:
                    pass
            i += 1
            if i >= at_a_time:
                i = 0
                await asyncio.sleep(0.01)
        break
    if os.path.exists('./__pycache__'):
        for root, dirs, files in os.walk('./__pycache__'):
            for file in files:
                if is_hanging_script(os.path.join(root, file), '.pyc'):
                    try:
                        os.remove(os.path.join(root, file))
                    except FileNotFoundError:
                        pass
                i += 1
                if i >= at_a_time:
                    i = 0
                    await asyncio.sleep(0.01)
            break

    for temp_dir in ['/tmp', '/var/tmp']:
        if os.path.exists(temp_dir):
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if is_old(os.path.join(root, file)):
                        try:
                            os.remove(os.path.join(root, file))
                        except FileNotFoundError:
                            pass
                    i += 1
                    if i >= at_a_time:
                        i = 0
                        await asyncio.sleep(0.01)
                # break


async def cleaner():
    """
    Cleans up hanging scripts created by the worker that were not properly cleaned up
    """
    while True:
        await clean()
        await asyncio.sleep(60 * 10)  # Run every 10 minutes


def main(all_types=False):
    """
    Main function to run the worker
    """
    asyncio.run(_main(all_types))


async def _main(all_types=False):
    """
    Main coroutine to run the worker
    """
    global hub_client
    cleaner_task = asyncio.create_task(cleaner())
    transaction_thread = threading.Thread(target=transaction_worker, daemon=True)
    transaction_thread.start()

    with hub_client:
        await work(all_types)
    cleaner_task.cancel()

    transactions.put(None)
    print('waiting on transactions', transactions.qsize())
    transaction_thread.join()
    print('done', transactions.qsize())


# try:
#     from cython.c_worker import *
# except (ImportError, ModuleNotFoundError):
#     pass


if __name__ == '__main__':
    main()


