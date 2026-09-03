"""End-to-end demo of buelon.

Starts a hub and three workers, uploads `example.bue`, waits for the pipeline to
drain, prints what every job ended up as, then shuts everything down.

Run it with `bue demo`, or -- after `bue example` has dropped the files into the
current directory -- with `python demo.py`.
"""
import os
import shutil
import socket
import multiprocessing
import time

import buelon.hub
from buelon.settings import settings
from buelon.worker import run_worker
from buelon.hub import run_server as run_hub


PIPELINE_FILE = 'example.bue'

# `example.bue`'s python jobs name `example.py` as their code, and a job's file is
# opened by the *worker*, relative to its cwd. So both files have to sit in the
# directory the demo runs from.
REQUIRED_FILES = [PIPELINE_FILE, 'example.py']

NUMBER_OF_WORKERS = 3

# A job is finished when it reaches one of these. 'unknown' is in the list because
# the hub drops a whole pipeline -- jobs, statuses and results -- as soon as every
# job in it is done, so a job that stops being tracked has finished successfully.
FINISHED_STATUSES = {'success', 'error', 'cancel', 'unknown'}

POLL_INTERVAL = 0.25

HUB_START_TIMEOUT = 30.0
PIPELINE_TIMEOUT = float(os.environ.get('BUELON_DEMO_TIMEOUT', 600.0))


def attempted_to_kill(process: multiprocessing.Process) -> None:
    """Attempts to kill a process gracefully, then forcefully if necessary.

    Args:
        process: The multiprocessing.Process to kill.

    Returns:
        None
    """
    if not process.is_alive():
        return

    process.terminate()
    process.join(timeout=5)  # Wait for 5 seconds
    if process.is_alive():
        process.kill()  # Force kill if process doesn't terminate in time
        process.join(timeout=5)


def example_source_dir() -> str:
    """Where the pristine copies live.

    Resolved through the package rather than through `__file__`, so this still works
    from the copy `bue example` drops in your cwd -- there `__file__`'s directory IS
    the cwd, and there would be nothing to copy from. Imported here rather than at
    the top because `buelon.examples.__init__` imports this module.
    """
    import buelon.examples
    return os.path.dirname(buelon.examples.__file__)


def copy_example_files() -> None:
    """Put `example.bue` and `example.py` in the cwd if they are not there already.

    Existing files are never overwritten -- the point of `bue example` is that you
    can edit them and run the demo against your edits.
    """
    for name in REQUIRED_FILES:
        if os.path.exists(name):
            continue
        shutil.copyfile(os.path.join(example_source_dir(), name), name)
        print(f'copied {name} into {os.getcwd()}')


def hub_is_listening() -> bool:
    """True if something is already accepting connections on the hub's port."""
    try:
        with socket.create_connection((settings.worker.host, settings.worker.port), timeout=1):
            return True
    except OSError:
        return False


def wait_for_hub(process: multiprocessing.Process) -> None:
    """Block until the hub accepts connections, or give up."""
    deadline = time.time() + HUB_START_TIMEOUT

    while time.time() < deadline:
        if not process.is_alive():
            raise RuntimeError(f'the hub exited before it started listening '
                               f'(exit code {process.exitcode})')
        if hub_is_listening():
            return
        time.sleep(0.1)

    raise TimeoutError(f'the hub did not start listening on '
                       f'{settings.worker.host}:{settings.worker.port} '
                       f'within {HUB_START_TIMEOUT}s')


def wait_for_jobs(jobs: list) -> dict:
    """Poll the hub until every job in `jobs` is finished. Returns id -> status.

    Polling for a job stops at its first finished status, so a job we saw succeed
    keeps `success` even though the hub cleared it a moment later.
    """
    job_ids = [job.id for job in jobs]
    statuses = {job_id: 'unknown' for job_id in job_ids}
    deadline = time.time() + PIPELINE_TIMEOUT
    outstanding = set(job_ids)

    while outstanding and time.time() < deadline:
        for job_id, status in buelon.hub.check_job_status_bulk(sorted(outstanding)).items():
            statuses[job_id] = status
            if status in FINISHED_STATUSES:
                outstanding.discard(job_id)
        if outstanding:
            time.sleep(POLL_INTERVAL)

    return statuses


def report(jobs: list, statuses: dict) -> int:
    """Print what every job ended up as. Returns the process exit code."""
    counts: dict[str, int] = {}

    print('')
    for job in jobs:
        status = statuses.get(job.id, 'unknown')
        # The hub clears a pipeline -- jobs, statuses and results -- once every job in
        # it has succeeded, so a job it no longer knows about is one that finished
        # before we last asked. Say that rather than 'unknown'.
        status = 'cleared' if status == 'unknown' else status
        counts[status] = counts.get(status, 0) + 1
        print(f'{status:>10}  {job.name}  ({job.id})')

    print('')
    print(', '.join(f'{status}: {count}' for status, count in sorted(counts.items())))

    if counts.get('cleared'):
        print('(`cleared` means the job succeeded and the hub has already dropped it: '
              'it clears a pipeline once every job in it is done.)')

    unfinished = sum(count for status, count in counts.items()
                     if status not in FINISHED_STATUSES and status != 'cleared')
    if unfinished:
        print(f'\n{unfinished} job(s) never finished within {PIPELINE_TIMEOUT}s.')
        return 1

    if counts.get('error'):
        print('')
        buelon.hub.display_errors_from_server()
        return 1

    print('\nDemo complete.')
    return 0


def main() -> int:
    """Runs the demo pipeline system.

    Starts a hub and `NUMBER_OF_WORKERS` workers, uploads `example.bue`, waits for
    the resulting jobs to finish and prints their statuses. Always tears the
    processes back down.

    Returns:
        int: 0 if every job succeeded, 1 otherwise.
    """
    copy_example_files()

    hub_process = None
    worker_processes = []

    try:
        if hub_is_listening():
            print(f'a hub is already listening on {settings.worker.host}:'
                  f'{settings.worker.port} -- using it')
        else:
            hub_process = multiprocessing.Process(target=run_hub)
            hub_process.start()
            wait_for_hub(hub_process)
            print(f'hub listening on {settings.hub.host}:{settings.hub.port}')

        for _ in range(NUMBER_OF_WORKERS):
            process = multiprocessing.Process(target=run_worker)
            process.start()
            worker_processes.append(process)
        print(f'started {len(worker_processes)} workers')

        # Building the job graph runs the `for` loop's source pipe locally, so this
        # is also the first thing that would tell us `example.bue` no longer parses.
        jobs = buelon.hub.upload_file_to_server(PIPELINE_FILE, return_jobs=True)
        print(f'uploaded {PIPELINE_FILE}: built {len(jobs)} jobs')

        return report(jobs, wait_for_jobs(jobs))
    finally:
        print('Clean up')
        for process in worker_processes:
            attempted_to_kill(process)
        if hub_process is not None:
            attempted_to_kill(hub_process)


if __name__ == "__main__":
    raise SystemExit(main())
