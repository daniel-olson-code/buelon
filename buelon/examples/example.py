import os
import time
import sqlite3
import uuid
import random
import json
import sys

import buelon.hub
import buelon.helpers.sqlite3_helper
import buelon.core.step

import buelon.core.pipe_debug


counter = buelon.core.pipe_debug.counter


def accounts(*args):
    """
    a function that returns a list of accounts

    Returns:
        a list of accounts
    """
    # time.sleep(random.randint(1, 5))

    return [
        {'name': 'mr. business', 'id': 123},
        {'name': 'mrs. business', 'id': 456},
        {'name': 'sr. business', 'id': 789}
    ]


def request_report(data: dict) -> dict:
    """
    a function that requests a report

    Args:
        data: the data to include in the report

    Returns:
        the data with a report_id added
    """
    # time.sleep(random.randint(1, 5))

    report_id = f'{uuid.uuid4()}'
    return {**data, 'report_id': report_id}


def get_status(data: dict) -> tuple[buelon.core.step.StepStatus, dict] | dict:
    """
    a function that returns the status of a report

    Args:
        data: the data containing the report_id

    Returns:
        the status of the report
    """
    # time.sleep(random.randint(1, 5))

    if data.get('status') == 'failed':
        return buelon.core.step.StepStatus.cancel, data

    if data.get('status') == 'report deleted':
        return buelon.core.step.StepStatus.reset, data

    if counter(data['report_id']) < random.randint(3, 25):
        return buelon.core.step.StepStatus.pending, data

    return data


def get_report(data: dict) -> list[dict]:
    """
    a function that returns a report

    Args:
        data: the data containing the report_id

    Returns:
        a list of dictionaries representing the report
    """
    # time.sleep(random.randint(1, 5))

    return [{**data, 'sales': 100 * (13 % i), 'spend': 50 * (9 % i)}
            for i in range(1, 50)]


def upload_to_db(data: list[dict]) -> None:
    """
    a function that uploads data to a database

    Args:
        data: the data to upload
    """
    # time.sleep(random.randint(1, 5))

    db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
    db.upload_table('test', data)
    counter('done1')
# remove start

def setup():
    # Imported here rather than at the top: everything between the `# remove`
    # markers is stripped out of the copy handed to the user, so a top-level
    # import would leave them an unused one.
    import buelon.settings

    pipe_path = os.path.join(os.getcwd(), 'example.bue')
    example_py_path = os.path.join(os.getcwd(), 'example.py')
    demo_py_path = os.path.join(os.getcwd(), 'demo.py')

    # This used to write a `.env` of PIPELINE_HOST / PIPE_WORKER_* / BUCKET_* /
    # POSTGRES_* variables. Nothing on the hub/worker path has read any of them
    # since configuration moved to `.boo/settings.yaml`, so it taught a new user
    # the wrong model -- and it wrote unconditionally, clobbering a real `.env`.
    # Leave them the file that is actually read instead. BUGS.md #44.
    buelon.settings.init()

    files_to_copy = [pipe_path, example_py_path, demo_py_path]

    for path in files_to_copy:
        if not os.path.exists(path):
            with open(os.path.join(os.path.dirname(__file__), os.path.basename(path))) as f:
                txt = f.read()

            start = '# remove'' start'
            end = '# remove'' end'

            while start in txt and end in txt:
                if txt.index(start) > txt.index(end):
                    raise ValueError(f'Invalid remove section in '
                                     f'f: {os.path.basename(path)}, '
                                     f's: {txt.index(start)}, '
                                     f'e: {txt.index(end)}')
                txt = (txt[:txt.index(start)]
                       + txt[txt.index(end) + len(end):])

            with open(path, 'w') as f:
                f.write(txt)

# remove end

def main():
    pipe_path = os.path.join(os.getcwd(), 'example.bue')

    try:
        db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
        t = db.download_table('test')
    except sqlite3.OperationalError:
        t = []

    l1 = len(t)

    number_of_jobs = 1

    print('the table test currently has', l1, 'rows')
    try:
        for i in range(number_of_jobs):
            # `upload_pipe_code_from_file` has not existed on `buelon.hub` for a
            # long time -- this raised `AttributeError`, not a hub error. BUGS.md #44.
            buelon.hub.upload_file_to_server(pipe_path)
    except ConnectionRefusedError:
        raise ConnectionRefusedError('please start a hub and a worker first: '
                                     '`bue hub` in one terminal, `bue worker` in another')

    print(f'waiting waiting until all tasks finish')
    while len(accounts()) * number_of_jobs > (c := counter('done1', 0)):
        time.sleep(.1)

    db = buelon.helpers.sqlite3_helper.Sqlite3('test.db')
    t = db.download_table('test')
    print('the table test now has', len(t), 'rows. A difference of', len(t) - l1)

    counter_table = [row for row in db.download_table('counter') if row['id'] != 'done']
    print('take a look that the counter', json.dumps(counter_table, indent=4), 'is updated')

    db.query('delete from counter where 1=1;')


if __name__ == '__main__':
    main()





