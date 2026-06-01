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
DOT_ENV_CONTENT = '''
PIPELINE_HOST=0.0.0.0
PIPELINE_PORT=65432

PIPE_WORKER_HOST=localhost
PIPE_WORKER_PORT=65432
PIPE_WORKER_SCOPES=production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small
PIPE_WORKER_SUBPROCESS_JOBS=true

BUCKET_SERVER_HOST=0.0.0.0
BUCKET_SERVER_PORT=61535

BUCKET_CLIENT_HOST=localhost
BUCKET_CLIENT_PORT=61535

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=daniel
POSTGRES_PASSWORD=Password123
POSTGRES_DATABASE=my_database

'''


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
    pipe_path = os.path.join(os.getcwd(), 'example.bue')
    example_py_path = os.path.join(os.getcwd(), 'example.py')
    demo_py_path = os.path.join(os.getcwd(), 'demo.py')
    dot_env_path = os.path.join(os.getcwd(), '.env')

    with open(dot_env_path, 'w') as f:
        f.write(DOT_ENV_CONTENT)

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
            buelon.hub.upload_pipe_code_from_file(pipe_path)
    except ConnectionRefusedError:
        raise ConnectionRefusedError(f'please start the moss.hub first by running "{sys.executable} demo.py"')

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





