"""
This module provides utility functions for executing Python code and SQL queries dynamically.

It includes functions for running Python code from strings, executing PostgreSQL and SQLite queries,
and handling dynamic variable substitution in code strings.
"""
from __future__ import annotations
import os
import sys
import  tempfile
import importlib
import contextlib
from typing import List, Dict, Any

from buelon.helpers import pipe_util
import buelon.helpers.postgres
import buelon.helpers.sqlite3_helper
from . import pipe_debug


sys.path.append(os.getcwd())


class PipeLineException(Exception):
    """Custom exception for pipeline-related errors."""
    pass


@contextlib.contextmanager
def temp_mod(txt: str):
    """
    Create a temporary Python module from a string.

    Args:
        txt (str): The Python code to be written to the temporary module.

    Yields:
        str: The name of the temporary module.

    Raises:
        PipeLineException: If there's an error in creating or removing the temporary module.
    """
    mod = f'temp_bue_{pipe_util.get_id()}'
    with tempfile.NamedTemporaryFile(prefix=mod, dir=os.getcwd(), suffix='.py') as tf:
        tf.write(txt.encode())
        tf.flush()
        yield tf.name.replace('.py', '').split(os.sep)[-1]
    # path = os.path.join(os.getcwd(), f'{mod}.py')
    # try:
    #     with open(path, 'w') as f:
    #         f.write(txt)
    #     yield mod
    # finally:
    #     try:
    #         os.remove(path)
    #     except:
    #         pass


def fix_data(data: List[Dict]) -> List[Dict]:
    """
    Ensure all dictionaries in a list have the same keys, filling missing keys with None.

    Args:
        data (List[Dict]): A list of dictionaries to fix.

    Returns:
        List[Dict]: The fixed list of dictionaries.
    """
    if isinstance(data, list) and len(data):
        keys = set()
        for row in data:
            keys |= set(row.keys())
        keys = list(data[0].keys()) + list(keys - set(data[0].keys()))
        for row in data:
            for k in keys:
                if k not in row:
                    row[k] = None
    return data


def get_index(k: str) -> tuple[int, str]:
    """
    Extract an index from a string key if present.

    Args:
        k (str): The key string to parse.

    Returns:
        tuple[int, str]: A tuple containing the extracted index (or 0 if not present) and the remaining key.
    """
    if not k.startswith('index['):
        return 0, k
    i, last = '', 0
    for index, c in enumerate(k[len('index['):]):
        last = index
        if c.isdigit():
            i += c
        elif c == ']':
            break
    return int(i), k[last+len('index[')+1:]


def apply_kwargs_to_txt(txt: str, kwargs: dict) -> str:
    """
    Apply keyword arguments to a text string, replacing placeholders.

    Args:
        txt (str): The text containing placeholders.
        kwargs (dict): The keyword arguments to apply.

    Returns:
        str: The text with placeholders replaced by their corresponding values.
    """
    order, skip = {}, {}
    for k, v in kwargs.items():
        i, k = get_index(k)
        if i not in order:
            order[i] = []
        order[i].append((k, v))
        skip[k] = max(skip.get(k, 0), i)
    for i in sorted(order.keys(), reverse=True):
        for k, v in order[i]:
            if skip[k] > i:
                continue
            txt = txt.replace('{{'+k+'}}', f'{v}')
    return txt


def place_null_values(txt: str) -> str:
    """
    Replace unfilled placeholders with a null value string.

    Args:
        txt (str): The text to process.

    Returns:
        str: The processed text with unfilled placeholders replaced.
    """
    return txt  # Currently a no-op, implement as needed


def get_end(txt: str, start: int, end_token: str) -> int:
    """
    Find the end index of a token in a string.

    Args:
        txt (str): The text to search.
        start (int): The starting index for the search.
        end_token (str): The token to search for.

    Returns:
        int: The index of the end token, or -1 if not found.
    """
    for i in range(start, len(txt)):
        if txt[i:i+len(end_token)] == end_token:
            return i
    return -1


def check_for_uuid_kwargs(txt: str, kwargs: dict) -> tuple[str, dict]:
    """
    Check for UUID placeholders in text and replace them with generated UUIDs.

    Args:
        txt (str): The text to process.
        kwargs (dict): Existing keyword arguments.

    Returns:
        tuple[str, dict]: The processed text and a dictionary of new UUID values.

    Raises:
        PipeLineException: If a UUID placeholder is not properly closed.
    """
    n = {}
    for start_token, end_token in [('{{uuid|', '}}'), ('{{uuid:', '}}')]:
        if start_token in txt:
            while start_token in txt:
                s = txt.index(start_token)
                e = get_end(txt, s, end_token)
                if e == -1:
                    raise PipeLineException('uuid not closed')
                _id = txt[s + len(start_token):e]
                key = f'uuid_{_id}'
                if key not in kwargs:
                    kwargs[key] = pipe_util.get_id()
                    n[key] = kwargs[key]
                v = kwargs[key]
                txt = txt[:s] + v + txt[e+2:]
    return txt, n


def check_py_output(data: Any) -> Any:
    """
    Check if the output of a Python execution is of a valid type.

    Args:
        data: The data to check.

    Returns:
        The input data if it's of a valid type.

    Raises:
        PipeLineException: If the data is of an invalid type.
    """
    if not isinstance(data, (list, dict, int, float, str, bool, type(None))):
        raise PipeLineException(f'invalid output type {type(data)} must be: (list, dict, int, float, str, bool, type(None))')
    return data


@pipe_debug.timeit
def run_py(txt: str, func: str, *args, __pure__=False, **kwargs) -> Any:
    """
    Run Python code from a string.

    Args:
        txt (str): The Python code to run.
        func (str): The name of the function to call in the code.
        *args: Positional arguments to pass to the function.
        __pure__ (bool): If True, return the raw output of the function.
        **kwargs: Keyword arguments to apply to the code.

    Returns:
        The result of the function execution, processed based on the __pure__ flag.

    Raises:
        PipeLineException: If the specified function is not found in the code.
    """
    txt = apply_kwargs_to_txt(txt, kwargs)
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    txt = place_null_values(txt)

    with temp_mod(txt) as mod:
        module = importlib.import_module(mod)
        if hasattr(module, func):
            r = getattr(module, func)(*args)
            return r
        else:
            raise PipeLineException(f'function {func} not found.')
    return True, []


@pipe_debug.timeit
def run_postgres(txt: str, func: str, *args, **kwargs) -> Any:
    """
    Run a PostgreSQL query.

    Args:
        txt (str): The SQL query to run.
        func (str): The name of the function (used for table naming).
        *args: Data to be uploaded to temporary tables.
        **kwargs: Additional keyword arguments.

    Returns:
        The result of the SQL query execution.

    Raises:
        PipeLineException: If there's an error in query execution.
    """
    db: buelon.helpers.postgres.Postgres = buelon.helpers.postgres.get_postgres_from_env()
    tables = [pipe_util.get_id() for v in args]
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    try:
        for i, data in enumerate(args):
            if data:
                n = func if i == 0 else f'pipe{i}'
                if n in txt:
                    try:
                        db.download_table(sql=f'DROP TABLE {tables[i]};')
                    except:
                        pass
                    db.upload_table(tables[i], data)
                    txt = txt.replace(n, tables[i])
        txt = apply_kwargs_to_txt(txt, {**kwargs})
        v = db.download_table(sql=txt)
        return v
    finally:
        for table in tables:
            try:
                db.download_table(f'DROP TABLE {table};')
            except: pass


@pipe_debug.timeit
def run_sqlite3(txt: str, func: str, *args, **kwargs) -> Any:
    """
    Run a SQLite query.

    Args:
        txt (str): The SQL query to run.
        func (str): The name of the function (used for table naming).
        *args: Data to be uploaded to temporary tables.
        **kwargs: Additional keyword arguments.

    Returns:
        The result of the SQL query execution.
    """
    db = buelon.helpers.sqlite3_helper.Sqlite3()
    tables = [pipe_util.get_id() for v in args]
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    try:
        for i, data in enumerate(args):
            n = func if i == 0 else f'pipe{i}'
            if n in txt:
                try:
                    db.query(f'DROP TABLE {tables[i]};')
                except:
                    pass
                db.upload_table(tables[i], data)
                txt = txt.replace(n, tables[i])
        txt = apply_kwargs_to_txt(txt, {**kwargs})
        return db.download_table(sql=txt)
    finally:
        try:
            for table in tables:
                db.query(f'DROP TABLE {table};')
        except:
            pass

