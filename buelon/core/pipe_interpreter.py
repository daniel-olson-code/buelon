"""Module for parsing and executing a custom pipeline language.

This module provides functionality to parse and execute code written in a custom
pipeline language. It includes utilities for handling scopes, steps, pipes, actions,
and loops within the custom language.

Constants:
    INDENT (str): A string constant representing four spaces.
    PIPE_TOKENS (dict): A dictionary defining various tokens used in the pipeline language.
    LANGUAGES (dict): A dictionary mapping language names to their standardized identifiers.
"""
import sys
import inspect
import string
import sqlite3
import json
import traceback
import enum
import threading
import tempfile
from typing import Dict, Any, Generator

import yaml

# import buelon.core.step
from buelon.helpers import pipe_util, lazy_load_class
import buelon.helpers.pipe_util
import buelon.helpers.persistqueue
from . import step_definition
from . import pipe
from . import action
from . import loop
from . import step

# Constants
INDENT = '    '
PIPE_TOKENS = {
    'create job def': 'import',
    'pipe': '|',
    'scope': '!scope',
    'priority': '!priority',
    'job args': {
        'scope': {'type': 'string'},
        'priority': {'type': 'int'},
        'timeout': {'type': 'calculate'},
        'retries': {'type': 'int'}
    }
}


LANGUAGES = {
    'python': step.LanguageTypes.python.value,
    'python3': step.LanguageTypes.python.value,
    'py': step.LanguageTypes.python.value,
    'pg': step.LanguageTypes.postgres.value,
    'postgres': step.LanguageTypes.postgres.value,
    'postgresql': step.LanguageTypes.postgres.value,
    'sqlite3': step.LanguageTypes.sqlite3.value,
    'sqlite': step.LanguageTypes.sqlite3.value
}


def delete_last_line():
    sys.stdout.write("\033[F")
    sys.stdout.write("\033[K")


def calculate(s: str):
    if not valid_word(s, '.0123456789+-*/%() '):
        raise ValueError(f"Invalid expression: {s}")
    return eval(s, {}, {})


def check_for_scope(index: int, line: str, variables: Dict) -> bool:
    """Check if the current line defines a scope and update variables accordingly.

    Args:
        index (int): The current line index.
        line (str): The current line of code.
        variables (dict): A dictionary containing pipeline variables.

    Returns:
        bool: True if a scope was found and processed, False otherwise.

    Raises:
        SyntaxError: If the scope syntax is invalid.
    """
    token = PIPE_TOKENS['scope']
    if line.strip().startswith(token):
        if line.count(token) > 1:
            raise SyntaxError(f'Line {index+1}: Invalid scope')
        scope = line.split(token)[1].strip()
        if not scope:
            raise SyntaxError(f'Line {index+1}: Invalid scope')
        variables['__scope__'] = scope
        return True
    return False



def run(code: str, lazy_steps: bool = False) -> Dict:
    """Parse and execute the given pipeline code.

    Args:
        code (str): The pipeline code to be executed.
        lazy_steps (bool, optional): Whether to use lazy loading for steps. Defaults to False.

    Returns:
        dict: A dictionary containing the parsed pipeline variables.

    Raises:
        Exception: If an error occurs during parsing or execution.
    """
    try:

        if lazy_steps:
            print('Lazy Steps')
            variables = lazy_load_class.LazyMap()
            lm = lazy_load_class.LazyMap()
            lm.disable_deletion()
            variables['__steps__'] = lm
        else:
            variables = {
                '__steps__': {}
            }
        variables['__starters__'] = []
        variables['__scope__'] = 'default'

        configurations = {
            'importing': False,
            'current import': None,
            'in loop': False,
            'in code block': False,
        }

        if code.strip().startswith('{'):
            try:
                i, j = pipe_util.extract_json(code)
                kwargs = json.loads(j)
                code = code[i:]
            except Exception as e:
                print(e)

        def remove_comments(line: str) -> str:
            """Remove comments from a line of code.

            Args:
                line (str): A line of code.

            Returns:
                str: The line with comments removed.
            """
            return line.split('#')[0]

        def set_index(_i: int) -> None:
            """Set the current line index.

            This function is used to update the line index, typically when
            processing loops or other constructs that may alter the normal
            flow of line-by-line execution.

            Args:
                _i (int): The new line index to set.
            """
            nonlocal i
            i = _i

        def read_line(index: int) -> None:
            """Process a single line of the pipeline code.

            Args:
                index (int): The index of the current line.
            """
            nonlocal i
            line = lines[index]

            # else:
            if step_definition.check_for_step_definition(i, line, variables, configurations):
                return

            if check_for_scope(i, line, variables):
                return

            if pipe.check_for_pipe(i, line, variables):
                return

            if action.check_for_actions(i, line, variables):
                return

            if loop.check_loop(i, line, lines, variables, read_line, set_index):
                return

            if line.strip():
                raise SyntaxError(f'Line {i+1}: Invalid syntax. `{line}`')

        lines = [remove_comments(line) for line in code.splitlines()]
        i = 0
        while i < len(lines):
            read_line(i)
            i += 1

        return variables

    except Exception as e:
        traceback.print_exc()
        print(f'Unexpected Error: {type(e).__name__} | {e}')
        exit()


def get_steps_from_code(code: str, lazy_steps: bool = False) -> Dict[str, Any]:
    """DEPRECATED: Use generate_steps_from_code() instead.

    Extract steps and starters from the given pipeline code.

    Args:
        code (str): The pipeline code to be processed.
        lazy_steps (bool, optional): Whether to use lazy loading for steps. Defaults to False.

    Raises:
        Exception: If an error occurs during parsing or execution.

    Returns:
        dict: A dictionary containing the extracted steps and starters.
    """
    import warnings
    warnings.warn("`buelon.core.pipe_interpreter.get_steps_from_code(code, lazy_steps)`. Use `buelon.core.pipe_interpreter.generate_steps_from_code(code)`.",
                  DeprecationWarning,
                  stacklevel=2)
    variables = run(code, lazy_steps=lazy_steps)
    return {'steps': variables['__steps__'], 'starters': variables['__starters__'], 'variables': variables}


allowed_chars = string.ascii_letters + string.digits + '_- '


# scope = 'default'
# priority = 0


class BuelonSyntaxError(Exception):
    pass


class BuelonBuildError(Exception):
    pass


def name_warning(name: str):
    if '  ' in name:
        print(f'Warning: `{name}` contains consecutive spaces.')


def indexes(line: str, char: str):
    r = '^' if char != '^' else '%'
    indexes = []
    while char in line:
        index = line.index(char)
        indexes.append(index)
        line = line.replace(char, '^', 1)
    return indexes


def valid_word(word: str, _allowed_chars: str | set | list | tuple | frozenset | dict | None = None) -> bool:
    if not isinstance(_allowed_chars, (str, set, list, tuple, frozenset, dict)):
        _allowed_chars = allowed_chars
    if not word:
        return False
    return all(char in _allowed_chars for char in word)


def line_is_execution(line: str):
    if (not line.startswith(' ') and line.count('(') == 1 and line.count(')') == 1 and line.index('(') < line.index(')')
            and not line.strip().startswith('for ')):
        if '=' in line and line.count('=') == 1:
            _, end = line.split('=')
            begin, end = end.split('(')
            if not begin.strip():
                return False
        return True
    return False


# steps, pipes = create_pipeline_objects(content)

# print('steps', steps)
# print('pipes', pipes)


class PipelineParser:
    """
    TODO: check for variable passing and assignment on parse
    """
    scope: str = 'default'
    priority: int = 0
    args: dict | None = None
    conn = None
    tab = '    '
    _build_index = 0

    def __init__(self, scope: str = 'default', priority: int = 0):
        self.scope = scope
        self.priority = priority
        self.args = {
            'scope': self.scope,
            'priority': self.priority,
            'timeout': 20 * 60,
            'retries': 0
        }

    def build(self, prepared_content: str):
        with tempfile.NamedTemporaryFile(mode='w', dir='.bue', suffix='.db') as temp_file:
            self.conn = sqlite3.connect(temp_file.name)  # ('.bue/test.db')  # (':memory:')
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=NORMAL')
            self.conn.execute('DROP table if exists jobs;')
            self.conn.execute('CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, starter, value)')
            self.conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_id ON jobs (id);')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_starter_id ON jobs (starter, id);')
            self.conn.commit()
            self._build_index = 0
            print(f'{self._build_index:,} compiles')
            i = 0
            # for job in self._build(yaml.load(prepared_content, yaml.Loader)):
            #     i += 1
            #     if job.parents:
            #         yield job
            #     else:
            #         self.conn.execute("INSERT INTO jobs (id, starter, value) VALUES (?, ?, ?)",
            #                           (job.id, not job.parents, json.dumps(job.to_json())))
            self._build(yaml.load(prepared_content, yaml.Loader))

            # print('deploying initiators')
            print(f"Built {self.conn.execute('SELECT count(*) FROM jobs').fetchall()[0][0]:,} jobs")
            q = 'SELECT starter, value FROM jobs order by starter, id limit {limit} offset {offset}'
            limit = 500
            offset = 0
            chunk = self.conn.execute(q.format(limit=limit, offset=offset)).fetchall()
            while chunk:
                for starter, job_json in chunk:
                    i += 1
                    yield step.Step().from_json(json.loads(job_json))
                offset += limit
                chunk = self.conn.execute(q.format(limit=limit, offset=offset)).fetchall()

            # print(f'{i:,} jobs')
            # for _id, s, j in self.conn.execute('SELECT * FROM jobs where starter').fetchall():
            #     print(step.Step().from_json(json.loads(j)).name)
            # print(f"Saved {self.conn.execute('SELECT count(*) FROM jobs').fetchall()[0][0]:,} jobs")
            # print(self.conn.execute('SELECT starter, count(*) FROM jobs group by starter').fetchall())
            # t = time.time()
            # q = 'SELECT starter, value FROM jobs order by starter, id limit {limit} offset {offset}'
            # limit = 1000
            # offset = 0
            # chunk = self.conn.execute(q.format(limit=limit, offset=offset)).fetchall()
            # while chunk:
            #     for starter, job_json in chunk:
            #         # yield starter, step.Step().from_json(json.loads(job_json))
            #         _ = step.Step().from_json(json.loads(job_json))
            #         del _
            #     offset += limit
            #     chunk = self.conn.execute(q.format(limit=limit, offset=offset)).fetchall()
            # print(f'{time.time() - t:0.2f} sec(s)')
            # yield '', ''

    def run(self, prepared_content: str):
        with tempfile.NamedTemporaryFile(mode='w', dir='.bue', suffix='.jsonl') as temp_file:
            q = buelon.helpers.persistqueue.JsonPersistentQueue(temp_file.name)
            data = {}  # buelon.helpers.lazy_load_class.LazyMap()

            def run(job: buelon.core.step.Step):
                # job = self._get_job(job_id)
                job_id = job.id
                r: buelon.core.step.Result = job.run(*(data[parent] for parent in job.parents))
                data[job_id] = r.data

                if r.status == buelon.core.step.StepStatus.success:
                    for child in job.children:
                        q.put(child)
                elif r.status == buelon.core.step.StepStatus.pending:
                    q.put(job_id)
                elif r.status == buelon.core.step.StepStatus.reset:
                    parents = job.parents.copy()
                    while parents:
                        p = parents.pop(0)
                        j = self._get_job(p)
                        if not j.parents:
                            q.put(p)
                        else:
                            parents.extend(j.parents)

            for job in self.build(prepared_content):
                if not job.parents:
                    q.put(job.id)

            while q.qsize():
                job_id = q.get()
                job = self._get_job(job_id)
                run(job)

    def _get_job(self, job_id: str) -> step.Step:
        job_json = self.conn.execute("SELECT value FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]
        return step.Step().from_json(json.loads(job_json))

    def _run_job(self, job: step.Step | str) -> None:
        if isinstance(job, str):
            job = self._get_job(job)

        args = job.parents

        if args:
            args = [self._run_job(arg) for arg in args]

        return job.run(*args).data

    def _build(self, variables: dict, values: dict | None = None):
        self._build_index += 1
        delete_last_line()
        print(f'{self._build_index:,} compiles')
        # varibales = {
        #     'scope': self.scope,
        #     'priority': self.priority,
        #     'job_definitions': job_definitions,
        #     'pipes': pipes,
        #     'executions': executions,
        #     'loops': loops,
        # }
        # {#pipes
        #     'name': name,
        #     'jobs': jobs
        # }
        # {#executions
        #     'result': result,
        #     'name': name,
        #     'args': args
        # }
        # {#loops
        #     'name': var,
        #     'pipe_execution': pipe_exection,
        #     'varibales': varibales,
        # }
        # {#job_definitions
        #     'name': name,
        #     'language': LANGUAGES[language],
        #     'relation': relation,
        #     'code': code,
        #     'local': local,
        #     'scope': scope,
        #     'priority': priority,
        # }
        values = values or {}

        for execution in variables['executions']:
            # for job in self.build_execution(execution, values, variables):
            #     yield job
            self.build_execution(execution, values, variables)
        for loop in variables['loops']:
            # for job in self.build_loop(loop, values, variables):
            #     yield job
            self.build_loop(loop, values, variables)

    def merge_variables(self, a: dict, b: dict) -> dict:
        # varibales = {
        #     'scope': self.scope,
        #     'priority': self.priority,
        #     'job_definitions': job_definitions,
        #     'pipes': pipes,
        #     'executions': executions,
        #     'loops': loops,
        # }
        merged = {}
        merged['args'] = {**b['args'], **a['args']}
        merged['scope'] = a['scope']
        merged['priority'] = a['priority']
        merged['job_definitions'] = a['job_definitions'] + b['job_definitions']
        merged['pipes'] = a['pipes'] + b['pipes']
        merged['executions'] = a['executions'] + b['executions']
        merged['loops'] = a['loops'] + b['loops']
        return merged

    def build_loop(self, loop, values, variables):
        # {#loops
        #     'name': var,
        #     'pipe_execution': pipe_exection,
        #     'varibales': varibales,
        # }
        # {#job_definitions
        #     'name': name,
        #     'language': LANGUAGES[language],
        #     'relation': relation,
        #     'code': code,
        #     'local': local,
        #     'scope': scope,
        #     'priority': priority,
        # }
        # {#pipes
        #     'name': name,
        #     'jobs': jobs
        # }
        # {#executions
        #     'result': result,
        #     'name': name,
        #     'args': args
        # }
        local = True

        if local:
            # jobs = next(self.build_execution(loop['pipe_execution'], values, variables, store_result=False))
            jobs = self.build_execution(loop['pipe_execution'], values, variables, store_result=False)

            # if jobs[0].parents:
            #     print(jobs[0].parents)
            #     print('values', values)
            #     print('data', len(list(self._run_job(jobs[-1]))))
            #     # TODO: fix this. This only works for linear job pipes and not a pipe with arguments
            #     raise BuelonBuildError(f'Loop `{loop["name"]}`. '
            #                            f'Currently only pipe loops with no arguments are supported. '
            #                            f'Example: only --> `\npipe = job1 | job2 | job3\n\nfor v in pipe():'
            #                            f'\n{self.tab}any_pipe(v)`')

            # data = []
            #
            # for job in jobs:
            #     new_data = job.run(data).data
            #     del data
            #     del job
            #     data = new_data

            data = self._run_job(jobs[-1])  # .run().data

            if not isinstance(data, list) and not inspect.isgenerator(data):
                raise BuelonBuildError(f'Loop `{loop["name"]}` must return a list or a generator. Returned `{type(data)}`')

            i = 0
            for value in data:
                job_def = self.job_for_loop(i, value, loop, variables)
                pipe = {
                    'name': loop["name"],
                    'jobs': [job_def['name']]
                }
                execution = {
                    'result': loop['name'],
                    'name': loop["name"],
                    'args': []
                }
                merged = {
                    'scope': job_def['scope'],
                    'priority': job_def['priority'],
                    'job_definitions': [job_def],
                    'pipes': [pipe],
                    'executions': [execution],
                    'loops': [],
                    'args': job_def['args'],
                }

                merged = self.merge_variables(merged, loop['varibales'])
                v = json.loads(json.dumps(variables))
                v['loops'] = []
                v['executions'] = []
                merged = self.merge_variables(merged, v)
                # for job in self._build(merged, values=values):
                #     yield job
                self._build(merged, values=values)
                i += 1

    def job_for_loop(self, i, value, loop, variables):
        # self.job_from_definition
        _pipe = self.get_pipe(loop['pipe_execution']['name'], variables)
        loop_job_def = self.get_job_definition(_pipe['jobs'][-1], variables)
        priority: int = loop_job_def['priority']
        scope: str = loop_job_def['scope']
        return {
            'name': f'{loop["name"]}_{i}',
            'language': 'PYTHON',
            'local': False,
            'code': f'import json\ndef main(*args):\n    return json.loads({json.dumps(json.dumps(value))})',
            'relation': 'main',
            'priority': priority,
            'scope': scope,
            'args': json.loads(json.dumps(loop_job_def['args'])),
            'result': None
        }

    def build_execution(self, execution, values, variables, append=None, store_result=True):
        for arg in execution['args']:
            if arg not in values:
                raise BuelonBuildError(
                    f'Argument `{arg}` not found. arguments can only by execution asignments. Example: `arg = pipe()`')

        jobs = []
        pipe = self.get_pipe(execution['name'], variables)
        # parents = [values[arg] for arg in execution['args']]
        parents = []

        first_job_id = buelon.helpers.pipe_util.get_id()

        for arg in execution['args']:
            job_id = values[arg]
            parents.append(job_id)
            job_json = self.conn.execute(
                'select value from jobs where id = ?', (job_id,)
            ).fetchall()[0][0]
            job = step.Step().from_json(json.loads(job_json))
            job.children.append(first_job_id)
            self.conn.execute("UPDATE jobs SET value = ? WHERE id = ?", (json.dumps(job.to_json()), job.id))
            self.conn.commit()

        last_job = None

        for job_name in pipe['jobs']:
            job_def = self.get_job_definition(job_name, variables)
            job = self.job_from_definition(job_def, provide_id=first_job_id)
            first_job_id = None
            jobs.append(job)
            job.parents = parents
            job.children = []
            if last_job:
                last_job.children.append(job.id)
            last_job = job
            parents = [job.id]

        # if append:
        #     jobs.append(job)
        #     job.parents = parents
        #     job.children = []
        #     if last_job:
        #         last_job.children.append(job.id)
        #     last_job = job

        if last_job and execution['result']:
            values[execution['result']] = last_job.id

        if not store_result:
            return jobs
            # yield jobs
        else:
            for job in jobs:
                # yield job
                self.conn.execute("INSERT INTO jobs (id, starter, value) VALUES (?, ?, ?)", (job.id, not job.parents, json.dumps(job.to_json())))
            self.conn.commit()

    def get_pipe(self, name: str, variables: dict):
        for pipe in variables['pipes']:
            if pipe['name'] == name:
                return pipe
        raise BuelonBuildError(f'Pipe `{name}` not found.')

    def get_job_definition(self, name: str, variables: dict):
        for job in variables['job_definitions']:
            if job['name'] == name:
                return job
        raise BuelonBuildError(f'Job `{name}` not found.')

    def job_from_definition(self, definition: dict, env=None, provide_id: str | None = None):
        job = step.Step()
        job.id = provide_id if provide_id else buelon.helpers.pipe_util.get_id()
        job.name = definition['name']
        job.type = definition['language']
        job.local = definition['local']
        job.code = definition['code']
        job.func = definition['relation']
        job.kwargs = env or {}
        job.priority = definition['args']['priority']  # ['priority']
        # job.velocity = self.velocity
        # job.tag = self.tag
        job.scope = definition['args']['scope']  # ['scope']
        job.timeout = definition['args']['timeout']
        job.retries = definition['args']['retries']
        job.kwargs = definition['args']
        return job

    def prepare(self, content: str):
        return yaml.dump(self.parse(content))

    def get_line_at_index(self, index: int, lines: list[str]):
        if 0 > index >= len(lines):
            raise BuelonSyntaxError(f'Error trying to read line: {index + 1}')
        return lines[index]

    def set_line_at_index(self, index: int, line: str, lines: list[str]):
        if 0 > index >= len(lines):
            raise BuelonSyntaxError(f'Error trying to set line: {index + 1}')
        lines[index] = line

    def parse(self, content: str):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute("CREATE TABLE IF NOT EXISTS in_string (start_l, start_c, end_l, end_c);")
        self.conn.commit()

        lines = content.splitlines()
        self.fetch_in_strings(lines)
        lines = self.remove_comments(lines)
        return self.get_variables(lines)

    def get_variables(self, lines: list[str]):
        self.check_for_tab_assignment(lines)

        self.check_for_args(lines)

        # self.check_for_scope(lines)
        # self.check_for_priority(lines)

        job_definitions = self.fetch_job_definitions(lines)
        pipes = self.fetch_pipes(lines)
        executions = self.fetch_executions(lines)
        loops = self.fetch_loops(lines)

        varibales = {
            'args': json.loads(json.dumps(self.args)),
            'scope': self.args['scope'],  # self.scope,
            'priority': self.args['priority'],  # self.priority,
            'job_definitions': job_definitions,
            'pipes': pipes,
            'executions': executions,
            'loops': loops,
        }

        unidentified = [f'Line {i + 1}: `{line}`' for i, line in enumerate(lines) if line.strip()]

        if unidentified:
            raise BuelonSyntaxError(' - Unidentifieable lines - \n' + '\n'.join(unidentified))

        return varibales

    def check_for_tab_assignment(self, lines: list[str]):
        for i, line in enumerate(lines):
            if line.startswith('TAB') and line.count('=') == 1:
                if line.count('"') == 2 and line.index('=') < line.index('"'):
                    self.tab = line.split('"')[1]
                    # lines[i] = ''
                    self.set_line_at_index(i, '', lines)
                if line.count("'") == 2 and line.index('=') < line.index("'"):
                    self.tab = line.split("'")[1]
                    # lines[i] = ''
                    self.set_line_at_index(i, '', lines)

    def check_for_args(self, lines: list[str]):
        for i, line in enumerate(lines):
            for token, config in PIPE_TOKENS['job args'].items():
                if line.startswith(f'!{token}'):
                    self.args[token] = line[len(f'!{token}'):].strip()
                    if config['type'] == 'string':
                        if not valid_word(self.args[token]):
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid {token} name. `{line}`')
                        name_warning(self.args[token])
                    elif config['type'] == 'int':
                        try:
                            int(self.args[token])
                        except ValueError:
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid {token} value. `{line}`')
                    elif config['type'] == 'calculate':
                        try:
                            self.args[token] = calculate(self.args[token])
                        except Exception:
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid {token} value. `{line}`')
                    self.set_line_at_index(i, '', lines)

    def check_for_scope(self, lines: list[str]):
        for i, line in enumerate(lines):
            if line.startswith(PIPE_TOKENS['scope']):
                self.scope = line[len(PIPE_TOKENS['scope']):].strip()
                if not valid_word(self.scope):
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid scope name. `{line}`')
                name_warning(self.scope)
                # lines[i] = ''
                self.set_line_at_index(i, '', lines)

    def check_for_priority(self, lines: list[str]):
        for i, line in enumerate(lines):
            if line.startswith(PIPE_TOKENS['priority']):
                try:
                    self.priority = int(line[len(PIPE_TOKENS['priority']):].strip())
                except ValueError:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid priority value. `{line}`')
                # lines[i] = ''
                self.set_line_at_index(i, '', lines)

    def fetch_in_strings(self, lines: str):
        in_strings = []
        in_string = False
        last = None

        for i, line in enumerate(lines):
            for i1 in indexes(line, '`'):
                if in_string:
                    self.conn.execute("INSERT INTO in_string (start_l, start_c, end_l, end_c) VALUES (?, ?, ?, ?);",
                                      (*last, i, i1))
                    self.conn.commit()
                    in_string = False
                else:
                    if '#' in line and line.index('#') < i1:
                        break
                    in_string = True
                    last = (i, i1)

        return in_strings

    def in_string(self, line_index: int, char_index: int):
        cursor = self.conn.cursor()
        query = f'''
        SELECT count(*) FROM in_string 
        WHERE 
        case when start_l = {line_index} then start_c <= {char_index}
        else
            case when end_l = {line_index} then end_c >= {char_index}
            else
            start_l < {line_index}
            AND end_l > {line_index}
            end
        end;
        '''
        cursor.execute(query)
        r = cursor.fetchall()[0][0] > 0
        cursor.close()
        return r

    def remove_comments(self, lines: list[str]):
        return [
            (line.split('#')[0] if not self.in_string(i, line.index('#')) else line) if '#' in line else line
            for i, line in enumerate(lines)

        ]

    def fetch_job_definitions(self, lines: list[str]):
        definitions = []
        consumed_lines = []

        definition_starts = [
            index for index, line in enumerate(lines)
            if not line.strip().startswith('for')
                and not line.strip().startswith('loop')
                and line.strip().endswith(':')
                and not self.in_string(index, 0)
                and not line.startswith(' ')
        ]

        def check_indentation(line: str, index: int):
            if len(line) <= len(self.tab) or not line.startswith(self.tab) or line[len(self.tab)] == ' ':
                raise BuelonSyntaxError(f'Line: {index + 1} Invalid indentation. `{line}`')

        for start in definition_starts:
            scope = self.scope
            priority = self.priority
            args = json.loads(json.dumps(self.args))

            def check_args(i: int):
                nonlocal args, start, consumed_lines
                check_indentation(lines[i], i)
                # line = lines[i][len(self.tab):]
                line = self.get_line_at_index(i, lines)[len(self.tab):]
                for token, config in PIPE_TOKENS['job args'].items():
                    if line.startswith(f'!{token}'):
                        value = line[len(f'!{token}'):].strip()
                        if config['type'] == 'string':
                            if not valid_word(value):
                                raise BuelonSyntaxError(f'Line: {i} Invalid arg for Job Definition. !{token} `{value}`')
                        elif config['type'] == 'int':
                            try:
                                value = int(value)
                            except ValueError:
                                raise BuelonSyntaxError(f'Line: {i} Invalid arg for Job Definition. !{token} `{value}`')
                        elif config['type'] == 'calculate':
                            try:
                                value = calculate(value)
                            except Exception:
                                raise BuelonSyntaxError(f'Line: {i + 1} Invalid arg for Job Definition. !{token} `{value}`')
                        consumed_lines.append(i)
                        start += 1
                        args[token] = value
                        return check_args(i + 1)

            def check_scope(i: int):
                nonlocal scope, start, consumed_lines
                check_indentation(lines[i], i)
                # line = lines[i][len(self.tab):]
                line = self.get_line_at_index(i, lines)[len(self.tab):]
                if line.startswith(PIPE_TOKENS['scope']):
                    scope = line[len(PIPE_TOKENS['scope']):].strip()
                    if not valid_word(scope):
                        raise BuelonSyntaxError(
                            f'Line: {start + 1} Invalid char in scope for Job Definition. `{scope}`')
                    name_warning(scope)
                    consumed_lines.append(i)
                    start += 1
                    check_priority(i + 1)

            def check_priority(i: int):
                nonlocal priority, start, consumed_lines
                # check_indentation(lines[i], i)
                check_indentation(self.get_line_at_index(i, lines), i)
                # line = lines[i][len(self.tab):]
                line = self.get_line_at_index(i, lines)[len(self.tab):]
                if line.startswith(PIPE_TOKENS['priority']):
                    priority = int(line[len(PIPE_TOKENS['priority']):].strip())
                    try:
                        int(priority)
                    except ValueError:
                        raise BuelonSyntaxError(f'Line: {start + 1} Invalid priority for Job Definition. `{priority}`')
                    consumed_lines.append(i)
                    start += 1
                    check_scope(i + 1)

            # if lines[start].count(':') > 1:
            if self.get_line_at_index(start, lines).count(':') > 1:
                raise BuelonSyntaxError(f'Line: {start + 1} Invalid name for Job Definition. More than 1 `:` found.')

            # name = lines[start].split(':')[0].strip()
            name = self.get_line_at_index(start, lines).split(':')[0].strip()
            consumed_lines.append(start)

            if not valid_word(name):
                raise BuelonSyntaxError(f'Line: {start + 1} Invalid char in name for Job Definition. `{name}`')

            check_args(start + 1)
            # check_scope(start + 1)
            # check_priority(start + 1)
            # language = lines[start + 1]
            language = self.get_line_at_index(start + 1, lines)
            check_indentation(language, start + 1)
            consumed_lines.append(start + 1)

            language = language[len(self.tab):].rstrip()

            if language not in LANGUAGES:
                raise BuelonSyntaxError(
                    f'Line: {start + 2} Invalid language for Job Definition. `{language}` not found.')

            check_args(start + 2)
            # check_scope(start + 2)
            # check_priority(start + 2)
            # relation = lines[start + 2]
            relation = self.get_line_at_index(start + 2, lines)
            check_indentation(relation, start + 2)
            consumed_lines.append(start + 2)

            relation = relation[len(self.tab):].rstrip()
            if not valid_word(relation):
                raise BuelonSyntaxError(f'Line: {start + 3} Invalid char in relation for Job Definition. `{relation}`')

            check_args(start + 3)
            # check_scope(start + 3)
            # check_priority(start + 3)
            # check_indentation(lines[start + 3], start + 3)
            check_indentation(self.get_line_at_index(start + 3, lines), start + 3)
            consumed_lines.append(start + 3)

            local = False
            # code = lines[start + 3][len(self.tab):].rstrip()
            code = self.get_line_at_index(start + 3, lines)[len(self.tab):].rstrip()
            if not code.startswith('`'):
                local = True
                code = code.strip()
                if not code:
                    raise BuelonSyntaxError(f'Line: {start + 4} Invalid code for Job Definition. No code provided.')
            else:
                if not code.startswith('`'):
                    raise BuelonSyntaxError(
                        f'Line: {start + 4} Invalid code for Job Definition. Missing "`" at start of code.')
                elif code.count('`') > 2:
                    raise BuelonSyntaxError(
                        f'Line: {start + 4} Invalid code for Job Definition. More than 2 "`" found in "{code}"')
                elif code.count('`') == 2:
                    begin, middle, end = code.split('`')
                    if end.strip():
                        raise BuelonSyntaxError(
                            f'Line: {start + 4} Invalid code for Job Definition. Code ends with `{end.strip()}`')
                    if begin.strip():
                        raise BuelonSyntaxError(
                            f'Line: {start + 4} Invalid code for Job Definition. Code starts with `{begin.strip()}`')
                    code = middle
                else:
                    begin, end = code.split('`')
                    if begin.strip():
                        raise BuelonSyntaxError(
                            f'Line: {start + 4} Invalid code for Job Definition. Code starts with `{begin.strip()}`')
                    code = end
                    i = start + 4
                    # while i < len(lines) and '`' not in lines[i]:
                    while i < len(lines) and '`' not in self.get_line_at_index(i, lines):
                        code += '\n' + lines[i]
                        consumed_lines.append(i)
                        i += 1
                    # if lines[i].count('`') > 1:
                    if self.get_line_at_index(i, lines).count('`') > 1:
                        raise BuelonSyntaxError(
                            f'Line: {i + 1} Invalid code for Job Definition. More than 1 "`" found.')
                    consumed_lines.append(i)
                    # begin, end = lines[i].split('`')
                    begin, end = self.get_line_at_index(i, lines).split('`')
                    if end.strip():
                        raise BuelonSyntaxError(
                            f'Line: {i + 1} Invalid code for Job Definition. Code ends with `{end.strip()}`')
                    code += '\n' + begin

            name_warning(name)
            name_warning(relation)
            # print('def', {
            #     'name': name,
            #     'language': LANGUAGES[language],
            #     'relation': relation,
            #     'code': code,
            #     'local': local,
            #     'scope': scope,
            #     'priority': priority,
            # })
            definitions.append({
                'name': name,
                'language': LANGUAGES[language],
                'relation': relation,
                'code': code,
                'local': local,
                'scope': args['scope'],  # scope,
                'priority': args['priority'],  # priority,
                'args': args,
            })

            for i in consumed_lines:
                # lines[i] = ''
                self.set_line_at_index(i, '', lines)
            consumed_lines = []

        def get_import_values(i: int, arg: str, scope: str, priority: int, args: dict):
            alias = None

            def extract_args(arg: str, args: dict | None = None):
                args = args or {}
                for token, config in PIPE_TOKENS['job args'].items():
                    if f'!{token}' in arg:
                        arg, rest = arg.split(f'!{token}', 1)
                        _arg, _args = extract_args(rest, args)
                        _arg = _arg.strip()
                        if config['type'] == 'string':
                            if not valid_word(_arg):
                                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid arg name for Job Definition. `{_arg}`')
                        elif config['type'] == 'int':
                            try:
                                _arg = int(_arg)
                            except ValueError:
                                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid arg name for Job Definition. `{_arg}`')
                        elif config['type'] == 'calculate':
                            try:
                                _arg = calculate(_arg)
                            except Exception:
                                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid `{_arg}` value.')
                        args[token] = _arg
                return arg, args
            # print('\nargs ->', extract_args(arg), '\n\n')

            arg, args = extract_args(arg, json.loads(json.dumps(args)))

            # if PIPE_TOKENS['scope'] in arg:
            #     if arg.count(PIPE_TOKENS['scope']) > 1:
            #         raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid pipe definition. More than 1 `{PIPE_TOKENS["scope"]}` found.')
            #     arg, scope = arg.split(PIPE_TOKENS['scope'])
            #     if PIPE_TOKENS['priority'] in scope:
            #         if scope.count(PIPE_TOKENS['priority']) > 1:
            #             raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid pipe definition. More than 1 `{PIPE_TOKENS["priority"]}` found.')
            #         scope, priority = scope.split(PIPE_TOKENS['priority'])
            #     elif PIPE_TOKENS['priority'] in arg:
            #         if arg.count(PIPE_TOKENS['priority']) > 1:
            #             raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid pipe definition. More than 1 `{PIPE_TOKENS["priority"]}` found.')
            #         arg, priority = arg.split(PIPE_TOKENS['priority'])
            # elif PIPE_TOKENS['priority'] in arg:
            #     if arg.count(PIPE_TOKENS['priority']) > 1:
            #         raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid pipe definition. More than 1 `{PIPE_TOKENS["priority"]}` found.')
            #     arg, priority = arg.split(PIPE_TOKENS['priority'])

            if ' as ' in arg:
                # if arg.count(' as ') > 1:
                #     raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid pipe definition. More than 1 `as` found.')
                arg, alias = arg.split(' as ', 1)
            else:
                alias = arg

            arg, alias = arg.strip(), alias.strip()

            if not valid_word(arg):
                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid char in name for Job Definition. `{arg}`')
            if not valid_word(alias):
                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid char in alias for Job Definition. `{alias}`')
            if not valid_word(args.get('scope', scope)):
                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid char in scope for Job Definition. `{args.get("scope", scope)}`')
            try:
                priority = int(args.get('priority', priority))  # int(priority.strip() if isinstance(priority, str) else priority)
            except ValueError:
                raise BuelonSyntaxError(f'Line: ~{i + 1} Invalid priority for Job Definition. `{args.get("priority", priority)}`')

            name_warning(arg)
            name_warning(alias)
            name_warning(args.get('scope', scope))

            return arg.strip(), alias.strip(), args.get('scope', scope).strip(), priority, args

        i = 0
        consume = []
        while i < len(lines):
            line = lines[i]
            if (line.startswith(PIPE_TOKENS['create job def'])
                    and any(line[len(PIPE_TOKENS['create job def']):].lstrip().startswith(k + ' ') for k in LANGUAGES.keys())):
                line = line[len(PIPE_TOKENS['create job def']):].lstrip()

                if '(' not in line:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. Missing `(`')

                if line.count('(') > 1:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. More than 1 `(` found.')

                consume.append(i)

                language, rest = line.split('(')
                language = language.strip()

                if language not in LANGUAGES:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. `{language}` not found.')

                if ')' in line:
                    if line.count(')') > 1:
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. More than 1 `)` found.')

                    imports, code = rest.split(')')
                else:
                    imports = rest

                    while ')' not in line:
                        i += 1
                        if i >= len(lines):
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. Missing `)`')
                        line = lines[i]
                        consume.append(i)
                        if ')' in line:
                            if line.count(')') > 1:
                                raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. More than 1 `)` found.')
                        else:
                            imports += line

                    _imports, code = line.split(')')
                    imports += _imports
                    imports = imports.replace('\n', ' ')

                args = [get_import_values(i, arg.strip(), self.scope, self.priority, self.args) for arg in imports.split(',')]

                if not code.strip().startswith('`'):
                    local = True
                    code = code.strip()
                    if not code:
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid code for Job Definition. No code provided.')
                else:
                    local = False
                    if code.count('`') > 2:
                        raise BuelonSyntaxError(
                            f'Line: {i + 1} Invalid code for Job Definition. More than 2 "`" found in "{code}"')
                    if code.count('`') == 2:
                        begin, middle, end = code.split('`')
                        if end.strip():
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid code for Job Definition. Code ends with `{end.strip()}`')
                        if begin.strip():
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid code for Job Definition. Code starts with `{begin.strip()}`')
                        code = middle
                    else:
                        begin, end = code.split('`')
                        if begin.strip():
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid code for Job Definition. Code starts with `{begin.strip()}`')
                        code = end
                        i += 1
                        while i < len(lines) and '`' not in lines[i]:
                            code += '\n' + lines[i]
                            consume.append(i)
                            i += 1
                        if lines[i].count('`') > 1:
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid code for Job Definition. More than 1 "`" found.')
                        consume.append(i)
                        begin, end = lines[i].split('`')
                        if end.strip():
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid code for Job Definition. Code ends with `{end.strip()}`')
                        code += '\n' + begin

                # print(language, args, code)

                for arg in args:
                    func, alias, scope, priority, _args = arg
                    # print('args', _args)
                    definitions.append({
                        'name': alias,
                        'language': LANGUAGES[language],
                        'relation': func,
                        'code': code,
                        'local': local,
                        'scope': _args['scope'],  # scope,
                        'priority': _args['priority'],  # priority,
                        'args': _args,
                    })
            i += 1
        for j in consume:
            self.set_line_at_index(j, '', lines)

        return definitions

    def fetch_pipes(self, lines: list[str]):
        pipes = []
        consumed_lines = []

        for i, line in enumerate(lines):
            if '=' in line and not line.startswith(' ') and not line.strip().endswith(':') and not line_is_execution(
                    line):
                if line.count('=') > 1:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. More than 1 `=` found.')
                if line.split('=')[-1].strip().startswith('(') and line.count('(') == 1:

                    name, end = line.split('=')
                    name = name.strip()

                    if ')' in end:
                        end = end.strip()
                        if '|' not in end:
                            print('skip')
                            continue
                        if not end.endswith(')'):
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid pipe definition. `)` missplaced in `{line}`')
                        if end.count(')') > 1:
                            raise BuelonSyntaxError(
                                f'Line: {i + 1} Invalid pipe definition. More than 1 `)` found in `{line}`')
                        jobs = end.strip()[1:-1].strip()
                        consumed_lines.append(i)
                    else:
                        code = end
                        l = [i]
                        j = i + 1
                        # nl = lines[j]
                        nl = self.get_line_at_index(j, lines)
                        l.append(j)
                        code += ' ' + nl
                        while ')' not in nl:
                            j += 1
                            # nl = lines[j]
                            nl = self.get_line_at_index(j, lines)
                            l.append(j)
                            code += ' ' + nl

                        if nl.count(')') > 1:
                            raise BuelonSyntaxError(f'Line: {j + 1} Invalid pipe definition. More than 1 `)` found.')

                        if not nl.strip().endswith(')'):
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe definition. `)` missplaced in `{nl}`')

                        jobs = code.strip()[1:-1]
                        consumed_lines.extend(l)

                    if not valid_word(name):
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe name. `{name}`')

                    jobs = [job.strip() for job in jobs.split('|') if job.strip() != '']
                    for job in jobs:
                        if not valid_word(job):
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe job. `{job}`')
                        name_warning(job)
                    name_warning(name)
                    pipes.append({
                        'name': name,
                        'jobs': jobs
                    })

                elif '|' in line.split('=')[-1]:
                    consumed_lines.append(i)
                    name, jobs = line.split('=')
                    name = name.strip()
                    if not valid_word(name):
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe name. `{name}`')
                    jobs = [job.strip() for job in jobs.split('|') if job.strip() != '']
                    for job in jobs:
                        if not valid_word(job):
                            raise BuelonSyntaxError(f'Line: {i + 1} Invalid pipe job. `{job}`')
                        name_warning(job)
                    name_warning(name)
                    pipes.append({
                        'name': name,
                        'jobs': jobs
                    })

        for i in consumed_lines:
            # lines[i] = ''
            self.set_line_at_index(i, '', lines)

        return pipes

    def fetch_executions(self, lines: list[str]):
        executions = []  # [{'name': '', args: ['', '']}...]
        consumed_lines = []

        for i, line in enumerate(lines):
            if (not line.startswith(' ') and line.count('(') == 1 and line.count(')') == 1 and line.index(
                    '(') < line.index(')')
                    and not line.strip().startswith('for ')):
                consumed_lines.append(i)
                value = line
                if '=' not in line:
                    result = None
                else:
                    if line.count('=') > 1:
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid execution definition. More than 1 `=` found.')
                    result, value = line.split('=')
                    result = result.strip()
                    if not valid_word(result):
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid execution name. `{result}`')
                name, end = value.split('(')
                args, end = end.split(')')
                if end.strip():
                    raise BuelonSyntaxError(
                        f'Line: {i + 1} Invalid execution definition. Extra chars after `)`. `{line}`')

                name = name.strip()

                if not valid_word(name):
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid execution name. `{name}`')

                args = args.split(',')
                args = [arg.strip() for arg in args if arg.strip()]
                for arg in args:
                    if not valid_word(arg):
                        raise BuelonSyntaxError(f'Line: {i + 1} Invalid execution arg. `{arg}`')
                    name_warning(arg)
                if result:
                    name_warning(result)
                name_warning(name)
                executions.append({
                    'result': result,
                    'name': name,
                    'args': args
                })

        for i in consumed_lines:
            # lines[i] = ''
            self.set_line_at_index(i, '', lines)

        return executions

    def fetch_loops(self, lines: list[str]):
        loops = []

        for i, line in enumerate(lines):
            if (line.startswith('for ')
                    and ' in ' in line and line.count(' in ') == 1
                    and line.strip().endswith(':') and line.count(':') == 1
                    and line.count('(') == 1 and line.count(')')
                    and line.index('(') < line.index(')')
                    and line.index(')') < line.index(':')
                    and line.index(' in ') < line.index('(')
                    and line.index(' in ') > line.index('for ')):

                value = line[4:].replace(':', '').strip()
                var, pipe_exection = value.split(' in ')
                var = var.strip()
                if not valid_word(var):
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid loop variable name. `{var}`')

                pipe_exection = self.fetch_executions([pipe_exection.strip()])

                if not pipe_exection:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid loop definition. Missing pipe execution.')

                if len(pipe_exection) > 1:
                    raise BuelonSyntaxError(f'Line: {i + 1} Invalid loop definition. More than 1 pipe execution.')

                pipe_exection = pipe_exection[0]
                j = i + 1
                # nl = lines[j]
                nl = self.get_line_at_index(j, lines)
                if not nl.startswith(self.tab) or not nl.strip():
                    raise BuelonSyntaxError(f'Line: {j + 1} Invalid loop definition. Missing indentation.')

                code = nl[len(self.tab):]
                # lines[j] = ''
                self.set_line_at_index(j, '', lines)
                for j in range(j + 1, len(lines)):
                    # nl = lines[j]
                    nl = self.get_line_at_index(j, lines)
                    if nl.startswith(self.tab):
                        code += '\n' + nl[len(self.tab):]
                        # lines[j] = ''
                        self.set_line_at_index(j, '', lines)
                    else:
                        break

                # lines[i] = ''
                self.set_line_at_index(i, '', lines)
                varibales = self.get_variables([''] * (i + 1) + code.splitlines())

                loops.append({
                    'name': var,
                    'pipe_execution': pipe_exection,
                    'varibales': varibales,
                })

        return loops


def generate_steps_from_code(code: str) -> Generator[step.Step, None, None]:
    """Extract steps and starters from the given pipeline code.

    Args:
        code (str): The pipeline code to be processed.
        lazy_steps (bool, optional): Whether to use lazy loading for steps. Defaults to False.

    Raises:
        Exception: If an error occurs during parsing or execution.

    Returns:
        dict: A dictionary containing the extracted steps and starters.
    """
    # print('start')

    # with open('amazon_report_v2.bue') as f:  # ('test.bue') as f:  # ('amazon_report_v2.bue') as f:
    #     content = f.read()

    # with open('test.yaml', 'w') as f:
    #     f.write(PipelineParser().prepare(code))

    # with open('test.yaml', 'r') as f:
    #     content = f.read()
    #     for j in PipelineParser().build(content):
    #         # print(j)
    #         del j

    for job in PipelineParser().build(PipelineParser().prepare(code)):
        # print(j)
        yield job
        del job


def run_code(code: str):
    """Extract steps and starters from the given pipeline code.

    Args:
        code (str): The pipeline code to be processed.
        lazy_steps (bool, optional): Whether to use lazy loading for steps. Defaults to False.

    Raises:
        Exception: If an error occurs during parsing or execution.

    Returns:
        dict: A dictionary containing the extracted steps and starters.
    """
    pipeline_parser = PipelineParser()
    content = pipeline_parser.prepare(code)
    pipeline_parser.run(content)



