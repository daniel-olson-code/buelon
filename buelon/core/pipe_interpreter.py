"""Module for parsing and executing a custom pipeline language.

This module provides functionality to parse and execute code written in a custom
pipeline language. It includes utilities for handling scopes, steps, pipes, actions,
and loops within the custom language.

Constants:
    INDENT (str): A string constant representing four spaces.
    PIPE_TOKENS (dict): A dictionary defining various tokens used in the pipeline language.
    LANGUAGES (dict): A dictionary mapping language names to their standardized identifiers.
"""
import json
import sys
import traceback
from typing import Dict, Any

from buelon.helpers import pipe_util
from . import step_definition
from . import pipe
from . import action
from . import loop
from . import step


# Constants
INDENT = '    '
PIPE_TOKENS = {
    'import': ':',
    'pipe': '|',
    'func': ('(', ')'),
    'scope': '$',
    'str': ['"'],
    'num': ['#'],
    'bool': ['!'],
    'list': ['[', ']'],
    'call':  ['(', ')'],
    'kwargs': ['{', '}'],
    'block': [{'start': '`', 'end': '`'}]
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


def run(code: str) -> Dict:
    """Parse and execute the given pipeline code.

    Args:
        code (str): The pipeline code to be executed.

    Returns:
        dict: A dictionary containing the parsed pipeline variables.

    Raises:
        Exception: If an error occurs during parsing or execution.
    """
    try:
        variables = {
            '__steps__': {},
            '__starters__': [],
            '__scope__': 'default'
        }

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
                raise SyntaxError(f'Line {i+1}: Invalid syntax')

        lines = [remove_comments(line) for line in code.splitlines()]
        i = 0
        while i < len(lines):
            read_line(i)
            i += 1

        return variables

    except Exception as e:
        traceback.print_exc()
        print(f'Error {type(e).__name__} | {e}')
        sys.exit(1)


def get_steps_from_code(code: str) -> Dict[str, Any]:
    """Extract steps and starters from the given pipeline code.

    Args:
        code (str): The pipeline code to be processed.

    Returns:
        dict: A dictionary containing the extracted steps and starters.
    """
    variables = run(code)
    return {'steps': variables['__steps__'], 'starters': variables['__starters__']}

