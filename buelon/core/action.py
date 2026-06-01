"""Module for handling action checks in a pipeline system."""

from buelon.helpers import pipe_util
from . import pipe
from . import pipe_interpreter


def check_for_actions(index: int, line: str, variables: dict) -> bool:
    """Check and process action calls in a line of pipeline code.

    This function identifies and processes function calls in the pipeline code.
    It handles variable assignment from function returns and validates the syntax
    of the function call.

    Args:
        index (int): The current line index in the pipeline code.
        line (str): The current line of code being processed.
        variables (dict): A dictionary containing pipeline variables and functions.

    Returns:
        bool: True if an action was found and processed, False otherwise.

    Raises:
        SyntaxError: If there are syntax errors in the function call.
        TypeError: If the called object is not a Pipe instance.

    The function performs the following steps:
    1. Checks if the line contains a function call.
    2. Validates the syntax of the function call.
    3. Extracts the return value assignment if present.
    4. Parses the function name and arguments.
    5. Validates the function exists in the variables.
    6. Executes the function and assigns the return value if necessary.
    """
    begin_token, end_token = pipe_interpreter.PIPE_TOKENS['func']
    if begin_token in line and end_token in line and not line.strip().startswith('for'):
        # Validation and parsing logic...
        if line.count(begin_token) > 1:
            raise SyntaxError(f'Line {index + 1}: Invalid call. To many \'{begin_token}\'')
        if line.count(end_token) > 1:
            raise SyntaxError(f'Line {index + 1}: Invalid call. To many \'{end_token}\'')
        return_value = ''
        if '=' in line.replace('=>', ''):
            if line.count('=') > 1:
                raise SyntaxError(f'Line {index + 1}: Invalid call. To many \'=\'')
            return_value, line = line.split('=')
            return_value = pipe_util.trim(return_value)
        name, args = line.split(begin_token)
        args = ','.join(
            [pipe_util.trim(arg) for arg in args.split(end_token)[0].strip().split(',') if pipe_util.trim(arg) != ''])
        name = pipe_util.trim(name)
        if name not in variables:
            raise SyntaxError(f'Line {index + 1}: Call to unknown function \'{name}\'')

        # Execute the function
        _pipe: pipe.Pipe = variables[name]
        if not isinstance(_pipe, pipe.Pipe):
            print('pipe is ',  type(_pipe))
            raise TypeError(f'Line {index+1}: \'{name}\' is not a function')
        val = _pipe.create_steps(index, variables, args, pipe_util.json_copy(_pipe.kwargs))

        # Assign return value if necessary
        if return_value:
            variables[return_value] = val

        return True
    return False
