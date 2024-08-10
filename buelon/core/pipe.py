"""
This module defines the Pipe class and related functions for creating and managing pipeline steps.
"""

from buelon.helpers import pipe_util
from . import pipe_interpreter
from . import step_definition
from . import step


class Pipe:
    """
    Represents a pipeline that can create and manage a series of steps.

    Attributes:
        name (str): The name of the pipe.
        imports (str): A comma-separated string of import names.
        kwargs (dict): Optional keyword arguments for the pipe.
    """

    def __init__(self, name: str, imports: str, kwargs: dict = None):
        """
        Initialize a new Pipe instance.

        Args:
            name (str): The name of the pipe.
            imports (str): A comma-separated string of import names.
            kwargs (dict, optional): Keyword arguments for the pipe. Defaults to None.
        """
        self.name = name
        self.imports = imports
        self.kwargs = kwargs or {}

    def create_steps(self, i: int, variables: dict, args: str, env=None) -> list[step.Step]:
        """
        Create a series of steps based on the pipe's imports and given arguments.

        Args:
            i (int): The current line index in the pipeline code.
            variables (dict): A dictionary containing pipeline variables and functions.
            args (str): A comma-separated string of argument names.
            env (optional): Environment variables for the steps. Defaults to None.

        Returns:
            list[step.Step]: A list of created Step instances.

        Raises:
            SyntaxError: If an unknown argument or import is encountered.
            TypeError: If an import is not a StepDefinition instance.
        """
        steps: list[step.Step] = []
        last_step_id = None
        args_step_ids = {}

        if args:
            for arg in args.split(','):
                if arg in variables:
                    variables[arg]: list[step.Step]
                    args_step_ids[arg] = variables[arg][-1].id
                else:
                    raise SyntaxError(f'Line {i+1}: Unknown argument \'{arg}\'')

        for imp in self.imports.split(','):
            if imp not in variables:
                raise SyntaxError(f'Line {i+1}: Unknown import \'{imp}\'')
            if not isinstance(variables[imp], step_definition.StepDefinition):
                raise TypeError(f'Line {i+1}: \'{imp}\' is not an import')

            val: step_definition.StepDefinition = variables[imp]
            _step = val.to_step(env=env)
            if not last_step_id:
                _step.parents = []
                if args:
                    for arg in args.split(','):
                        _step.parents.append(args_step_ids[arg])
                        variables['__steps__'][args_step_ids[arg]].children += [_step.id]
            else:
                _step.parents = [last_step_id]
                variables['__steps__'][last_step_id].children += [_step.id]

            _step.children = []
            last_step_id = _step.id
            variables['__steps__'][_step.id] = _step
            steps.append(_step)

        if steps and not steps[0].parents:
            variables['__starters__'].append(steps[0].id)

        return steps


def check_for_pipe(index: int, line: str, variables: dict) -> bool:
    """
    Check if a line defines a pipe and create a Pipe instance if it does.

    Args:
        index (int): The current line index in the pipeline code.
        line (str): The current line of code being processed.
        variables (dict): A dictionary containing pipeline variables and functions.

    Returns:
        bool: True if a pipe was found and created, False otherwise.

    Raises:
        SyntaxError: If the pipe definition syntax is invalid.
    """
    token = pipe_interpreter.PIPE_TOKENS['pipe']
    if '=' in line.replace('=>', '') and token in line:
        if line.count('=') > 1:
            raise SyntaxError(f'Line {index+1}: Invalid pipe. Too many \'=\'')
        name, imports = line.split('=')
        name = pipe_util.trim(name)
        imports = ','.join([pipe_util.trim(imp) for imp in imports.split(token) if pipe_util.trim(imp) != ''])
        variables[name] = Pipe(name, imports)
        return True
    return False

