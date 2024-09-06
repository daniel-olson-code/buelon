"""Module for handling execution steps and results in a pipeline.

This module provides classes and functions for managing execution steps,
their results, and associated utilities in a pipeline structure.
"""

from __future__ import annotations
import enum
import os
from typing import Any, List

import orjson

from . import execution
from buelon.helpers import pipe_util
import buelon.hub

import pickle


class Result:
    """Represents the result of an execution step.

    Attributes:
        status (StepStatus): The status of the step execution.
        env (dict): Environment variables associated with the result.
        priority (int): Priority of the result.
        velocity (float): Velocity associated with the result.
        data (Any): Any data produced by the step execution.
    """

    def __init__(self, status=None, env=None, priority=None, velocity=None, data=None):
        """Initialize a Result object.

        Args:
            status (StepStatus, optional): The status of the step execution.
            env (dict, optional): Environment variables. Defaults to an empty dict.
            priority (int, optional): Priority of the result.
            velocity (float, optional): Velocity associated with the result.
            data (Any, optional): Any data produced by the step execution.
        """
        self.status = status
        self.env = env or {}
        self.priority = priority
        self.velocity = velocity
        self.data = data

    @classmethod
    def from_result(cls, result):
        """Create a Result object from a tuple.

        Args:
            result (tuple): A tuple containing result data.

        Returns:
            Result: A new Result object initialized with the tuple data.
        """
        self = cls()
        status, env, p, v, data = result
        self.status = status
        self.env = env
        self.priority = p
        self.velocity = v
        self.data = data
        return self


class StepStatus(enum.Enum):
    """Enumeration of possible step statuses."""
    success = 1
    queued = 2
    pending = 3
    cancel = 4
    reset = 5
    working = 6
    error = 7


class LanguageTypes(enum.Enum):
    """Enumeration of supported language types in the pipeline."""
    python = 'PYTHON'
    postgres = 'POSTGRESQL'
    sqlite3 = 'SQLITE3'


def create_return_value(value) -> Result:
    """Create a Result object from various input types.

    Args:
        value (Any): The input value to convert into a Result.

    Returns:
        Result: A Result object created from the input value.

    Raises:
        ValueError: If the input value is not a valid type or format.
    """
    if isinstance(value, Result):
        return value

    if not isinstance(value, tuple):
        result = StepStatus.success, None, None, None, value
    else:
        if len(value) == 2:
            result = value[0], None, None, None, value[1]
        elif len(value) == 3:
            result = value[0], value[1], None, None, value[2]
        elif len(value) == 4:
            result = value[0], value[1], value[2], None, value[3]
        elif len(value) == 5:
            result = value[0], value[1], value[2], value[3], value[4]
        else:
            raise ValueError('Tuple return values are reserved for pipeline operations.')

    return Result.from_result(result)


class Step(pipe_util.PipeObject):
    """Represents a step in the execution pipeline.

    Attributes:
        id (str): Unique identifier for the step.
        name (str): Name of the step.
        type (str): Type of the step (e.g., 'POSTGRESQL', 'PYTHON', 'SQLITE3').
        code (str): Code to be executed in this step.
        func (str): Function to be called within the code.
        local (bool): Whether the code is found locally.
        kwargs (dict): Additional keyword arguments for the step.
        scope (str): Scope of the step.
        tag (str): Tag associated with the step.
        priority (int): Priority of the step.
        velocity (float): Velocity associated with the step.
        parents (list[str]): List of parent step IDs.
        children (List[str]): List of child step IDs.
    """
    id: str = None
    name: str = 'empty'
    type: str = None
    code: str = None
    func: str = None
    local: str = False  # code found locally e.i. `code` is a file path.
    kwargs: dict = None
    scope: str = 'default'
    tag: str = None
    priority: int = 0
    velocity: float = None

    parents: list[str] = None
    children: List[str] = None

    def get_code(self):
        """Retrieve the code for this step.

        Returns:
            str: The code to be executed in this step.
        """
        code = self.code
        if self.local:
            with open(self.code) as f:
                code = f.read()
        return code

    def run(self, *args: Any) -> Result:
        """Execute the step.

        Args:
            *args: Variable length argument list to be passed to the execution function.

        Returns:
            Result: The result of the step execution.

        Raises:
            ValueError: If the step type is not recognized.
        """
        code = self.get_code()

        postgres = LanguageTypes.postgres.value
        python = LanguageTypes.python.value
        sqlite3 = LanguageTypes.sqlite3.value

        if self.type == postgres:
            return create_return_value(execution.run_postgres(code, self.func, *args, **self.kwargs))

        if self.type == python:
            return create_return_value(execution.run_py(code, self.func, *args, **self.kwargs))

        if self.type == sqlite3:
            return create_return_value(execution.run_sqlite3(code, self.func, *args, **self.kwargs))

        raise ValueError(f"Unrecognized step language type: {self.type}")

    @classmethod
    def lazy_save(cls, self: Step, path, shared_variables):
        # buelon.hub.set_step(self)
        with open(path, 'wb') as f:
            f.write(orjson.dumps(self.to_json()))
        return self.id

    @classmethod
    def lazy_load(cls, path, result, shared_variables):
        # return buelon.hub.get_step(result)
        with open(path, 'rb') as f:
            return cls().from_json(orjson.loads(f.read()))

    @classmethod
    def lazy_delete(cls, path, result, shared_variables):
        # buelon.hub.remove_step(result)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass





