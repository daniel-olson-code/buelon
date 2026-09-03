"""Module for handling execution steps and results in a pipeline.

This module provides classes and functions for managing execution steps,
their results, and associated utilities in a pipeline structure.
"""

from __future__ import annotations
import enum
import os
import asyncio
import inspect
from typing import Any, Container, Iterable, List

import orjson
import unsync

from . import execution
from buelon.helpers import pipe_util
# import buelon.hub

# import pickle


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

    def to_dict(self):
        """Convert the Result object to a dictionary.

        Returns:
            dict: A dictionary representation of the Result object.
        """
        return {
            'status': self.status.value if self.status else None,
            'env': self.env,
            'priority': self.priority,
            'velocity': self.velocity,
            'data': self.data
        }

    def from_dict(self, d):
        """Populate the Result object from a dictionary.

        Args:
            d (dict): A dictionary containing result data.
        """
        self.status = StepStatus(d['status']) if d['status'] else None
        self.env = d['env']
        self.priority = d['priority']
        self.velocity = d['velocity']
        self.data = d['data']
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
    unknown = 8


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


def _local_module_name(path: str) -> str:
    """Module name for a local code file -- strips the `.py` suffix, nothing else.

    `rstrip('.py')` used to be used here; it strips *characters*, so `apply.py`
    became `appl` and `happy.py` became `ha` (BUGS.md #17).
    """
    return os.path.basename(path).removesuffix('.py')


def all_parents_complete(parents: Iterable[str], completed: Container[str]) -> bool:
    """Is every one of `parents` finished, so a child is safe to run?

    The one condition two separate schedulers kept getting wrong in the same way, so
    they now ask it here:

      * the hub's `handle_step` success branch used to promote a child as soon as it
        found it in `queued` -- true after the *first* of two parents finished
        (BUGS.md #51);
      * `PipelineParser.run`, the `bue run -f` local runner, had no gate at all and
        enqueued a fan-in child once per parent, running it twice (BUGS.md #52).

    `completed` must mean "actually succeeded", not "has an entry somewhere". The hub
    passes its `done` dict rather than `db`, because a job that returned `pending`
    ("not ready, try me again") is already in `db` carrying a placeholder -- see #33
    and #51. A `dict`, `set` or anything else supporting `in` works.

    A job with no parents is complete by definition, so this returns True for `()`.
    """
    return all(parent in completed for parent in parents)


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
        attempts (int): Number of attempts made for the step.
        handbacks (int): Number of times the step handed itself back as `pending`.
        max_handbacks (int): Ceiling on `handbacks`; 0 means unlimited.
        timeout (float): Timeout for the step execution.
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
    retries: int = 0
    timeout: float = 0.0
    # How many times this job has already come back as an error. Counted hub-side in
    # `handle_step` and compared against `retries` -- BUGS.md #14. It rides along in
    # `__dict__` (so it survives the requeue -> dispatch -> release round trip) and
    # defaults to 0 for every job built before the field existed.
    attempts: int = 0
    # Wall-clock timestamp before which the hub must not dispatch this job; 0.0 means
    # "dispatchable now". Set by `handle_step` when a failed job is requeued for a retry
    # and skipped over by `get_steps_v2` until it passes -- BUGS.md #35. Hub-owned
    # bookkeeping, not a `.bue` job arg, and like `attempts` it rides along in
    # `__dict__` so it survives the requeue -> dispatch -> release round trip and the
    # hub snapshot.
    not_before: float = 0.0
    # How many times this job has handed itself back as `pending` -- BUGS.md #50.
    # Counted hub-side in `handle_step`'s `pending` branch. Deliberately NOT
    # `attempts`: that is the error budget `!retries` spends, and a poll that says
    # "not ready yet" is not a failure. Kept separate so a job can do both without
    # one starving the other. Like `attempts` it rides along in `__dict__`.
    handbacks: int = 0
    # Optional ceiling on `handbacks`; 0 (the default) means unlimited, because
    # unbounded re-queueing is the documented point of `pending` -- a poll loop has no
    # idea how many attempts it needs. Set `!max_handbacks N` on a job that should give
    # up rather than spin forever. BUGS.md #50.
    max_handbacks: int = 0

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

    def run(self, *args: Any, mut=None) -> Result:
        """Execute the step.

        Args:
            *args: Variable length argument list to be passed to the execution function.

        Returns:
            Result: The result of the step execution.

        Raises:
            ValueError: If the step type is not recognized.
        """
        # code = self.get_code()
        code = self.code
        module_name = None

        if self.local:
            module_name = _local_module_name(self.code)
            with open(self.code) as f:
                code = f.read()

        postgres = LanguageTypes.postgres.value
        python = LanguageTypes.python.value
        sqlite3 = LanguageTypes.sqlite3.value

        if self.type == postgres:
            return create_return_value(execution.run_postgres(code, self.func, *args, **self.kwargs))

        if self.type == python:
            return create_return_value(execution.run_py(code, module_name, self.func, *args, mut=mut, **self.kwargs))

        if self.type == sqlite3:
            return create_return_value(execution.run_sqlite3(code, self.func, *args, **self.kwargs))

        raise ValueError(f"Unrecognized step language type: {self.type}")

    async def arun(self, *args: Any, mut=None) -> Result:
        """Execute the step.

        Args:
            *args: Variable length argument list to be passed to the execution function.

        Returns:
            Result: The result of the step execution.

        Raises:
            ValueError: If the step type is not recognized.
        """
        # code = self.get_code()
        code = self.code
        module_name = None

        if self.local:
            module_name = _local_module_name(self.code)
            with open(self.code) as f:
                code = f.read()

        postgres = LanguageTypes.postgres.value
        python = LanguageTypes.python.value
        sqlite3 = LanguageTypes.sqlite3.value

        if self.type == postgres:
            return create_return_value(await execution.arun_postgres(code, self.func, *args, **self.kwargs))

        if self.type == python:
            return create_return_value(await execution.arun_py(code, module_name, self.func, *args, mut=mut, **self.kwargs))

        if self.type == sqlite3:
            # return create_return_value(execution.run_sqlite3(code, self.func, *args, **self.kwargs))
            return create_return_value(await asyncio.to_thread(execution.run_sqlite3, code, self.func, *args, **self.kwargs))

        raise ValueError(f"Unrecognized step language type: {self.type}")

    def is_async(self):
        postgres = LanguageTypes.postgres.value
        python = LanguageTypes.python.value
        sqlite3 = LanguageTypes.sqlite3.value

        if self.type == postgres:
            return True

        if self.type == python:
            return True  # create_return_value(execution.run_py(code, self.func, *args, **self.kwargs))

        if self.type == sqlite3:
            return False

        return False

    async def run_async(self, *args: Any, mut = None) -> Result:
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
            return create_return_value(await execution.run_py_async(code, self.func, *args, mut=mut, **self.kwargs))

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


class Job(Step):
    pass




