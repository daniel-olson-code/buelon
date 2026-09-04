"""
Deferred imports for dependencies that are not part of the base install.

`buelon` splits its heavy drivers into pip extras (`buelon[postgres]`,
`buelon[web]`, `buelon[all]`) so that a machine running only `bue hub` or
`bue worker` does not have to build or download a Postgres driver. But
`buelon/__init__.py` imports `helpers.postgres` and `bucket` eagerly, and
`buelon.helpers.postgres` is a documented, directly-used module -- so the
imports cannot simply move inside the functions that need them without
breaking `import buelon` on a base install, or changing the public API.

`optional_import` resolves that: it returns the real module when the extra is
installed, and otherwise a placeholder that imports cleanly and only raises --
with a message naming the extra to install -- if something actually touches it.

    psycopg2 = optional_import('psycopg2', 'postgres',
                               submodules=('extras', 'errors'))

BUGS.md #58.
"""
import importlib
from typing import Iterable


class MissingDependency:
    """
    Stand-in for a module that is not installed.

    Importing it is free; any attribute access raises `ModuleNotFoundError`
    chained to the original import failure.
    """

    def __init__(self, name: str, extra: str, error: BaseException) -> None:
        # Written straight into __dict__ so __getattr__ never sees these and
        # recurses. __getattr__ only fires for names that are *not* found
        # normally, so the instance attributes below resolve without it.
        self.__dict__['_name'] = name
        self.__dict__['_extra'] = extra
        self.__dict__['_error'] = error

    def _fail(self, item: str) -> 'ModuleNotFoundError':
        name = self.__dict__['_name']
        extra = self.__dict__['_extra']
        raise ModuleNotFoundError(
            f'`{name}` is not installed, so `{name}.{item}` is unavailable. '
            f'It ships as an optional extra: `pip install buelon[{extra}]` '
            f'(or `pip install buelon[all]`).'
        ) from self.__dict__['_error']

    def __getattr__(self, item: str):
        self._fail(item)

    def __call__(self, *args, **kwargs):
        self._fail('__call__')

    def __repr__(self) -> str:
        return f'<MissingDependency {self.__dict__["_name"]!r} (buelon[{self.__dict__["_extra"]}])>'

    def __bool__(self) -> bool:
        # So `if psycopg2:` reads as "is it available", rather than raising.
        return False


def optional_import(
    name: str,
    extra: str,
    submodules: Iterable[str] = (),
):
    """
    Import `name`, or return a `MissingDependency` placeholder for it.

    Args:
        name: the top-level module to import, e.g. `'psycopg2'`.
        extra: the pip extra that provides it, e.g. `'postgres'`. Used only
            to build the error message.
        submodules: submodules to import alongside it. `import psycopg2` alone
            does not bind `psycopg2.extras`; listing it here reproduces what
            an explicit `import psycopg2.extras` would have done.

    Returns:
        The module, or a `MissingDependency` that raises on first use.
    """
    try:
        module = importlib.import_module(name)
        for submodule in submodules:
            importlib.import_module(f'{name}.{submodule}')
        return module
    except ImportError as e:
        return MissingDependency(name, extra, e)


def is_available(module) -> bool:
    """True if `module` is a real import rather than a missing-dependency stub."""
    return not isinstance(module, MissingDependency)
