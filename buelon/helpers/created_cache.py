import os
import contextlib

from buelon.settings import DIR_PATH

CREATED_INDEXES_PATH = os.path.join(DIR_PATH, 'created.cache.text')


def _ensure_dir() -> None:
    """Create the state directory on first *write*, not at import.

    This used to be an `os.makedirs` at module scope, which made a bare `import buelon`
    create the state directory as a side effect -- and so made "does the new directory
    already exist?" unanswerable for `buelon.migration`. BUGS.md #55.
    """
    parent = os.path.dirname(CREATED_INDEXES_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)


__createds = None


def get_createds():
    global __createds

    if __createds:
        return __createds

    try:
        with open(CREATED_INDEXES_PATH) as f:
            __createds = {line.strip() for line in f.readlines() if line.strip()}
        return __createds
    except:
        return set()


def add_created(index_name: str):
    global __createds
    if not check_created(index_name):
        __createds = get_createds() | {index_name}
        _ensure_dir()
        with open(CREATED_INDEXES_PATH, 'w') as f:
            f.write('\n'.join(__createds))


def check_created(index_name: str):
    return index_name in get_createds()


def is_not_created(object_name: str):
    return not check_created(object_name)


class AlreadyCreated:
    name: str
    created: bool

    def __init__(self, object_name: str):
        self.name = object_name
        self.created = check_created(object_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # had error
        if exc_type:
            return
        if not self.created:
            add_created(self.name)


@contextlib.contextmanager
def created_cache():
    global __createds
    __createds = get_createds()
    yield
    _ensure_dir()
    with open(CREATED_INDEXES_PATH, 'w') as f:
        f.write('\n'.join(__createds))






