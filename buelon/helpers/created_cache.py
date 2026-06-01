import os
import contextlib

os.makedirs('.bue', exist_ok=True)
CREATED_INDEXES_PATH = os.path.join('.bue', 'created.cache.text')

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
    with open(CREATED_INDEXES_PATH, 'w') as f:
        f.write('\n'.join(__createds))






