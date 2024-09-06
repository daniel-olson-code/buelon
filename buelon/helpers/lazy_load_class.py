from __future__ import annotations

import os
import tempfile
import uuid
from typing import Any

import orjson


LAZY_LOAD_PREFIX = '__lazy_load__'
TEMP_FILE_DIR = os.path.join('.bue', 'lazy_load_classes')
os.makedirs(TEMP_FILE_DIR, exist_ok=True)


class LazyMap:
    __items: dict = None
    classes: dict[str, Any] = None
    shared_variables: dict[str, Any] = None

    def __init__(self):
        self.__items = {}
        self.classes = {}
        self.shared_variables = {}

    def add_shared_variable(self, key: str, value: Any) -> None:
        self.shared_variables[key] = value

    def quiet_remove(self, key):
        if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
            del self.__items[f'{LAZY_LOAD_PREFIX}{key}']
        if key in self.__items:
            del self.__items[key]

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return len(self.__items)

    def __getitem__(self, key):
        if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
            key = f'{LAZY_LOAD_PREFIX}{key}'
            file_path = self.__items[key]['path']
            cls = self.classes[self.__items[key]['class']]
            result = self.__items[key]['result']
            return cls.lazy_load(file_path, result, self.shared_variables)

        if key not in self.__items:
            raise KeyError(f'Key: {key} not found.')

        return self.__items[key]

    def __setitem__(self, key, value):
        if hasattr(value.__class__, 'lazy_load') and hasattr(value.__class__, 'lazy_save'):
            self.classes[value.__class__.__name__] = value.__class__
            if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
                file_path = self.__items[f'{LAZY_LOAD_PREFIX}{key}']['path']
            else:
                file_path = os.path.join(TEMP_FILE_DIR, f'lazy_bue_{uuid.uuid4().hex}')  # tempfile.NamedTemporaryFile(prefix='lazy_bue_', dir=TEMP_FILE_DIR, delete=False).name
            result = value.__class__.lazy_save(value, file_path, self.shared_variables)
            self.__items[f'{LAZY_LOAD_PREFIX}{key}'] = {'class': value.__class__.__name__, 'path': file_path, 'result': result}
            if key in self.__items:
                del self.__items[key]
            return
        self.__items[key] = value

    def __delitem__(self, key):
        if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
            key = f'{LAZY_LOAD_PREFIX}{key}'
            item = self.__items[key]
            cls = self.classes[item['class']]
            if hasattr(cls, 'lazy_delete'):
                cls.lazy_delete(item['path'], item['result'], self.shared_variables)

        del self.__items[key]

    def __del__(self):
        for key in self.__items.keys():
            if key.startswith(LAZY_LOAD_PREFIX):
                item = self.__items[key]
                cls = self.classes[item['class']]
                if hasattr(cls, 'lazy_delete'):
                    cls.lazy_delete(
                        item['path'],
                        item['result'],
                        self.shared_variables
                    )

                try:
                    os.unlink(item['path'])
                except FileNotFoundError:
                    pass

    def __contains__(self, item):
        return item in self.__items or f'{LAZY_LOAD_PREFIX}{item}' in self.__items

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()
        return False

    def values(self):
        for key in list(self.__items):
            if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
                if key in self.__items:
                    yield self.__getitem__(key[len(LAZY_LOAD_PREFIX):])
            else:
                if key in self.__items:
                    yield self.__items[key]

    def keys(self):
        for key in list(self.__items):
            if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
                yield key[len(LAZY_LOAD_PREFIX):]
            else:
                if key in self.__items:
                    yield key

    def items(self):
        for key in list(self.__items):
            if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
                key = key[len(LAZY_LOAD_PREFIX):]
                if key in self.__items:
                    yield key, self.__getitem__(key)
            else:
                if key in self.__items:
                    yield key, self.__items[key]


class LazyLoader:
    variables_to_save = None

    def __init__(self):
        if not isinstance(self.variables_to_save, (list, set, tuple)):
            raise ValueError('`variables_to_save` must be (list, set, tuple)')

    @classmethod
    def lazy_save(cls, self, path, shared_variables):
        data = {var: getattr(self, var) for var in self.variables_to_save}

        with open(path, 'wb') as f:
            f.write(orjson.dumps(data))

        for var in self.variables_to_save:
            delattr(self, var)

        return path

    @classmethod
    def lazy_load(cls, path, result, shared_variables):
        if not os.path.exists(path):
            raise FileNotFoundError(f'File not found: {path}')

        self = cls()

        with open(path, 'rb') as f:
            data = orjson.loads(f.read())

        for var, value in data.items():
            setattr(self, var, value)

        return self

    @classmethod
    def lazy_delete(cls, path, result, shared_variables):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
