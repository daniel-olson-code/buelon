from __future__ import annotations

import os
import tempfile
import uuid
import sqlite3
from typing import Any

import orjson


LAZY_LOAD_PREFIX = '__lazy_load__'
TEMP_FILE_DIR = os.path.join('.bue', 'lazy_load_classes')
os.makedirs(TEMP_FILE_DIR, exist_ok=True)


class LazyMap:
    # __items: dict = None
    __classes: dict[str, Any] = {}
    __shared_variables: dict[str, Any] = {}
    __can_delete = True

    __db_path: str = None
    __conn: sqlite3.Connection = None

    def __init__(self, path=None):
        # self.__items = {}
        # self.__classes = (classes or {})
        # self.__shared_variables = {}
        self.__db_path = (path or os.path.join(TEMP_FILE_DIR, f'lazy_map_{uuid.uuid1().hex}.db'))
        self.__conn = sqlite3.connect(self.__db_path, check_same_thread=False)

        # Enable WAL mode
        self.__conn.execute('PRAGMA journal_mode=WAL')

        # Increase cache size (adjust the value based on your system's available memory)
        self.__conn.execute('PRAGMA cache_size = -10000')  # 10MB cache

        # Other performance-related PRAGMAs
        self.__conn.execute('PRAGMA synchronous = NORMAL')
        # self.__conn.execute('PRAGMA temp_store = MEMORY')
        self.__conn.execute(f'PRAGMA temp_store_directory = "{TEMP_FILE_DIR}"')

        # Create table if not exists
        self.__conn.executescript("""
            CREATE TABLE IF NOT EXISTS lazy_map (
                key TEXT PRIMARY KEY,
                value ,
                path TEXT,
                class TEXT,
                result 
            );
            CREATE UNIQUE INDEX IF NOT EXISTS lazy_map_idx ON lazy_map(key);
        """)

        self.__conn.commit()

    def add_shared_variable(self, key: str, value: Any) -> None:
        self.__shared_variables[key] = value

    # def quiet_remove(self, key):
    #     if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
    #         del self.__items[f'{LAZY_LOAD_PREFIX}{key}']
    #     if key in self.__items:
    #         del self.__items[key]

    def quiet_remove(self, key):
        self.__conn.execute("DELETE FROM lazy_map WHERE key = ? or key = ?", (key, f'{LAZY_LOAD_PREFIX}{key}'))

    def enable_deletion(self):
        self.__can_delete = True

    def disable_deletion(self):
        self.__can_delete = False

    def __iter__(self):
        return self.keys()

    # def __len__(self):
    #     return len(self.__items)

    def __len__(self):
        return self.__conn.execute("SELECT COUNT(*) FROM lazy_map").fetchone()[0]

    # def __getitem__(self, key):
    #     if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
    #         key = f'{LAZY_LOAD_PREFIX}{key}'
    #         file_path = self.__items[key]['path']
    #         cls = self.__classes[self.__items[key]['class']]
    #         result = self.__items[key]['result']
    #         return cls.lazy_load(file_path, result, self.__shared_variables)
    #
    #     if key not in self.__items:
    #         raise KeyError(f'Key: {key} not found.')
    #
    #     return self.__items[key]

    def get(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            return None

    def __getitem__(self, key):
        cursor = self.__conn.execute("SELECT key, value, path, class, result FROM lazy_map WHERE key = ? or key = ?", (key, f'{LAZY_LOAD_PREFIX}{key}'))
        row = cursor.fetchone()

        if row is None:
            raise KeyError(f'Key: {key} not found.')

        key, value, file_path, cls_name, result = row

        if key.startswith(LAZY_LOAD_PREFIX):
            cls = self.__classes[cls_name]
            return cls.lazy_load(file_path, result, self.__shared_variables)

        return orjson.loads(value)

    # def __setitem__(self, key, value):
    #     if hasattr(value.__class__, 'lazy_load') and hasattr(value.__class__, 'lazy_save'):
    #         self.__classes[value.__class__.__name__] = value.__class__
    #         if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
    #             file_path = self.__items[f'{LAZY_LOAD_PREFIX}{key}']['path']
    #         else:
    #             file_path = os.path.join(TEMP_FILE_DIR, f'lazy_bue_{uuid.uuid4().hex}')  # tempfile.NamedTemporaryFile(prefix='lazy_bue_', dir=TEMP_FILE_DIR, delete=False).name
    #         result = value.__class__.lazy_save(value, file_path, self.__shared_variables)
    #         self.__items[f'{LAZY_LOAD_PREFIX}{key}'] = {'class': value.__class__.__name__, 'path': file_path, 'result': result}
    #         if key in self.__items:
    #             del self.__items[key]
    #         return
    #     self.__items[key] = value

    def __setitem__(self, key, value):
        if hasattr(value.__class__, 'lazy_load') and hasattr(value.__class__, 'lazy_save'):
            self.__classes[value.__class__.__name__] = value.__class__

            row = self.__conn.execute("SELECT path FROM lazy_map WHERE key = ?", (f'{LAZY_LOAD_PREFIX}{key}',)).fetchone()

            if row:
                file_path = row[0]
            else:
                file_path = os.path.join(TEMP_FILE_DIR, f'lazy_bue_{uuid.uuid4().hex}')

            result = value.__class__.lazy_save(value, file_path, self.__shared_variables)

            self.__conn.execute("INSERT OR REPLACE INTO lazy_map (key, value, path, class, result) VALUES (?, ?, ?, ?, ?)", (f'{LAZY_LOAD_PREFIX}{key}', None, file_path, value.__class__.__name__, result))

            self.__conn.execute("DELETE FROM lazy_map WHERE key = ?", (key,))
            self.__conn.commit()
            return

        self.__conn.execute("INSERT OR REPLACE INTO lazy_map (key, value) VALUES (?, ?)", (key, orjson.dumps(value)))
        self.__conn.commit()

    # def __delitem__(self, key):
    #     if f'{LAZY_LOAD_PREFIX}{key}' in self.__items:
    #         key = f'{LAZY_LOAD_PREFIX}{key}'
    #         item = self.__items[key]
    #         cls = self.__classes[item['class']]
    #         if hasattr(cls, 'lazy_delete'):
    #             cls.lazy_delete(item['path'], item['result'], self.__shared_variables)
    #
    #     del self.__items[key]

    def __delitem__(self, key):
        row = self.__conn.execute("SELECT key, path, class, result FROM lazy_map WHERE key = ? or key = ?", (key, f'{LAZY_LOAD_PREFIX}{key}')).fetchone()
        if row is None:
            raise KeyError(f'Key: {key} not found.')

        key, file_path, cls_name, result = row

        if key.startswith(LAZY_LOAD_PREFIX):
            cls = self.__classes[cls_name]
            if hasattr(cls, 'lazy_delete'):
                cls.lazy_delete(file_path, result, self.__shared_variables)

        self.__conn.execute("DELETE FROM lazy_map WHERE key = ? or key = ?", (key, f'{LAZY_LOAD_PREFIX}{key}'))
        self.__conn.commit()
    # def __del__(self):
    #     for key in self.__items.keys():
    #         if key.startswith(LAZY_LOAD_PREFIX):
    #             item = self.__items[key]
    #             cls = self.__classes[item['class']]
    #             if hasattr(cls, 'lazy_delete'):
    #                 cls.lazy_delete(
    #                     item['path'],
    #                     item['result'],
    #                     self.__shared_variables
    #                 )
    #
    #             try:
    #                 os.unlink(item['path'])
    #             except FileNotFoundError:
    #                 pass

    def __del__(self):
        if not self.__can_delete:
            return

        with self.__conn:
            for key, file_path, cls_name, result in self.__conn.execute("SELECT key, path, class, result FROM lazy_map").fetchall():
                if key.startswith(LAZY_LOAD_PREFIX):
                    cls = self.__classes[cls_name]
                    if hasattr(cls, 'lazy_delete'):
                        cls.lazy_delete(file_path, result, self.__shared_variables)

                    try:
                        os.unlink(file_path)
                    except FileNotFoundError:
                        pass
        try:
            os.unlink(self.__db_path)
        except FileNotFoundError:
            pass

    # def __contains__(self, item):
    #     return item in self.__items or f'{LAZY_LOAD_PREFIX}{item}' in self.__items

    def __contains__(self, item):
        with self.__conn:
            return self.__conn.execute("SELECT COUNT(*) FROM lazy_map WHERE key = ? or key = ?", (item, f'{LAZY_LOAD_PREFIX}{item}')).fetchone()[0] > 0

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()
        return False

    # def values(self):
    #     for key in list(self.__items):
    #         if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
    #             if key in self.__items:
    #                 yield self.__getitem__(key[len(LAZY_LOAD_PREFIX):])
    #         else:
    #             if key in self.__items:
    #                 yield self.__items[key]

    def values(self):
        for _, value in self.items():
            yield value

    # def keys(self):
    #     for key in list(self.__items):
    #         if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
    #             yield key[len(LAZY_LOAD_PREFIX):]
    #         else:
    #             if key in self.__items:
    #                 yield key

    def keys(self):
        with self.__conn:
            for key, _ in self.__conn.execute("SELECT key, path FROM lazy_map").fetchall():
                if key.startswith(LAZY_LOAD_PREFIX):
                    yield key[len(LAZY_LOAD_PREFIX):]
                else:
                    yield key

    # def items(self):
    #     for key in list(self.__items):
    #         if isinstance(key, str) and key.startswith(LAZY_LOAD_PREFIX):
    #             key = key[len(LAZY_LOAD_PREFIX):]
    #             if key in self.__items:
    #                 yield key, self.__getitem__(key)
    #         else:
    #             if key in self.__items:
    #                 yield key, self.__items[key]

    def items(self):
        with self.__conn:
            for key, value, file_path, cls_name, result in self.__conn.execute("SELECT key, value, path, class, result FROM lazy_map").fetchall():
                if key.startswith(LAZY_LOAD_PREFIX):
                    cls = self.__classes[cls_name]
                    yield key[len(LAZY_LOAD_PREFIX):], cls.lazy_load(file_path, result, self.__shared_variables)
                else:
                    yield key, orjson.loads(value)

    @classmethod
    def lazy_save(cls, self, path, shared_variables):
        return self.__db_path

    @classmethod
    def lazy_load(cls, path, result, shared_variables):
        lm = LazyMap(result)
        lm.disable_deletion()
        return lm

    @classmethod
    def lazy_delete(cls, path, result, shared_variables):
        LazyMap(result).__del__()


class LazyLoader:
    variables_to_save = None

    def __init__(self):
        if not isinstance(self.variables_to_save, list):
            raise ValueError('`variables_to_save` must be list')

    @classmethod
    def lazy_save(cls, self, path, shared_variables):
        data = {var: getattr(self, var) for var in self.variables_to_save}
        data['variables_to_save'] = self.variables_to_save

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

        self.variables_to_save = data['variables_to_save']

        for var, value in data.items():
            setattr(self, var, value)

        return self

    @classmethod
    def lazy_delete(cls, path, result, shared_variables):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
