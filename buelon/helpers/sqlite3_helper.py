"""

This gives a little more functionality to the python library sqlite3.
This is made more  for developers who do not know, or work with many data sets
who do not have the time to study and implement the data structure of every single one,
for various reasons.

The main focus is on the SQL class and it's two functions download_table and upload_table.

Create the class as follows:
`
db = SQL({'location': 'my.db'})

# or
db = SQL()
db.location = './my.db'

# or
db = SQL()
db['location'] = 'my.db'
`


The JsonObject class allows us to simply turn our `db` into a json or "class json"
Example:
`
import json

# save db
with open('my_db_config.json', 'w') as f:
    json.dump(db.cjson, f)

# load db
with open('my_db_config.json', 'r') as f:
    db = SQL(json.load(f))
`


You can simply upload and download tables from the database.
Example:
`
# upload to table if the table does not exists it creates the table.
# columns are added automatically if they are not in the table.
table = [
    {'a': 1, 'b': '23', 'c': False},
    {'a': 45, 'b': 90, 'c': True}
]
db.upload_table('my_table', table)

new_columns = [
    {'a': 3},
]

# download table
table = db.download_table('my_table')
`




"""
import os
import json
import datetime
import sqlite3


# home = os.path.expanduser('~')
default_db_location = os.path.join('.bue', 'sqlite3_helper', 'pipe.db')  # os.path.join(home, '.sqlite3_helper', 'my.db')

if not os.path.exists(default_db_location):
    os.makedirs(os.path.dirname(default_db_location), exist_ok=True)


def change_column_type():
    """ALTER TABLE your_main_table
       ADD COLUMN new_column_name new_column_data_type
    UPDATE your_main_table
           SET new_column_name = CAST(old_column_name as new_data_type_you_want)"""


def check_cell(v):
    """
    This will check the data type of the cell and fix it if needed.
    Any data type not recognised is turn into a str as follows: `return f'{value}'   `
    :param v:
    :return:
    """
    if isinstance(v, (type(None), int, float, str, datetime.datetime, datetime.date)):
        return v
    if isinstance(v, (list, dict, tuple)):
        return json.dumps(v)
    if isinstance(v, (set, frozenset)):
        return json.dumps(list(v))
    return f'{v}'


def fix_table_headers(table: list[dict], order: list[str]=None) -> list[dict]:
    """
    This will iterate through the table and fix the headers.
    If a header is not in the row it will be added with a `None` value.
    Also, each value is passed through the `check_cell` function
    to ensure it has correct data type.

    :param table:
    :param order: This will override the order of the headers
    :return: list[dict]
    """
    headers = set()
    for row in table:
        headers.update(row.keys())
    for i, row in enumerate(table):
        for h in headers:
            if h not in row:
                row[h] = None
            else:
                row[h] = check_cell(row[h])
        if order:
            table[i] = {k: row[k] for k in order}
    return table


class Sqlite3:
    location: str = default_db_location

    def __init__(self, location: str = None):
        self.location = location if isinstance(location, str) else self.location
        self.check_location()

    def check_location(self):
        if not os.path.exists(self.location):
            if os.path.dirname(self.location) not in {'', '.', '/', '\\', './', '.\\'}:
                os.makedirs(os.path.dirname(self.location), exist_ok=True)

    def query(self, q, *args, commit=True, cur=None):
        with sqlite3.connect(self.location) as conn:
            cur = conn.cursor()
            cur.execute(q, args)
            r = None
            try:
                r = cur.fetchall()
            except:
                pass
            if commit:
                conn.commit()
            return r

    def columns(self, name: str):
        with sqlite3.connect(self.location) as conn:
            c = conn.cursor()
            c.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{name}');")
            return [row[0] for row in c.fetchall()]

    def upload_table(self, name: str, table: list[dict], id_column: str = None):
        if len(table):
            with sqlite3.connect(self.location) as conn:
                c = conn.cursor()
                q = (f'CREATE TABLE IF NOT EXISTS {name} ('+", ".join([
                        f'"{h}"' if h != id_column else f'"{h}" primary key'
                        for h in table[0].keys()
                    ])
                    + (f'CONSTRAINT constraint_name PRIMARY KEY ('+", ".join([f'"{h}"' for h in id_column])+')' if isinstance(id_column, list) else '')
                    + ')')
                # print('query: ', q)
                c.execute(q)
                conn.commit()
                table = fix_table_headers(table)
                cols = set(self.columns(name))
                for h in set(table[0].keys()) | cols:
                    if h not in cols:
                        try:
                            c.execute(f'ALTER TABLE {name} ADD COLUMN {h};')
                            conn.commit()
                        except sqlite3.OperationalError:  # str(e) == f'duplicate column name: {h}'
                            pass
                cols = self.columns(name)
                table = [tuple((row[h] if h in row else None) for h in cols) for row in table]
                # c.executemany(f'INSERT INTO {name} VALUES ({", ".join(["?"]*len(cols))});', table)
                c.executemany(f'''INSERT OR REPLACE INTO {name} ({", ".join([f'"{h}"' for h in cols])}) VALUES ({", ".join(["?"]*len(cols))});''', table)
                conn.commit()
                return c.rowcount

    def download_table(self, name: str = None, sql: str = None, parameters=None):
        with sqlite3.connect(self.location) as conn:
            c = conn.cursor()

            c.execute(sql or f'SELECT * FROM {name};', parameters if parameters else tuple())
            headers = [row[0] for row in c.description]
            return [dict(zip(headers, row)) for row in c.fetchall()]

    @property
    def conn(self):
        return sqlite3.connect(self.location)

