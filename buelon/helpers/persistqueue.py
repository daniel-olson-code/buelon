import os
import tempfile
import json
import threading
import collections


class JsonPersistentQueue:
    def __init__(self, path, max_size=1000):
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        # self.deque = collections.deque()
        # self.max_size = max_size
        self.path = path

        folder = os.path.dirname(path)
        if not os.path.exists(folder) and folder not in {'.', '', './', '.\\'}:
            os.makedirs(folder)

        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write('')

    def qsize(self):
        return self.file_length()  # len(self.deque)

    def append_line(self, line: str):
        with open(self.path, 'a') as f:
            f.write('\n' + line)

    def consume_first_line(self):
        first_line = None
        with tempfile.NamedTemporaryFile('r+') as f_temp:
            with open(self.path, 'r+') as f:
                for line in f:
                    if line.strip():
                        if not first_line:
                            first_line = line.strip('\n')
                        else:
                            f_temp.write(line + '\n')
                f.seek(0)
                f.truncate()
                f_temp.seek(0)
                for line in f_temp:
                    if line.strip('\n'):
                        f.write(line.strip('\n') + '\n')
        return first_line

    def file_length(self):
        with open(self.path) as f:
            return sum(1 for line in f if line.strip())

    def put(self, item):
        with self.not_empty:
            self.append_line(json.dumps(item))
            # if len(self.deque) > self.max_size:
            #     self.append_line(json.dumps(item))
            # else:
            #     self.deque.append(item)
            self.not_empty.notify()

    def get(self):
        with self.not_empty:
            while not (item := self.consume_first_line()):
                self.not_empty.wait()
            return json.loads(item)
            # while not self.deque:
            #     self.not_empty.wait()
            # item = self.deque.popleft()
            # if (next_item := self.consume_first_line()):
            #     self.deque.append(json.loads(next_item))
            # return item