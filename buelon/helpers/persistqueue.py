import os
import tempfile
import json
import threading
import collections


class JsonlPersistentQueue:
    def __init__(self, path=None, max_size=1000):
        if not path:
            path = tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False).name

        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.path = path
        self._position_file = path + '.pos'

        folder = os.path.dirname(path)
        if not os.path.exists(folder) and folder not in {'.', '', './', '.\\'}:
            os.makedirs(folder)

        # Create or load position and size
        if not os.path.exists(self._position_file):
            self._write_state({"position": 0, "size": 0})

        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write('')

    def _write_state(self, state):
        with open(self._position_file, 'w') as f:
            json.dump(state, f)

    def _read_state(self):
        try:
            with open(self._position_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, ValueError):
            return {"position": 0, "size": 0}

    def append_line(self, line: str):
        state = self._read_state()
        with open(self.path, 'a') as f:
            f.write(line + '\n')
        # Increment size when adding new item
        state["size"] += 1
        self._write_state(state)

    def consume_first_line(self):
        state = self._read_state()

        with open(self.path, 'r') as f:
            # Skip to our current position
            f.seek(state["position"])

            # Find next non-empty line
            while True:
                start_pos = f.tell()
                line = f.readline()

                if not line:  # EOF
                    return None

                if line.strip():
                    # Update position and decrement size
                    state["position"] = f.tell()
                    state["size"] = max(0, state["size"] - 1)  # Ensure size never goes negative
                    self._write_state(state)
                    return line.strip('\n')

    def qsize(self):
        state = self._read_state()
        return state["size"]

    def cleanup(self):
        """Remove consumed lines from the file and reset position."""
        state = self._read_state()

        # If we're still at the start, no cleanup needed
        if state["position"] == 0:
            return

        remaining_content = []
        with open(self.path, 'r') as f:
            f.seek(state["position"])
            remaining_content = f.readlines()

        # Write only remaining content and reset position
        with open(self.path, 'w') as f:
            f.writelines(line for line in remaining_content if line.strip())

        # Size remains the same, only position resets
        state["position"] = 0
        self._write_state(state)

    def put(self, item):
        with self.not_empty:
            self.append_line(json.dumps(item))
            self.not_empty.notify()

    def get(self):
        with self.not_empty:
            while not (item := self.consume_first_line()):
                self.not_empty.wait()
            return json.loads(item)

    def delete_file(self):
        try:
            os.remove(self.path)
        except:
            pass
        try:
            os.remove(self._position_file)
        except:
            pass

    def itr(self):
        state = self._read_state()

        with open(self.path) as f:
            f.seek(state["position"])
            count = 0
            for line in f:
                if not line.strip():
                    continue
                count += 1
                yield json.loads(line.strip('\n'))

            # After iteration, assume all content is consumed
            state["position"] = f.tell()
            state["size"] = max(0, state["size"] - count)  # Adjust size based on consumed items
            self._write_state(state)

    def persistent_itr(self):
        with open(self.path) as f:
            for line in f:
                if not line.strip():
                    continue
                yield json.loads(line.strip('\n'))

# class JsonlPersistentQueue:
#     """
#     More performant.
#
#     """
#     def __init__(self, path, max_size: int = 1000):
#         """
#
#         Args:
#             max_size (int): Deprecated, does nothing
#         """
#         self.mutex = threading.Lock()
#         self.not_empty = threading.Condition(self.mutex)
#         self.path = path
#         self._position_file = path + '.pos'
#
#         folder = os.path.dirname(path)
#         if not os.path.exists(folder) and folder not in {'.', '', './', '.\\'}:
#             os.makedirs(folder)
#
#         # Create or load position
#         if not os.path.exists(self._position_file):
#             self._write_position(0)
#
#         if not os.path.exists(path):
#             with open(path, 'w') as f:
#                 f.write('')
#
#     def _write_position(self, pos):
#         with open(self._position_file, 'w') as f:
#             f.write(str(pos))
#
#     def _read_position(self):
#         try:
#             with open(self._position_file, 'r') as f:
#                 return int(f.read().strip() or '0')
#         except (FileNotFoundError, ValueError):
#             return 0
#
#     def append_line(self, line: str):
#         with open(self.path, 'a') as f:
#             f.write(line + '\n')
#
#     def consume_first_line(self):
#         current_pos = self._read_position()
#
#         with open(self.path, 'r') as f:
#             # Skip to our current position
#             f.seek(current_pos)
#
#             # Find next non-empty line
#             while True:
#                 start_pos = f.tell()
#                 line = f.readline()
#
#                 if not line:  # EOF
#                     return None
#
#                 if line.strip():
#                     # Update position file to point after this line
#                     self._write_position(f.tell())
#                     return line.strip('\n')
#
#     def cleanup(self):
#         """Remove consumed lines from the file and reset position."""
#         current_pos = self._read_position()
#
#         # If we're still at the start, no cleanup needed
#         if current_pos == 0:
#             return
#
#         remaining_content = []
#         with open(self.path, 'r') as f:
#             f.seek(current_pos)
#             remaining_content = f.readlines()
#
#         # Write only remaining content and reset position
#         with open(self.path, 'w') as f:
#             f.writelines(line for line in remaining_content if line.strip())
#
#         self._write_position(0)
#
#     def file_length(self):
#         current_pos = self._read_position()
#         count = 0
#
#         with open(self.path) as f:
#             f.seek(current_pos)
#             count = sum(1 for line in f if line.strip())
#
#         return count
#
#     def put(self, item):
#         with self.not_empty:
#             self.append_line(json.dumps(item))
#             self.not_empty.notify()
#
#     def get(self):
#         with self.not_empty:
#             while not (item := self.consume_first_line()):
#                 self.not_empty.wait()
#             return json.loads(item)
#
#     def delete_file(self):
#         try:
#             os.remove(self.path)
#             os.remove(self._position_file)
#         except FileNotFoundError:
#             pass
#
#     def itr(self):
#         current_pos = self._read_position()
#
#         with open(self.path) as f:
#             f.seek(current_pos)
#             for line in f:
#                 if not line.strip():
#                     continue
#                 yield json.loads(line.strip('\n'))
#
#         # After iteration, assume all content is consumed
#         self._write_position(f.tell())


# Original Name
JsonPersistentQueue = JsonlPersistentQueue


# # ORIGINAL
# class JsonPersistentQueue:
#     def __init__(self, path, max_size=1000):
#         self.mutex = threading.Lock()
#         self.not_empty = threading.Condition(self.mutex)
#         # self.deque = collections.deque()
#         # self.max_size = max_size
#         self.path = path
#
#         folder = os.path.dirname(path)
#         if not os.path.exists(folder) and folder not in {'.', '', './', '.\\'}:
#             os.makedirs(folder)
#
#         if not os.path.exists(path):
#             with open(path, 'w') as f:
#                 f.write('')
#
#     def qsize(self):
#         return self.file_length()  # len(self.deque)
#
#     def append_line(self, line: str):
#         with open(self.path, 'a') as f:
#             f.write('\n' + line)
#
#     def consume_first_line(self):
#         """Efficiently consume and return the first non-empty line from the file."""
#         if not os.path.getsize(self.path):
#             return None
#
#         first_line = None
#         with tempfile.NamedTemporaryFile('w', delete=False) as temp_file:
#             temp_path = temp_file.name
#             with open(self.path, 'r') as f:
#                 for line in f:
#                     if line.strip() and first_line is None:
#                         first_line = line.strip('\n')
#                     elif line.strip():
#                         temp_file.write(line)
#         # Replace the original file with the temp file
#         os.replace(temp_path, self.path)
#         return first_line
#
#         # with open(self.path, 'r+') as f:
#         #     # Read first non-empty line
#         #     while True:
#         #         pos = f.tell()
#         #         line = f.readline()
#         #         if not line:  # EOF
#         #             return None
#         #         if line.strip():
#         #             first_line = line.strip('\n')
#         #             break
#         #         # Continue if empty line
#         #
#         #     # Read the rest of the file in chunks and shift content up
#         #     buffer_size = 8192  # 8KB chunks
#         #     shift_pos = pos
#         #     next_pos = f.tell()
#         #
#         #     while True:
#         #         chunk = f.read(buffer_size)
#         #         if not chunk:
#         #             break
#         #
#         #         # Move file pointer back to write position
#         #         f.seek(shift_pos)
#         #         f.write(chunk)
#         #         shift_pos = f.tell()
#         #
#         #         # Move to next read position
#         #         f.seek(next_pos)
#         #         next_pos = f.tell()
#         #
#         #     # Truncate the file to remove the shifted content
#         #     f.truncate(shift_pos)
#         #
#         # return first_line
#
#     # def consume_first_line(self):
#     #     first_line = None
#     #     with tempfile.NamedTemporaryFile('r+') as f_temp:
#     #         with open(self.path, 'r+') as f:
#     #             for line in f:
#     #                 if line.strip():
#     #                     if not first_line:
#     #                         first_line = line.strip('\n')
#     #                     else:
#     #                         f_temp.write(line + '\n')
#     #             f.seek(0)
#     #             f.truncate()
#     #             f_temp.seek(0)
#     #             for line in f_temp:
#     #                 if line.strip('\n'):
#     #                     f.write(line.strip('\n') + '\n')
#     #     return first_line
#
#     def file_length(self):
#         with open(self.path) as f:
#             return sum(1 for line in f if line.strip())
#
#     def put(self, item):
#         with self.not_empty:
#             self.append_line(json.dumps(item))
#             # if len(self.deque) > self.max_size:
#             #     self.append_line(json.dumps(item))
#             # else:
#             #     self.deque.append(item)
#             self.not_empty.notify()
#
#     def get(self):
#         with self.not_empty:
#             while not (item := self.consume_first_line()):
#                 self.not_empty.wait()
#             return json.loads(item)
#             # while not self.deque:
#             #     self.not_empty.wait()
#             # item = self.deque.popleft()
#             # if (next_item := self.consume_first_line()):
#             #     self.deque.append(json.loads(next_item))
#             # return item
#
#     def delete_file(self):
#         try:
#             os.remove(self.path)
#         except FileNotFoundError:
#             pass
#
#     def itr(self):
#         with open(self.path) as f:
#             for line in f:
#                 if not line.strip():
#                     continue
#                 yield json.loads(line.strip('\n'))
#
#         with open(self.path, 'w') as f:
#             f.write('')




