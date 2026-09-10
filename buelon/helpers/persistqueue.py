import os
import tempfile
import json
import threading
import collections
import uuid

from buelon.settings import DIR_PATH


TEMP_FILE_DIR = os.path.join(DIR_PATH, 'persist_queues')


class QueueCursorError(Exception):
    """The `.pos` cursor is missing or unparseable for a queue that has data.

    Raised instead of silently resuming from position 0, which would re-consume every
    already-consumed item.
    """


def _ensure_dir(folder: str) -> None:
    """Create a queue's folder on first use rather than at import -- see
    `created_cache._ensure_dir`: a bare `import buelon` must not create state dirs.
    """
    if folder and folder not in {'.', '', './', '.\\'}:
        os.makedirs(folder, exist_ok=True)


class JsonlPersistentQueue:
    """A queue backed by a JSONL file plus a sibling `.pos` cursor file.

    Durability contract:

    - State lives in two files that must be moved, copied or removed *together*:
      `<path>` holds the items, `<path>.pos` holds how far into it we have read.
      Losing the `.pos` alone means replaying consumed items, so it is an error
      (`QueueCursorError`), not a silent rewind.
    - By default that pair goes under `<DIR_PATH>/persist_queues`, never the OS temp
      dir: the state is meant to outlive the handle, and `/tmp` is the one directory
      the OS reclaims out from under a running process. Pass `path` to place it on a
      specific volume.
    - The cursor is authoritative *in memory* for a live object and flushed to disk on
      every mutation, so one path belongs to one live object at a time. A second
      object on the same path resumes from the last flush; two concurrent objects on
      one path will diverge.
    """

    def __init__(self, path=None, max_size=1000, temp_dir=None, delete_on_close=None,
                 flush_every=1):
        """
        Args:
            path: file to back the queue. When omitted, a file is created inside
                `temp_dir` (default `<DIR_PATH>/persist_queues`) instead of the
                system temp dir, so queue files live with the rest of buelon's state.
            temp_dir: where to put the generated file. Ignored when `path` is given.
            delete_on_close: whether `close()`/`__exit__`/garbage collection removes
                the files. Defaults to True for a generated file and False for a
                caller-supplied `path` -- a named path is the caller's to keep.
            flush_every: how many mutations to buffer before writing the cursor to
                disk. 1 (the default) flushes every put/get, so a hard kill loses
                nothing. N > 1 trades durability for throughput: the cursor write is
                the dominant per-item cost, but a process killed between flushes
                resumes up to N-1 items early and re-consumes them. `close()` always
                flushes, so only an unclean exit can replay. Use it for a queue that
                is filled and drained inside one process (where a crash discards the
                whole queue anyway), not for one meant to survive a restart.
        """
        # A generated file has no owner but this object, so it is ours to delete.
        self._owns_file = not path
        self.delete_on_close = self._owns_file if delete_on_close is None else delete_on_close
        self.temp_dir = temp_dir or TEMP_FILE_DIR
        self.flush_every = max(1, int(flush_every))
        self._unflushed = 0

        if not path:
            _ensure_dir(self.temp_dir)
            path = os.path.join(self.temp_dir, f'queue_{uuid.uuid4().hex}.jsonl')

        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.path = path
        self._position_file = path + '.pos'
        self._closed = False

        _ensure_dir(os.path.dirname(path))

        # The cursor is read off disk exactly once, here, and kept in memory after
        # that. Re-reading it per operation was both two file opens per put/get and
        # the mechanism of the silent-rewind bug: a `.pos` that vanished or was
        # half-written mid-run degraded to position 0 on the very next call.
        self._state = self._load_state()

        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write('')

    def _load_state(self):
        """Load the cursor at construction, refusing to guess when it is unusable.

        Only one case legitimately starts at zero: no data file, so nothing can have
        been consumed yet. A missing or corrupt cursor next to a *non-empty* data file
        means the cursor was lost, and starting over would re-consume every item that
        was already handled -- silently, which is worse than crashing.
        """
        try:
            with open(self._position_file, 'r') as f:
                state = json.load(f)
        except FileNotFoundError:
            state = None
        except ValueError as e:
            raise QueueCursorError(
                f'{self._position_file} is not valid JSON ({e}); the cursor for '
                f'{self.path} was lost or half-written. Resuming would re-consume '
                f'already-consumed items. Remove both files to start the queue over.'
            ) from e

        if state is not None and not (isinstance(state, dict) and 'position' in state):
            raise QueueCursorError(
                f'{self._position_file} does not hold a cursor (got {state!r}); '
                f'refusing to rewind {self.path} to position 0.'
            )

        if state is None:
            try:
                has_data = os.path.getsize(self.path) > 0
            except OSError:
                has_data = False

            if has_data:
                raise QueueCursorError(
                    f'{self._position_file} is missing but {self.path} has data. The '
                    f'cursor was deleted or never moved with its data file; resuming '
                    f'would re-consume already-consumed items. Remove both files to '
                    f'start the queue over.'
                )

            state = {"position": 0, "size": 0}
            self._write_state(state, force=True)

        state.setdefault('size', 0)
        return state

    def _write_state(self, state, force=False):
        self._state = state

        # `flush_every > 1` buffers the cursor in memory; the in-memory copy is
        # authoritative for this object either way, so skipping the write only ever
        # costs an unclean exit some replay (see `flush_every` in `__init__`).
        if not force and self.flush_every > 1:
            self._unflushed += 1
            if self._unflushed < self.flush_every:
                return
        self._unflushed = 0

        # Truncate-in-place left a half-written cursor on disk if the process died
        # mid-write, and `_load_state` now refuses to guess at one. Write a sibling
        # and rename instead: `os.replace` is atomic on POSIX, so what is on disk is
        # always either the whole old cursor or the whole new one.
        tmp_path = self._position_file + '.tmp'
        try:
            with open(tmp_path, 'w') as f:
                json.dump(state, f)
            os.replace(tmp_path, self._position_file)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _read_state(self):
        # In memory, not a file read -- see `_load_state`. Callers mutate the dict they
        # get back and hand it to `_write_state`, so this returns the live object
        # rather than a copy.
        return self._state

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
        for file_path in (self.path, self._position_file, self._position_file + '.tmp'):
            try:
                os.remove(file_path)
            except (OSError, TypeError):
                pass
        self._closed = True

    def close(self):
        """Release the queue's files if it owns them. Idempotent."""
        if self._closed:
            return
        if self.delete_on_close:
            self._closed = True
            self.delete_file()
            return
        # Files we are keeping must land with an accurate cursor, or the next object
        # on this path replays whatever `flush_every` was still holding.
        if self._unflushed:
            self._write_state(self._state, force=True)
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        # Last-resort cleanup: a queue dropped without `close()` still must not leak
        # its file. Interpreter shutdown can already have torn down the globals this
        # touches, so nothing here may raise.
        try:
            self.close()
        except Exception:
            pass

    def itr(self, limit: int | None = None):
        if isinstance(limit, int):
            yield from self.limit(limit)
            return

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

    def limit(self, amount: int):
        state = self._read_state()

        with open(self.path) as f:
            f.seek(state["position"])
            count = 0
            while True:
                line = f.readline()
                if not line:  # EOF
                    break
                if not line.strip():
                    continue
                if count >= amount:
                    break
                count += 1
                yield json.loads(line.strip('\n'))
                # Update position after each successful yield
                state["position"] = f.tell()

            # After iteration, save the final state
            state["size"] = max(0, state["size"] - count)
            self._write_state(state)

    def persistent_itr(self):
        with open(self.path) as f:
            for line in f:
                if not line.strip():
                    continue
                yield json.loads(line.strip('\n'))

    def __len__(self):
        return self.qsize()

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




