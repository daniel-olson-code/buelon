# Buelon

<p align="center">
  <img src="https://raw.githubusercontent.com/daniel-olson-code/buelon/refs/heads/main/buelon/static/logo.png" alt="Buelon logo" width="50%">
</p>

Buelon is a Python orchestration system with a small scripting language (a DML) for
managing large amounts of I/O-heavy work — API calls for ETL and ELT, and other programs
that need coordinated Python and/or SQL execution.

A **hub** holds the job queue. **Workers** connect to it, pull jobs, run them, and hand the
results back. Jobs form a DAG: a job's return value becomes its children's arguments.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Command Reference](#command-reference)
- [Supported Languages](#supported-languages)
- [Learn by Example](#learn-by-example)
- [Production Notes](#production-notes)
- [Known Defects](#known-defects)
- [Future of Buelon](#future-plans)
- [In Loving Memory](#in-loving-memory)
- [License](#license)

## Installation

```bash
pip install buelon
```

That's it. This installs the `bue` CLI (`boo` and `pete` are aliases). Check the install
with `bue --version`.

Python 3.10 or newer is required.

## Quick Start

Everything below runs in one directory. Each command reads its configuration from
`.bue/settings.yaml` in the current working directory.

```bash
# 1. Create .bue/settings.yaml
bue init

# 2. (optional) edit .bue/settings.yaml -- host, port, scopes
$EDITOR .bue/settings.yaml

# 3. Start the hub. It holds the queue; leave it running.
bue hub

# 4. In another terminal, start a worker (start as many as you like)
bue worker

# 5. Upload a pipeline
bue upload -f example.bue

# 6. Watch it
bue status            # one-shot
bue status -s         # refresh every 3 seconds
bue web               # web UI on http://localhost:11011
```

A finished pipeline disappears from `bue status`: once every job in a DAG has succeeded,
the hub drops the whole DAG, so `total: 0` means "everything completed", not "nothing was
uploaded".

See [Learn by Example](#learn-by-example) for an `example.bue` / `example.py` pair that
runs as written.

## Architecture

There are two long-running processes, and both keep all their state in memory:

| process | command | what it does |
|---|---|---|
| hub | `bue hub` | Holds the job queue, the job graph, and every job's result. One per cluster. |
| worker | `bue worker` | Connects to the hub, pulls jobs, runs them, reports back. Any number. |

Every other command is a short-lived client that connects to the hub: `upload`, `status`,
`errors`, `reset`, `delete`, `run-job`, and the `web` UI.

Job results are held in the hub's memory and passed to child jobs from there. There is no
separate results store — `bue bucket` still exists as a standalone key/value server, but
nothing on the hub/worker path talks to it.

**Hub state is snapshotted to disk.** By default the hub writes `.auto_save/snapshot` every
ten minutes, plus once more on a clean shutdown (including `SIGTERM`, so `docker stop` and
`systemctl stop` are safe), and reloads it on startup. A crash loses at most one interval.

### Scopes and priority

Every job has a **scope** (a free-form name) and a **priority** (any integer; 0-100 is the
usual range, but nothing is clamped and negatives are fine). A worker only pulls jobs whose
scope is in its own `scopes` list, highest priority first. That is how you keep heavy jobs
on big machines, or stop one misbehaving pipeline from starving everything else.

## Configuration

Configuration lives in **`.bue/settings.yaml`**, relative to the directory each command is
run from. `bue init` writes one with the defaults; `bue where` prints the path it will use.

The hub/worker path reads nothing else. In particular there is no `.env` support for
hub/worker configuration and **no command-line flags for host or port** — the CLI parses
with `parse_known_args()`, so `bue hub -b 0.0.0.0:65432` is accepted and then silently
ignored. Edit the yaml.

```yaml
hub:
  host: 0.0.0.0        # interface the hub binds
  port: 65432
  encryption: faster   # faster | secure | off -- MUST match on the hub and every worker

worker:
  host: localhost      # the hub's address, as seen by this machine
  port: 65432
  scopes: production-heavy,production-small,default   # comma-separated, no spaces
  reverse: false       # pull the lowest-priority scope first instead of the highest
  info:
    name: Worker       # shown in `bue web`'s worker list

bucket:                # only used by `bue bucket`, which nothing else talks to
  server: {use: true, path: .bue/bucket, host: 0.0.0.0, port: 61535}
  client: {use: true, host: localhost, port: 61535}
  postgres: {use: false, table: buelon_bucket, persistent_path: __PERSISTENT__}

postgres:              # NOT read by anything -- see Known Defects
  host: localhost
  port: 5432
  username: XXXXX
  password: XXXXX
  database: XXXXX
```

`hub.port` and `worker.port` must match, and every client command (`upload`, `status`, …)
uses the **`worker`** block to find the hub.

#### `hub.encryption`

The wire format for every hub/worker connection. It lives under `hub:` but is read by
**both** ends — the hub and every worker, CLI command and `bue web` process — so there is
one value to keep in step rather than two that can disagree.

| value | wire format |
|---|---|
| `faster` (default) | AES-GCM. |
| `secure` | AES-GCM, then bz2 over the ciphertext. bisocket's own default. |
| `off` | Plaintext. Only on a network you fully trust. |

`secure` is *slower and bigger* than `faster` here, not safer: the bz2 pass runs after
encryption, so it compresses ciphertext, which is incompressible — a 25-job batch measures
~0.65 ms/frame of hub CPU and comes out ~26% larger on the wire. Buelon already bz2-
compresses job batches itself before they reach the transport, which is why the second pass
buys nothing. Both modes use the same AES-GCM encryption and the same `CRYPTO_KEY`.

**The hub and every worker must agree.** A mismatch is refused with `EncryptionMismatch`
naming both modes — it is not negotiated — so upgrading from a version that predates this
setting (or changing the value) means restarting the hub and all its workers together, not
a rolling restart. To upgrade without a coordinated restart, set `encryption: secure`
everywhere first, then switch to `faster` when you can take the hub down.

Leave the value empty (`encryption:`) to defer to `$BISOCKET_ENCRYPTION`, and to
bisocket's `secure` default if that is unset too.

### Environment variables

| variable | default | effect |
|---|---|---|
| `BISOCKET_ENCRYPTION` | — | Wire format, consulted only when `hub.encryption` in the yaml is left empty. Same values as that setting. |
| `CRYPTO_KEY` | an insecure built-in default | Transport encryption key. **Set this in production.** Every hub, worker and CLI invocation must use the same value; a mismatch fails the connection with `EncryptionMismatch`. |
| `BUELON_SETTINGS_PATH` | `.bue/settings.yaml` | Full path to the settings file. |
| `BUELON_DIR_PATH` | `.bue` | Directory the default settings path is built from. |
| `BUELON_AUTO_SAVE` | `true` | Set to `false` to disable hub snapshots entirely. |
| `BUELON_AUTO_SAVE_PATH` | `.auto_save` | Directory the hub snapshot is written to. |
| `BUELON_AUTO_SAVE_INTERVAL` | `600` | Seconds between snapshots. |
| `BOO_WEB_HOST` / `BOO_WEB_PORT` | `localhost` / `11011` | Where `bue web` listens. |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE` | localhost/5432/… | Connection used by `postgres` **jobs** (see [Supported Languages](#supported-languages)). Read by workers, not by the hub. |
| `ENV_PATH` | `.env` | A `.env` file at this path is loaded, if `python-dotenv` is installed. Only the variables in this table have any effect. |

A working `.bue/` directory is created in the current working directory on import
regardless of `BUELON_DIR_PATH`; the parser uses it for scratch files.

## Command Reference

```
bue init                    create .bue/settings.yaml
bue where                   print the settings.yaml path in use

bue hub                     run the hub (foreground)
bue worker                  run a worker (foreground)
bue work                    same as `bue worker`
bue run-job -j <job_id>     run one job by id, once, then exit

bue upload -f <file.bue>    build the pipeline here, send its jobs to the hub
bue submit -f <file.bue>    send the script instead, and build it on a worker
                            (-s SCOPE picks which; default: last of worker.scopes)
bue run -f <file.bue>       run a pipeline locally, start to finish, with no hub

bue status                  one-shot job counts
bue status -s               refresh every 3 seconds
bue status -s -l            ...and print a permanent line every 15 minutes
bue errors                  print every errored job with its traceback
bue reset                   move errored jobs back onto the queue
bue delete                  cancel jobs belonging to errored pipelines
bue delete --all            delete every job on the hub (prompts; -y to skip)
bue web [-o]                web UI on :11011 (-o opens a browser)

bue bucket                  run the standalone bucket server
bue example                 write example.bue / example.py / demo.py into the cwd
bue demo                    run a bucket, a hub and three workers in one process
bue joke                    a boo joke
bue --version               print the version
```

`bue repair` and `bue test` are accepted and do nothing.

**`bue worker` and `bue work` exit on their own after 20 minutes.** That is deliberate
(a periodic restart drops any leaked memory or module state), and it is not configurable.
Run them under a supervisor that restarts them — systemd with `Restart=always`, a Docker
restart policy, or a shell loop.

## Supported Languages

A job's language is the second line of its definition. Three are available:

- `python` (also `python3`, `py`)
- `sqlite3` (also `sqlite`)
- `postgres` (also `postgresql`, `pg`)

For `python`, the third line is the **function** to call. For the SQL languages it is the
**table name** the incoming rows are loaded under, and the job's code is a query against
that table; the query's result set becomes the job's return value.

> **`postgres` here means "run this job's SQL on Postgres", not "store Buelon's state in
> Postgres".** There is no Postgres backend for the hub. Postgres jobs connect using the
> `POSTGRES_*` environment variables on the worker that runs them, and require
> `psycopg2-binary` and `asyncpg`.

### Job return values

Whatever a Python job returns is sent to the hub and handed to its children, so it has to
survive JSON serialization: dicts, lists, strings, numbers, booleans, `None`. Return an
object that cannot be — a `uuid.UUID`, a `set`, a `datetime` — and that job fails with

```
job 'request' (a699…) returned a value that cannot be sent to the hub:
Object of type UUID is not JSON serializable
```

visible in `bue errors`. Only that job fails; the rest of its batch is unaffected. Convert
to a primitive (`f'{uuid.uuid4()}'`, `dt.isoformat()`, `list(s)`) before returning.

A job can also return a `Result` to control what the hub does next:

```python
from buelon.core.step import Result, StepStatus

return Result(status=StepStatus.pending)   # not ready; re-queue me and try again later
return Result(status=StepStatus.reset)     # start this chain over from its root
return Result(status=StepStatus.cancel)    # drop this chain
```

`pending` is the important one: it is how you poll a slow API without holding a worker slot
for the whole wait.

## Learn by Example

The two files below are the ones used to verify this README. Write both into the same
directory, then `bue upload -f example.bue`. (`bue example` writes a second, slightly
fuller working example into the current directory — same pipeline shape, plus a `sqlite3`
job and a `.bue/settings.yaml`.)

#### example.bue

```
# Defaults for every job in this file.
!scope default
!timeout 20 * 60

# A job definition: name, language, function/table name, then the code
# (a file path, or inline code between backticks).
accounts:
    python
    accounts
    example.py

# Or define several at once out of the same file.
import python (
    request_report as request,
    get_status as status,
    get_report
        as download
        !priority 9,
    upload_to_db as upload
) example.py

# SQL jobs take their input table under the name you give here.
manipulate_data:
    sqlite3
    some_table
    `
SELECT
    *,
    CASE WHEN sales = 0 THEN 0.0 ELSE spend / sales END AS acos
FROM some_table
`

# Pipes say what order jobs run in, and pass each job's return value to the next.
accounts_pipe = | accounts
api_pipe = request | status | download | manipulate_data | upload

# Run them. `accounts_pipe` returns a list, so each element starts its own
# `api_pipe` -- three independent chains from one job.
for account in accounts_pipe():
    api_pipe(account)
```

#### example.py

```python
import time
import uuid

from buelon.core.step import Result, StepStatus


def accounts(*args) -> list[dict]:
    """The first job in the pipeline. Takes no arguments and returns a list."""
    return [
        {'account_id': 123, 'account': 'mr. business'},
        {'account_id': 456, 'account': 'mrs. business'},
        {'account_id': 789, 'account': 'sr. business'},
    ]


def request_report(account: dict) -> dict:
    """Ask the (imaginary) API for a report. Returns whatever the next job needs."""
    return {**account, 'report_id': f'{uuid.uuid4()}', 'requested_at': time.time()}


def get_status(request: dict) -> Result | dict:
    """Poll until the report is ready.

    `StepStatus.pending` hands the job back to the hub, which re-queues it, so this
    job runs again later instead of blocking a worker for the whole wait.
    """
    if time.time() - request['requested_at'] < 10:
        return Result(status=StepStatus.pending)
    return request


def get_report(request: dict) -> list[dict]:
    """Download the report. Returns a table -- a list of flat dicts."""
    return [
        {**request, 'sales': i * 10.0, 'spend': i * 3.0}
        for i in range(1, 50)
    ]


def upload_to_db(table: list[dict]) -> None:
    """The last job. Returning None is fine."""
    print(f'uploaded {len(table)} rows for {table[0]["account"]}')
```

### Syntax notes

- **Indentation is four spaces.** Change it with `TAB = '  '` on the first line.
- `!scope`, `!priority`, `!timeout` and `!retries` set defaults for the whole file when they
  are at the left margin, and override them for one job when they are indented inside a job
  definition or attached to an `import (...)` entry.
- `!timeout` takes an arithmetic expression in seconds (`20 * 60`, `60**2 * 5`), but it must
  not contain parentheses inside an `import (...)` block — the parser counts brackets.
- A single-job pipe needs a leading `|`: `p = | accounts`.
- A pipe can be wrapped across lines in parentheses.
- Only two ways to run a pipe: `pipe()` on its own, or `for x in pipe1(): pipe2(x)`.
- **`bue upload` runs the script locally and uploads the jobs it produces.** A `.bue` file
  is a program, not a manifest: `bue upload` executes it on your machine to build the job
  graph, then sends the resulting jobs to the hub. Mostly that is just parsing — but a `for`
  loop has to know how many jobs to create, so the loop's source pipe genuinely runs
  locally, in the uploading process and working directory, and outside any `!scope`. Use
  **`bue submit`** instead to do that build on a worker (see below).
- `#` starts a comment.

### `bue upload` vs `bue submit`

|  | `bue upload` | `bue submit` |
|---|---|---|
| where the script is built | your machine | a worker, in `-s SCOPE` |
| what is sent to the hub | the finished jobs | one bootstrap job carrying the script |
| a `for` loop's source pipe | runs locally, no `!scope` / `!timeout` / `!retries` | runs as a normal job, with all three |
| needs the script's imports and referenced files | on your machine | on the worker |
| you see build errors | immediately, in your terminal | in `bue errors` |

`submit` is the one to reach for when the loop source is expensive, needs credentials or
network access your laptop does not have, or belongs on a machine in a particular scope.
`upload` is simpler and tells you about syntax errors on the spot, so it stays the default.

## Production Notes

**Security.** The hub speaks a custom encrypted protocol with no authentication: anything
that can reach the port can queue and run arbitrary code. Keep the hub and its workers on a
private network, and put anything user-facing (the `bue web` UI, an upload endpoint) in
front of it rather than exposing the hub itself. Set `CRYPTO_KEY` to a real secret on every
process — hub, workers and any machine running `bue upload` / `bue status` — or the built-in
default key is used and the transport is effectively unencrypted. `hub.encryption` picks the
wire format and must be identical on every process; `off` disables encryption entirely.

**Sizing.** One hub, N workers. Each worker runs up to 25 jobs concurrently on an asyncio
loop, so the useful number of worker processes is driven by how CPU-bound your jobs are;
for the I/O-heavy work Buelon is built for, a handful of processes per machine is plenty.
Use scopes to route heavy jobs to the machines that can take them.

**Restarts.** Workers are disposable — a worker that dies mid-job has its jobs requeued by
the hub, and it exits by itself every 20 minutes anyway, so run it under a supervisor. The
hub is not disposable: it holds the queue and every job result, and loses up to
`BUELON_AUTO_SAVE_INTERVAL` seconds of progress on an unclean stop.

**Memory.** The hub keeps every intermediate result until the whole DAG finishes. A pipeline
with an errored branch pins its results in hub memory until you run `bue reset` or
`bue delete`.

## Known Defects

- **The `postgres:` block in `settings.yaml` is not read by anything.** Postgres jobs use
  the `POSTGRES_*` environment variables instead.
- **`bue demo` starts a bucket server that nothing uses** and is not a useful demo.
- Error handling and logging work but are thin.

## Future Plans

If this project sees some love, or I just find more free time, I'd like to support more
languages like `javascript` and even compiled languages such as `rust`, `go` and `c++`,
allowing teams that write different languages to work on the same program.

Web app for logging, execution and worker management.

Add a scheduler process to allow scheduled pipelines.

Create an official programming/scripting language for parallel processing. This would be
separate from the current DML while still being designed to use the Buelon orchestration
system.

## In Loving Memory

In loving memory of Buelon Rexford Moss.

<!-- Oct 24, 1937 - Jan 22, 2025 -->

<!---
your comment goes here
and here

## Contributing
[Contributing guidelines]
-->

## License
* MIT License
