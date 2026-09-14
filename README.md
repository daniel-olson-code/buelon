# Buelon

<p align="center">
  <img src="https://raw.githubusercontent.com/daniel-olson-code/buelon/refs/heads/main/buelon/static/cow-glass.jpg" alt="Buelon logo" width="50%">
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

That's it. This installs the `boo` CLI (`bue` and `pete` are aliases). Check the install
with `boo --version`.

Python 3.10 or newer is required.

## Quick Start

Everything below runs in one directory. Each command reads its configuration from
`.boo/settings.yaml` in the current working directory.

```bash
# 1. Create .boo/settings.yaml
boo init

# 2. (optional) edit .boo/settings.yaml -- host, port, scopes
$EDITOR .boo/settings.yaml

# 3. Start the hub. It holds the queue; leave it running.
boo hub

# 4. In another terminal, start a worker (start as many as you like)
boo worker

# 5. Upload a pipeline
boo upload -f example.boo

# 6. Watch it
boo status            # one-shot
boo status -s         # refresh every 3 seconds
boo web               # web UI on http://localhost:11011
```

A finished pipeline disappears from `boo status`: once every job in a DAG has succeeded,
the hub drops the whole DAG, so `total: 0` means "everything completed", not "nothing was
uploaded".

See [Learn by Example](#learn-by-example) for an `example.boo` / `example.py` pair that
runs as written.

## Architecture

There are two long-running processes, and both keep all their state in memory:

| process | command | what it does |
|---|---|---|
| hub | `boo hub` | Holds the job queue, the job graph, and every job's result. One per cluster. |
| worker | `boo worker` | Connects to the hub, pulls jobs, runs them, reports back. Any number. |

Every other command is a short-lived client that connects to the hub: `upload`, `status`,
`errors`, `reset`, `delete`, `run-job`, and the `web` UI.

Job results are held in the hub's memory and passed to child jobs from there. There is no
separate results store — `boo bucket` still exists as a standalone key/value server, but
nothing on the hub/worker path talks to it.

**Hub state is snapshotted to disk.** By default the hub writes `.auto_save/snapshot` every
ten minutes, plus once more on a clean shutdown (including `SIGTERM`, so `docker stop` and
`systemctl stop` are safe), and reloads it on startup. A crash loses at most one interval.
`BUELON_AUTO_SAVE=false` turns both halves off; `BUELON_AUTO_SAVE=load-only` restores a
snapshot without writing one back.

Note that `.auto_save/` is a **sibling** of the state directory, not inside it, so it is
untouched by anything you do to `.boo/`. A hub started in a directory holding an old
snapshot adopts the jobs in it. On every load the hub prints how long ago the snapshot was
written, and warns loudly past a week — a restored job re-enters the pipeline carrying the
payload it was built with, and a loop job re-runs the arguments frozen into it at build
time. A snapshot whose format version this hub does not recognise is refused outright and
moved aside rather than partly restored. `BUELON_MAX_SNAPSHOT_AGE` makes the age a hard
limit as well; `boo status` reports the age of the oldest job still in play.

### Scopes and priority

Every job has a **scope** (a free-form name) and a **priority** (any integer; 0-100 is the
usual range, but nothing is clamped and negatives are fine). A worker only pulls jobs whose
scope is in its own `scopes` list, highest priority first. That is how you keep heavy jobs
on big machines, or stop one misbehaving pipeline from starving everything else.

## Configuration

Configuration lives in **`.boo/settings.yaml`**, relative to the directory each command is
run from. `boo init` writes one with the defaults; `boo where` prints the path it will use.

The hub/worker path reads nothing else. In particular there is no `.env` support for
hub/worker configuration and **no command-line flags for host or port** — the CLI parses
with `parse_known_args()`, so `boo hub -b 0.0.0.0:65432` is accepted and then silently
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
  max_time: 0          # seconds before `boo worker` exits; 0 (default) = run forever
  job_timeout: 172800  # seconds a job may run without its own !timeout; 0 = no limit
  info:
    name: Worker       # shown in `boo web`'s worker list

bucket:                # only used by `boo bucket`, which nothing else talks to
  server: {use: true, path: .boo/bucket, host: 0.0.0.0, port: 61535}
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
**both** ends — the hub and every worker, CLI command and `boo web` process — so there is
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
| `BUELON_SETTINGS_PATH` | `.boo/settings.yaml` | Full path to the settings file. |
| `BUELON_DIR_PATH` | `.boo` | Directory every state file lives under — settings, the parser's scratch files, the bucket store. Setting it also disables the `.bue/` → `.boo/` migration below. |
| `BUELON_AUTO_MIGRATE` | `true` | Set to `false` to skip the one-time `.bue/` → `.boo/` copy described below. |
| `BUELON_AUTO_SAVE` | `true` | Set to `false` to disable hub snapshots entirely — nothing is written *and* nothing is restored on startup. `load-only` restores an existing snapshot but never overwrites it. |
| `BUELON_AUTO_SAVE_PATH` | `.auto_save` | Directory the hub snapshot is written to. |
| `BUELON_AUTO_SAVE_INTERVAL` | `600` | Seconds between snapshots. |
| `BUELON_RETRY_BACKOFF_BASE` | `5` | Seconds before a job's first retry. Each further attempt doubles it. `0` retries immediately. Read by the hub. |
| `BUELON_RETRY_BACKOFF_MAX` | `300` | Ceiling on that doubling, in seconds. |
| `BUELON_HANDBACK_DELAY` | `5` | Seconds a job that returns `pending` waits before it is offered again. Constant, not doubling — a poll is not a failure. `0` re-queues immediately. Read by the hub and by `boo run -f`. |
| `BUELON_MAX_JOB_AGE` | `0` (off) | Seconds a job may be old — measured from when it was *built* — and still be dispatched. An older one is recorded as an error instead of run. Off by default; a job's age says nothing on its own about whether running it is correct. |
| `BUELON_MAX_SNAPSHOT_AGE` | `0` (off) | Seconds a hub snapshot may be old and still be loaded on startup. An older one is moved aside to `snapshot.rejected-<time>` and the hub starts empty. Off by default; the age is printed on every load either way. |
| `BOO_WEB_HOST` / `BOO_WEB_PORT` | `localhost` / `11011` | Where `boo web` listens. |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE` | localhost/5432/… | Connection used by `postgres` **jobs** (see [Supported Languages](#supported-languages)). Read by workers, not by the hub. |
| `ENV_PATH` | `.env` | A `.env` file at this path is loaded, if `python-dotenv` is installed. Only the variables in this table have any effect. |

The state directory is created in the current working directory the first time something
needs it — the parser uses it for scratch files, and `boo upload` / `boo run` will make it.

**It used to be called `.bue/`.** buelon is named after Buelon Rexford Moss, whose nickname
was *boo*; `bue` was a misspelling. On startup, if a non-empty `.bue/` is present and
`.boo/` is not, buelon **copies** the one to the other and prints a line saying so. `.bue/`
is left where it is — so a downgrade still finds its state — but it stops being read from
that moment, so the two drift apart. Delete it once you are happy. Set
`BUELON_AUTO_MIGRATE=false`, or point `BUELON_DIR_PATH` somewhere explicit, to skip this.

## Command Reference

```
boo init                    create .boo/settings.yaml
boo where                   print the settings.yaml path in use

boo hub                     run the hub (foreground)
boo worker                  run a worker (foreground)
boo work                    same as `boo worker`
boo run-job -j <job_id>     run one job by id, once, then exit

boo upload -f <file.boo>    build the pipeline here, send its jobs to the hub
boo submit -f <file.boo>    send the script instead, and build it on a worker
                            (-s SCOPE picks which; default: last of worker.scopes)
boo run -f <file.boo>       run a pipeline locally, start to finish, with no hub

boo status                  one-shot job counts
boo status -s               refresh every 3 seconds
boo status -s -l            ...and print a permanent line every 15 minutes
boo errors                  print every errored job with its traceback
boo reset                   move errored jobs back onto the queue
boo delete                  cancel jobs belonging to errored pipelines
boo delete --all            delete every job on the hub (prompts; -y to skip)
boo web [-o]                web UI on :11011 (-o opens a browser)

boo bucket                  run the standalone bucket server
boo example                 write example.boo / example.py / demo.py into the cwd
boo demo                    run a bucket, a hub and three workers in one process
boo joke                    a boo joke
boo --version               print the version
```

`boo repair` and `boo test` are accepted and do nothing.

**`boo worker` runs until it is stopped.** It used to exit on its own after 20 minutes,
which was really a workaround for a worker that could stop pulling jobs and never notice;
that is fixed (BUGS.md #67), so the timer is now opt-in via `worker.max_time` (seconds; 0,
the default, means forever). Set it to e.g. `86400` if you want a daily recycle anyway.

A worker still exits by itself when something is actually wrong — one of its two internal
tasks dying, or its job slots leaking — so run it under a supervisor that restarts it:
systemd with `Restart=always`, a Docker restart policy, or a shell loop. `boo work` and
`boo run-job` are single-shot and exit when their job is done, as before.

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

visible in `boo errors`. Only that job fails; the rest of its batch is unaffected. Convert
to a primitive (`f'{uuid.uuid4()}'`, `dt.isoformat()`, `list(s)`) before returning.

A job can also return a `Result` to control what the hub does next:

```python
from buelon.core.step import Result, StepStatus

return Result(status=StepStatus.pending)   # not ready; re-queue me and try again later
return Result(status=StepStatus.reset)     # start this chain over from its root
return Result(status=StepStatus.cancel)    # drop this chain
```

`pending` is the important one: it is how you poll a slow API without holding a worker slot
for the whole wait. The hub holds the job back for `BUELON_HANDBACK_DELAY` seconds (5 by
default) before offering it again, so the poll is a poll rather than a hot loop, and counts
the hand-backs — `boo status` reports them as `handed back`, and the web UI as *Handed
Back*. There is no limit unless you set `!max_handbacks`. `boo run -f` applies the same
delay and the same ceiling, so a polling pipeline behaves locally the way it will on the
cluster; it steps over a waiting job and runs the rest of the pipeline meanwhile.

Those three are the whole list. The other `StepStatus` members — `queued`, `working`,
`success`, `error`, `unknown` — are hub bookkeeping, not things a job returns. `queued` in
particular is not a slower `pending`: it means "this job is blocked on a parent that has not
finished yet", and the hub sets and clears it as parents complete. Returning it from a job
is recorded as an error, because by the time a worker holds a job its parents are already
done, so there is nothing left to wait for. Use `pending` to be tried again.

## Learn by Example

The two files below are the ones used to verify this README. Write both into the same
directory, then `boo upload -f example.boo`. (`boo example` writes a second, slightly
fuller working example into the current directory — same pipeline shape, plus a `sqlite3`
job and a `.boo/settings.yaml`.)

#### example.boo

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
- `!scope`, `!priority`, `!timeout`, `!retries` and `!max_handbacks` set defaults for the
  whole file when they are at the left margin, and override them for one job when they are
  indented inside a job definition or attached to an `import (...)` entry.
- `!retries N` gives a failed job N further attempts. They are spaced out, not immediate:
  the hub holds the job back for 5s, then 10s, then 20s and so on, capped at 5 minutes, so
  a rate limit or a failover has time to clear. `boo status` counts the jobs currently
  waiting as `delayed`. Tune it with `BUELON_RETRY_BACKOFF_BASE` / `_MAX` on the hub.
- `!max_handbacks N` caps how many times a job may return `StepStatus.pending` before the
  hub gives up and records it as an error (`boo run -f` fails the job instead). It defaults to `0`, meaning unlimited, because
  a poll loop genuinely does not know how many turns it needs — set it only on a job that
  should not poll forever. It is a separate budget from `!retries`, which counts failures.
- `!timeout` takes an arithmetic expression in seconds (`20 * 60`, `60**2 * 5`), but it must
  not contain parentheses inside an `import (...)` block — the parser counts brackets. A job
  that declares none gets `worker.job_timeout`, 48 hours by default; set that to `0` for no
  ceiling at all. Before BUGS.md #68 the parser seeded every file with a 20-minute
  `!timeout` instead, so jobs uploaded by an older client carry 1200 seconds until they are
  re-uploaded.
- A single-job pipe needs a leading `|`: `p = | accounts`.
- A pipe can be wrapped across lines in parentheses.
- Only two ways to run a pipe: `pipe()` on its own, or `for x in pipe1(): pipe2(x)`.
- **`boo upload` runs the script locally and uploads the jobs it produces.** A `.boo` file
  is a program, not a manifest: `boo upload` executes it on your machine to build the job
  graph, then sends the resulting jobs to the hub. Mostly that is just parsing — but a `for`
  loop has to know how many jobs to create, so the loop's source pipe genuinely runs
  locally, in the uploading process and working directory, and outside any `!scope`. Use
  **`boo submit`** instead to do that build on a worker (see below).
- `#` starts a comment.

### `boo upload` vs `boo submit`

|  | `boo upload` | `boo submit` |
|---|---|---|
| where the script is built | your machine | a worker, in `-s SCOPE` |
| what is sent to the hub | the finished jobs | one bootstrap job carrying the script |
| a `for` loop's source pipe | runs locally, no `!scope` / `!timeout` / `!retries` | runs as a normal job, with all three |
| needs the script's imports and referenced files | on your machine | on the worker |
| you see build errors | immediately, in your terminal | in `boo errors` |

`submit` is the one to reach for when the loop source is expensive, needs credentials or
network access your laptop does not have, or belongs on a machine in a particular scope.
`upload` is simpler and tells you about syntax errors on the spot, so it stays the default.

## Production Notes

**Security.** The hub speaks a custom encrypted protocol with no authentication: anything
that can reach the port can queue and run arbitrary code. Keep the hub and its workers on a
private network, and put anything user-facing (the `boo web` UI, an upload endpoint) in
front of it rather than exposing the hub itself. Set `CRYPTO_KEY` to a real secret on every
process — hub, workers and any machine running `boo upload` / `boo status` — or the built-in
default key is used and the transport is effectively unencrypted. `hub.encryption` picks the
wire format and must be identical on every process; `off` disables encryption entirely.

**Sizing.** One hub, N workers. Each worker runs up to 25 jobs concurrently on an asyncio
loop, so the useful number of worker processes is driven by how CPU-bound your jobs are;
for the I/O-heavy work Buelon is built for, a handful of processes per machine is plenty.
Use scopes to route heavy jobs to the machines that can take them.

**Restarts.** Workers are disposable — a worker that dies mid-job has its jobs requeued by
the hub, and it exits on its own when it detects it can no longer make progress, so run it
under a supervisor. The hub is not disposable: it holds the queue and every job result, and loses up to
`BUELON_AUTO_SAVE_INTERVAL` seconds of progress on an unclean stop.

**Memory.** The hub keeps every intermediate result until the whole DAG finishes, and a
pipeline parked on an error keeps its chain's results for as long as the error sits there.
That is deliberate, not a leak: `boo reset` requeues the errored job *by itself*, so its
parents' results have to still be there when you fix the code two days later and re-run
it. It is also what the web UI's job tree shows you when you click into a failure.

The cost is that those results are re-serialized on every autosave. `boo status` and the
web UI report it, so it is a number you can watch rather than a surprise:

```
$ boo status
done: 0, queued: 18, errors: 4, jobs: 3, delayed: 0, holds: 0, remaining: 21, total: 21, results: 12 (~4.7 MB), staged: 0 in 0 upload(s), handed back: 0 (max 0)
```

`results` counts held job results and is deliberately outside `total` — a result is not a
job. The size is estimated from a sample, hence the `~`. Two things release
it: the pipeline finishing (a DAG that fully succeeds drops all of it), or `boo delete`,
which discards errored pipelines outright. `boo reset` only releases it if the re-run
succeeds. Errors are per pipeline, so one parked chain does not hold another one's
results.

`staged` is the other number outside `total`: the chunks of a `boo upload` that has not
finished yet. A multi-chunk upload is buffered on the hub and only enters the queue when
the whole thing commits, so those jobs are not runnable and must not count towards
`total` — but the hub is holding them in memory, and a non-zero `staged` that never moves
is a stalled uploader. An abandoned buffer is discarded when the connection drops, or
reaped after fifteen minutes if the client hangs around without sending anything.

`handed back` is the third: how many jobs still in play have returned `pending` at least
once, and the highest count among them. These *are* already counted in `jobs` and
`holds` — the number exists because a job polling an API that will never be ready looks
exactly like a job waiting its turn, and `max 4,000` on an otherwise quiet hub is the
only thing that gives it away. There is no cap by default; add `!max_handbacks N` to a
job that should give up and land in `boo errors` instead of polling forever.

## Known Defects

- **The `postgres:` block in `settings.yaml` is not read by anything.** Postgres jobs use
  the `POSTGRES_*` environment variables instead.
- **`boo demo` starts a bucket server that nothing uses** and is not a useful demo.
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
