import os
import sys
import shlex
import asyncio
import webbrowser
import subprocess
import importlib.resources as resources

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from pydantic import BaseModel

import buelon
from buelon.hub import BiWorkerClient, settings, compressed_message_to_steps
from buelon.web_history import ALLOWED_INTERVALS, HistoryStore

app = FastAPI()
worker_client: BiWorkerClient | None = None
settings.worker.info['name'] = f"Web App ({settings.worker.info.get('name', 'Unknown')})"

# `worker_client` is a single shared `BiWorkerClient` over ONE bisocket connection, and
# every route reaches for it. Two coroutines calling it at once interleave frames on
# that socket, and the reconnect path is worse still: both race to re-enter
# `__aenter__` and stomp the global. Harmless-looking while only a browser talked to
# it; a fact the moment the history sampler started firing on a timer next to live
# requests. So every use of the client -- routes and sampler alike -- goes through
# `with_client`, and reconnection happens exactly once, under the lock.
# Created on first use rather than at import: an `asyncio.Lock` binds to the loop that
# first awaits it, so an import-time lock would be tied to a loop that `asyncio.run`
# may already have closed (and raise "bound to a different event loop").
_client_lock: asyncio.Lock | None = None
_client_lock_loop = None

history = HistoryStore()
_sampler_task: asyncio.Task | None = None


def client_lock() -> asyncio.Lock:
    global _client_lock, _client_lock_loop
    loop = asyncio.get_running_loop()
    if _client_lock is None or _client_lock_loop is not loop:
        _client_lock = asyncio.Lock()
        _client_lock_loop = loop
    return _client_lock


async def with_client(call):
    """Run `call(client)` under the client lock, reconnecting once if it fails."""
    global worker_client
    async with client_lock():
        assert worker_client is not None
        try:
            return await call(worker_client)
        except Exception:
            worker_client = await worker_client.__aenter__()
            return await call(worker_client)


def get_static_file(filename: str) -> str:
    # Opens the file as text
    with resources.files("buelon.static").joinpath(filename).open("r", encoding="utf-8") as f:
        return f.read()


def get_static_path(filename: str) -> str:
    # Gets the actual filesystem path (works even if installed in venv)
    return str(resources.files("buelon.static").joinpath(filename))


def get_static_dir() -> str:
    # The static directory itself, as a filesystem path. Taken as the parent of a
    # known member rather than str() of the package: `buelon.static` used to be a
    # namespace package, whose MultiplexedPath str()s to a repr instead of a path
    # (it has an `__init__.py` now -- BUGS.md #26 -- but this form works either way).
    return os.path.dirname(get_static_path("index.html"))


# Served by StaticFiles rather than a hand-rolled route: it confines every
# request to the static directory, so `..` segments cannot escape it.
app.mount("/static", StaticFiles(directory=get_static_dir()), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    return get_static_file("index.html")


@app.post("/data")
async def get_data():
    data = await with_client(lambda client: client.get_web_info(True))
    return JSONResponse(content=data)


@app.post("/errors")
async def get_errors():
    compressed_jobs, error_info = await with_client(lambda client: client.errors())
    jobs = compressed_message_to_steps(compressed_jobs)
    json_jobs = [job.to_json() for job in jobs]
    data = {
        "jobs": json_jobs,
        "errors": error_info
    }
    return JSONResponse(content=data)


@app.post('/reset-errors')
async def reset_errors():
    await with_client(lambda client: client.reset_errors())
    return JSONResponse(content={'status': 'success'})


class Job(BaseModel):
    id: str


@app.post("/job-parents-and-results")
async def api_job_parents_and_results(job: Job):
    data = await with_client(
        lambda client: client.get_job_parents_and_results(job.id)
    )
    return JSONResponse(content=data)


class HistoryQuery(BaseModel):
    # Both optional, so `POST /history` with no body means "everything you have".
    limit: int | None = None
    since: float | None = None


class HistoryConfig(BaseModel):
    interval_minutes: int


@app.post("/history")
async def get_history(query: HistoryQuery | None = None):
    """The recorded series plus the sampler's config. Additive -- nothing else moved.

    `ts` is unix seconds, deliberately unformatted: the server has no idea what
    timezone the operator is in. Samples are oldest-first and MAY be unevenly spaced
    (the interval is changeable), and a sample carrying `error` instead of `counts` is
    a real gap in the record, not a bug to filter out.
    """
    query = query or HistoryQuery()
    return JSONResponse(content=history.payload(limit=query.limit, since=query.since))


@app.post("/history/config")
async def set_history_config(config: HistoryConfig):
    """Set the sampling cadence, server-side, for every browser pointed at this server."""
    try:
        new_config = history.set_interval(config.interval_minutes)
    except ValueError as e:
        return JSONResponse(content={"detail": str(e)}, status_code=400)
    except OSError as e:
        return JSONResponse(
            content={"detail": f"could not persist the interval: {e}"}, status_code=500
        )
    return JSONResponse(content={"config": new_config})


async def sample_history():
    """What the sampler records: counts, and how many workers were connected.

    `workers_info=True` costs a worker list the sample throws away, but the worker
    *count* is the whole reason history can answer "every worker vanished at 3am", and
    the hub has no cheaper call for it. At one call per interval (10 minutes by
    default) that is noise next to the dashboard's own 30-second refresh.
    """
    return await with_client(lambda client: client.get_web_info(True))


async def stream_subprocess_logs(cmd_parts):
    """
    Asynchronously runs a command and streams its stdout and stderr.
    This version uses an asyncio.Queue to correctly merge the two streams.

    Cancellation is real. When the client goes away -- the web console's Stop
    button aborts the fetch, or the operator simply closes the tab -- ASGI
    cancels this generator and the `finally` below kills the subprocess.
    Without it every abandoned run left an orphaned worker process behind,
    still executing the step against real credentials with nobody watching.
    """
    process = await asyncio.create_subprocess_exec(
        *cmd_parts,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    queue = asyncio.Queue()

    # This task reads from a stream (stdout or stderr) and puts lines into the queue.
    async def reader_task(stream):
        while True:
            line = await stream.readline()
            if not line:
                break
            await queue.put(line)
        # When the stream is finished, put a sentinel value in the queue.
        await queue.put(None)

    # Start tasks for both stdout and stderr
    stdout_reader = asyncio.create_task(reader_task(process.stdout))
    stderr_reader = asyncio.create_task(reader_task(process.stderr))

    try:
        finished_streams = 0
        while finished_streams < 2:
            line = await queue.get()
            if line is None:
                # A stream has finished.
                finished_streams += 1
            else:
                yield line.decode('utf-8', errors='replace')

        # Wait for the process and reader tasks to complete.
        await process.wait()
        await asyncio.gather(stdout_reader, stderr_reader)
    finally:
        # Cleanup must stay synchronous: on GeneratorExit an `await` here
        # raises "async generator ignored GeneratorExit". Killing is enough --
        # the event loop's child watcher reaps the process.
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        stdout_reader.cancel()
        stderr_reader.cancel()


@app.post("/run-job")
async def api_run_job(job: Job):
    # # assert worker_client is not None
    # # Construct the command to run the buelon worker for a specific job
    # # Using repr() ensures the job.id string is correctly quoted
    # # The `-u` flag is important for unbuffered output, which is ideal for streaming
    # import_txt = 'import buelon'
    # new_name = f"Web App Subprocess ({settings.worker.info['name']})"
    # rename_worker_txt = f"buelon.settings.settings.worker.info['name'] = {repr(new_name)}"
    # execution_txt = f'buelon.worker.work({repr(job.id)})'
    # command = f'{sys.executable} -u -c "{import_txt};{rename_worker_txt};{execution_txt}"'
    import_txt = 'import buelon'
    new_name = f"Web App Subprocess ({settings.worker.info.get('name', 'Unknown')})"
    rename_worker_txt = f"buelon.settings.settings.worker.info['name'] = {repr(new_name)}"
    execution_txt = f'buelon.worker.work({repr(job.id)})'

    # Build the Python code first, then properly quote it for the shell
    python_code = f"{import_txt};{rename_worker_txt};{execution_txt}"
    command = f'{sys.executable} -u -c {shlex.quote(python_code)}'

    cmd_parts = shlex.split(command)

    return StreamingResponse(stream_subprocess_logs(cmd_parts), media_type="text/plain")


async def start_app(open_browser: bool = False):
    global worker_client, _sampler_task
    try:
        port = int(os.environ.get('BOO_WEB_PORT', 11011))
    except:
        port = 11011

    host = os.environ.get('BOO_WEB_HOST', "localhost")

    if open_browser:
        webbrowser.open(f"http://{host}:{port}")

    # async with BiWorkerClient() as client:
    #     worker_client = client
    #     # Start FastAPI with uvicorn
    #     config = uvicorn.Config(app, host=host, port=port, log_level="info")
    #     server = uvicorn.Server(config)
    #     await server.serve()

    worker_client = BiWorkerClient()
    worker_client = await worker_client.__aenter__()

    # History accumulates in this process, not the browser: it has to survive a tab
    # being closed and a page reload, and be the same series for everyone.
    history.load()
    _sampler_task = asyncio.create_task(history.run(sample_history))

    try:
        # Start FastAPI with uvicorn
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
    except:
        try:
            await worker_client.__aexit__(*sys.exc_info())
        except: pass
    finally:
        _sampler_task.cancel()
        try:
            await _sampler_task
        except (asyncio.CancelledError, Exception):
            pass


def run(open_browser: bool = False):
    asyncio.run(start_app(open_browser=open_browser))


if __name__ == "__main__":
    asyncio.run(start_app(open_browser=True))





# import sys
# import asyncio
# import webbrowser
# import importlib.resources as resources
#
# from unsync import unsync
# from flask import Flask, request, jsonify, send_file, render_template_string
# from buelon.hub import BiWorkerClient
#
# app = Flask(__name__)
# worker_client = BiWorkerClient()
#
#
# def get_static_file(filename: str) -> str:
#     # Opens the file as text
#     with resources.files("buelon.static").joinpath(filename).open("r", encoding="utf-8") as f:
#         return f.read()
#
#
# def get_static_path(filename: str) -> str:
#     # Gets the actual filesystem path (works even if installed in venv)
#     return str(resources.files("buelon.static").joinpath(filename))
#
#
# @app.route("/")
# def index():
#     return render_template_string(get_static_file("index.html"))
#
#
# @app.route("/static/<path:path>")
# def static_file(path):
#     return send_file(get_static_path(path))
#
#
# @app.route('/data', methods=['POST'])
# def get_data():
#     @unsync
#     async def get_data_sync():
#         return await worker_client.get_web_info(True)
#
#     return jsonify(get_data_sync().result())
#
#
# def run(open_browser: bool = False):
#     _run(open_browser).result()
#
#
# @unsync
# async def _run(open_browser: bool = False):
#     global worker_client
#     port = 11011
#     host = 'localhost'
#
#     if open_browser:  # ('-y' in sys.argv and '-n' not in sys.argv) or f'{input("Open Browser? (y/n)")}'.lower().startswith('y'):
#         webbrowser.open(f'http://{host}:{port}')
#
#     async with BiWorkerClient() as client:
#         worker_client = client
#         app.run(port=port, host=host)
#
#
# if __name__ == '__main__':
#     run()
#
#
#
#
