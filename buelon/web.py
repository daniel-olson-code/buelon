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

app = FastAPI()
worker_client: BiWorkerClient | None = None
settings.worker.info['name'] = f"Web App ({settings.worker.info['name']})"


def get_static_file(filename: str) -> str:
    # Opens the file as text
    with resources.files("buelon.static").joinpath(filename).open("r", encoding="utf-8") as f:
        return f.read()


def get_static_path(filename: str) -> str:
    # Gets the actual filesystem path (works even if installed in venv)
    return str(resources.files("buelon.static").joinpath(filename))


@app.get("/", response_class=HTMLResponse)
async def index():
    return get_static_file("index.html")


@app.get("/static/{path:path}")
async def static_file(path: str):
    return FileResponse(get_static_path(path))


@app.post("/data")
async def get_data():
    global worker_client
    assert worker_client is not None
    try:
        data = await worker_client.get_web_info(True)
    except:
        worker_client = await worker_client.__aenter__()
        data = await worker_client.get_web_info(True)
    return JSONResponse(content=data)


@app.post("/errors")
async def get_errors():
    global worker_client
    assert worker_client is not None
    try:
        compressed_jobs, error_info = await worker_client.errors()
    except:
        worker_client = await worker_client.__aenter__()
        compressed_jobs, error_info = await worker_client.errors()
    jobs = compressed_message_to_steps(compressed_jobs)
    json_jobs = [job.to_json() for job in jobs]
    data = {
        "jobs": json_jobs,
        "errors": error_info
    }
    return JSONResponse(content=data)


@app.post('/reset-errors')
async def reset_errors():
    global worker_client
    assert worker_client is not None
    try:
        await worker_client.reset_errors()
    except:
        worker_client = await worker_client.__aenter__()
        await worker_client.reset_errors()
    return JSONResponse(content={'status': 'success'})


class Job(BaseModel):
    id: str


@app.post("/job-parents-and-results")
async def api_job_parents_and_results(job: Job):
    global worker_client
    assert worker_client is not None
    try:
        data = await worker_client.get_job_parents_and_results(job.id)
    except:
        worker_client = await worker_client.__aenter__()
        data = await worker_client.get_job_parents_and_results(job.id)
    return JSONResponse(content=data)


async def stream_subprocess_logs(cmd_parts):
    """
    Asynchronously runs a command and streams its stdout and stderr.
    This version uses an asyncio.Queue to correctly merge the two streams.
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

    finished_streams = 0
    while finished_streams < 2:
        line = await queue.get()
        if line is None:
            # A stream has finished.
            finished_streams += 1
        else:
            yield line.decode('utf-8')

    # Wait for the process and reader tasks to complete.
    await process.wait()
    await asyncio.gather(stdout_reader, stderr_reader)


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
    new_name = f"Web App Subprocess ({settings.worker.info['name']})"
    rename_worker_txt = f"buelon.settings.settings.worker.info['name'] = {repr(new_name)}"
    execution_txt = f'buelon.worker.work({repr(job.id)})'

    # Build the Python code first, then properly quote it for the shell
    python_code = f"{import_txt};{rename_worker_txt};{execution_txt}"
    command = f'{sys.executable} -u -c {shlex.quote(python_code)}'

    cmd_parts = shlex.split(command)

    return StreamingResponse(stream_subprocess_logs(cmd_parts), media_type="text/plain")


async def start_app(open_browser: bool = False):
    global worker_client
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

    try:
        # Start FastAPI with uvicorn
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
    except:
        try:
            await worker_client.__aexit__(*sys.exc_info())
        except: pass


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
