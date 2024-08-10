"""Demo script for the Pipeline project.

This script demonstrates how to use the Pipeline project from GitHub.
It sets up and runs a demo pipeline system with a bucket, a pipeline,
and multiple worker processes. The script can use either Python or Cython
modules, depending on availability.

"""
import os
import multiprocessing
import time

from buelon.worker import main as run_worker
from buelon.bucket import main as run_bucket
from buelon.hub import main as run_hub


def attempted_to_kill(process: multiprocessing.Process) -> None:
    """Attempts to kill a process gracefully, then forcefully if necessary.

    Args:
        process: The multiprocessing.Process to kill.

    Returns:
        None
    """
    process.terminate()
    try:
        process.join(timeout=5)  # Wait for 5 seconds
    except TimeoutError:
        process.kill()  # Force kill if process doesn't terminate in time


def main() -> None:
    """Runs the demo pipeline system.

    This function sets up and runs the bucket, pipeline, and worker processes
    to demonstrate the functionality of the Pipeline project. It manages the
    lifecycle of these processes and ensures proper cleanup on termination.

    Returns:
        None
    """
    # Open bucket first to allow pipeline and worker to store files
    bucket_process = multiprocessing.Process(target=run_bucket)
    # Then open pipeline to allow worker to request steps
    hub_process = multiprocessing.Process(target=run_hub)
    # Start as many workers as you would like
    worker1_process = multiprocessing.Process(target=run_worker)
    worker2_process = multiprocessing.Process(target=run_worker)
    worker3_process = multiprocessing.Process(target=run_worker)

    bucket_process.start()
    hub_process.start()
    time.sleep(1)
    worker1_process.start()
    worker2_process.start()
    worker3_process.start()

    try:
        bucket_process.join()
        hub_process.join()
        worker1_process.join()
        worker2_process.join()
        worker3_process.join()
    finally:
        print("Clean up")
        attempted_to_kill(worker1_process)
        attempted_to_kill(worker2_process)
        attempted_to_kill(worker3_process)
        attempted_to_kill(hub_process)
        attempted_to_kill(bucket_process)


if __name__ == "__main__":
    main()

