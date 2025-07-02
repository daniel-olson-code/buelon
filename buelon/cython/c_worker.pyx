import asyncio
from buelon.hub import run_worker, work as _work


def run():
    run_worker()


def work(single_step: str | None = None):
    asyncio.run(_work(single_step))


if __name__ == '__main__':
    run()
