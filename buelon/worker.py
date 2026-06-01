import asyncio
from buelon.hub import run_worker, work as _work, test_worker, bi_test_worker


def run():
    # run_worker()
    asyncio.run(test_worker())


def work(single_step: str | None = None):
    # # asyncio.run(_work(single_step))
    # # asyncio.run(test_worker(single_step=single_step, iterations=100))
    # asyncio.run(test_worker(single_step=single_step, iterations=100))
    asyncio.run(bi_test_worker(single_step=single_step, iterations=2))


def run_file_on_server_until_done(path: str, sleep_time: float | int = 5.0, verbose: bool = False):
    with open(path, 'r') as f:
        code = f.read()
    run_code_on_server_until_done(code, sleep_time, verbose)


def run_code_on_server_until_done(code: str, sleep_time: float | int = 5.0, verbose: bool = False):
    import time
    from buelon.hub import upload_code_to_server, check_job_status_bulk
    from buelon.command_line import delete_last_lines

    jobs = upload_code_to_server(code, return_jobs=True)
    dones = set()
    first_run = True

    def done():
        nonlocal first_run
        verbose_text = ''
        job_statuses = check_job_status_bulk([job.id for job in jobs if job.id not in dones])
        for job in jobs:
            if job.id in dones:
                verbose_text += f'{job.id} done\n'
                yield True

            status = job_statuses.get(job.id, 'unknown')
            is_done = status in {'error', 'success', 'cancel', 'unknown'}

            if is_done:
                dones.add(job.id)
                verbose_text += f'{job.id} done\n'
            else:
                verbose_text += f'{job.id} {status}\n'

            yield is_done

        if verbose:
            verbose_text = verbose_text.strip('\n')
            if first_run:
                print(verbose_text)
            else:
                delete_last_lines(len(jobs), text=verbose_text)

        first_run = False

    while not all(list(done())):
        time.sleep(sleep_time)


async def clean():
    pass


if __name__ == '__main__':
    run()
