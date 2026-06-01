import argparse
import os
import json
import sys
import time
import uuid
import asyncio
import logging
import datetime

from buelon.settings import settings, init, SETTINGS_PATH

# Import necessary modules from buelon
import buelon as pete
from buelon.jokes.jokes import tell_a_boo_joke


def delete_last_line():
    sys.stdout.write("\033[F")
    sys.stdout.write("\033[K")


def delete_last_lines(n: int = 1, text: str | None = None):
    if n > 0:
        sys.stdout.write(f"\033[{n}F")  # Move up n lines
        sys.stdout.write("\033[J")      # Clear from cursor to end of screen
        if text:
            sys.stdout.write(text + '\n')
        sys.stdout.flush()

    # sys.stdout.write("\033[?25l")  # Hide cursor
    # for _ in range(n):
    #     sys.stdout.write("\033[F")  # Move up
    #     sys.stdout.write("\033[K")  # Clear line
    # sys.stdout.write("\033[?25h")  # Show cursor again
    # sys.stdout.flush()


def run_hub(args):
    # pete.hub.run_server()
    pete.hub.bi_test_server()


def run_worker(args):
    pete.worker.run_worker()


def run_test(args):
    pass


def run_bucket(args):
    # Run the bucket
    pete.bucket.main()


def run_demo():
    # Run the demo
    pete.examples.demo.main()


def run_example():
    # Run the example
    pete.examples.example.setup()


def upload_pipe_code(file_path):  # , binding, lazy_steps):
    pete.hub.upload_file_to_server(file_path)  # , lazy_steps)


def submit_pipe_code(file_path, binding, bucket_binding, scope):
    pass
    # pete.hub.submit_pipe_code_from_file(file_path, scope)


def display_status(args):
    async def func():
        async with pete.hub.BiWorkerClient(settings.worker.host, settings.worker.port, settings.worker.scopes.split(',')) as client:
            async def display(prefix: str = '', suffix: str = ''):
                r = await client.display()
                return prefix + r + suffix

            refuse_count = 0

            log_every_once_and_a_while = args.log
            last_log = time.time()
            time_each_log = 15 * 60

            async def display_status_worker(should_delete=False):
                nonlocal refuse_count
                lines = 1
                try:
                    s = await display(datetime.datetime.now().strftime('%b %d, %Y | %H:%M %Ss') + '\n')
                    if not should_delete:
                        print(s)
                    else:
                        # sys.stdout.write("\033[F")
                        # sys.stdout.write("\033[K")
                        sys.stdout.write(f"\033[{2}F")  # Move up n lines
                        sys.stdout.write("\033[J")
                        sys.stdout.write(s + '\n')
                        sys.stdout.flush()
                        # print(datetime.datetime.now().strftime('%b %d, %Y %H:%M'))
                        # prefix = '' # datetime.datetime.now().strftime('%b %d, %Y %H:%M') + '\n'
                        # pete.hub.display_from_server(prefix=prefix)
                    lines += 1
                except ConnectionRefusedError:
                    print('Connection refused' + '.' * (refuse_count % 3))
                    refuse_count += 1
                except OSError as e:  # socket error
                    print(f'{e}')

                return lines
            if not args.subscribe:
                await display_status_worker()
            else:
                last_length = 0

                if log_every_once_and_a_while:
                    await display_status_worker()
                    print(f'>')
                    await display_status_worker()

                while True:
                    try:
                        if log_every_once_and_a_while and (time.time() - last_log) > time_each_log:
                            last_log = time.time()
                            print(f'>')
                            last_length = await display_status_worker()
                        else:
                            # delete_last_lines(last_length)
                            last_length = await display_status_worker(True)
                        # last_length = await display_status_worker()
                        time.sleep(3)
                    except KeyboardInterrupt:
                        print('\n - Have a great day! :)\n')
                        break

    return asyncio.run(func())


def display_status_v1(args):
    refuse_count = 0

    log_every_once_and_a_while = args.log
    last_log = time.time()
    time_each_log = 15 * 60

    def display_status_worker(should_delete=False):
        nonlocal refuse_count
        lines = 1
        try:
            s = pete.hub.display_from_server(datetime.datetime.now().strftime('%b %d, %Y | %H:%M %Ss') + '\n', return_value=True)
            if not should_delete:
                print(s)
            else:
                # sys.stdout.write("\033[F")
                # sys.stdout.write("\033[K")
                sys.stdout.write(f"\033[{2}F")  # Move up n lines
                sys.stdout.write("\033[J")  
                sys.stdout.write(s + '\n')
                sys.stdout.flush()
                # print(datetime.datetime.now().strftime('%b %d, %Y %H:%M'))
                # prefix = '' # datetime.datetime.now().strftime('%b %d, %Y %H:%M') + '\n'
                # pete.hub.display_from_server(prefix=prefix)
            lines += 1
        except ConnectionRefusedError:
            print('Connection refused' + '.' * (refuse_count % 3))
            refuse_count += 1
        except OSError as e:  # socket error
            print(f'{e}')

        return lines
    if not args.subscribe:
        display_status_worker()
    else:
        last_length = 0

        if log_every_once_and_a_while:
            display_status_worker()
            print(f'>')
            display_status_worker()

        while True:
            try:
                if log_every_once_and_a_while and (time.time() - last_log) > time_each_log:
                    last_log = time.time()
                    print(f'>')
                    last_length = display_status_worker()
                else:
                    # delete_last_lines(last_length)
                    last_length = display_status_worker(True)
                # last_length = display_status_worker()
                time.sleep(3)
            except KeyboardInterrupt:
                print('\n - Have a great day! :)\n')
                break


def fetch_errors(args):
    pete.hub.display_errors_from_server()
    # try:
    #     count = int(args.count)
    # except ValueError:
    #     count = 25
    #
    # exclude = args.exclude
    # if not isinstance(exclude, str):
    #     exclude = None
    # elif ',' in exclude:
    #     exclude = exclude.split(',')
    #
    # print('exclude', exclude)
    #
    # client = pete.hub.HubClient()
    # return_value = client.fetch_errors(count, exclude)
    # count, total, table = return_value['count'], return_value['total'], return_value['table']
    # print(f'Fetched {count} errors of {total} total errors.\n')
    # for row in table:
    #     _id, step, msg, _trace = row['id'], row['step'], row['msg'], row['trace']
    #     print(f'ID: \n  {_id}')
    #     print(f'Step: \n  {json.dumps(step, indent=4)}')
    #     print(f'Message: \n  {msg}')
    #     print(f'Trace: \n  {_trace}\n\n--**--\n')


def has_postgres_env_vars() -> bool:
    return all([var in os.environ for var in [
        'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DATABASE']])


def cli():
    parser = argparse.ArgumentParser(description='Buelon command-line interface')
    parser.add_argument('-v', '--version', action='version', version='Buelon 1.0.77-alpha4')
    parser.add_argument('-b', '--binding', help='Binding for uploading pipe code (host:port)')
    parser.add_argument('file_path', nargs='?', default='nothing', help='File path for uploading pipe code')

    subparsers = parser.add_subparsers(title='Commands', dest='command', required=False)

    # Check if environment variables are already set
    hub_binding_required = not ('PIPELINE_HOST' in os.environ and 'PIPELINE_PORT' in os.environ)
    hub_bucket_required = not ('BUCKET_CLIENT_HOST' in os.environ and 'BUCKET_CLIENT_PORT' in os.environ)
    if os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true':
        hub_bucket_required = not has_postgres_env_vars()

    worker_binding_required = not ('PIPE_WORKER_HOST' in os.environ and 'PIPE_WORKER_PORT' in os.environ)
    worker_bucket_required = not ('BUCKET_CLIENT_HOST' in os.environ and 'BUCKET_CLIENT_PORT' in os.environ)
    if os.environ.get('USING_POSTGRES_HUB', 'false') == 'true':
        worker_binding_required = not has_postgres_env_vars()
    if os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true':
        worker_bucket_required = not has_postgres_env_vars()

    bucket_binding_required = not ('BUCKET_SERVER_HOST' in os.environ and 'BUCKET_SERVER_PORT' in os.environ)

    init_parser = subparsers.add_parser('init', help='Create the settings.yaml')

    where_parser = subparsers.add_parser('where', help='Display the settings.yaml path')

    # Pipe file run
    file_parser = subparsers.add_parser('upload', help='Upload pipe code from a file')
    # file_parser.add_argument('-b', '--hub-binding', required=worker_binding_required, help='Binding to hub (host:port)')
    file_parser.add_argument('-f', '--file-path', help='File path for uploading pipe code', dest='file_path')
    # file_parser.add_argument('-l', '--lazy', default=False, action=argparse.BooleanOptionalAction, dest='lazy')

    # Hub command
    hub_parser = subparsers.add_parser('hub', help='Run the hub')
    # hub_parser.add_argument('-b', '--binding', required=hub_binding_required, help='Main binding for hub (host:port)')
    # hub_parser.add_argument('-k', '--bucket-binding', required=hub_bucket_required, help='Bucket binding (host:port)')
    # hub_parser.add_argument('-p', '--postgres', help='Postgres connection (host:port:user:password:database)')

    # Worker command
    worker_parser = subparsers.add_parser('worker', help='Run the worker')
    # worker_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for worker (host:port)')
    # worker_parser.add_argument('-k', '--bucket-binding', required=worker_bucket_required, help='Bucket binding (host:port)')
    # worker_parser.add_argument('-j', '--subprocess-jobs', choices=['true', 'false'], help='Set PIPE_WORKER_SUBPROCESS_JOBS environment variable')
    # worker_parser.add_argument('-s', '--scopes', help='Set PIPE_WORKER_SCOPES environment variable')
    # worker_parser.add_argument('-p', '--postgres', help='Postgres connection (host:port:user:password:database)')
    # worker_parser.add_argument('-a', '--all', default=False, action=argparse.BooleanOptionalAction, help='Include all status')

    work_parser = subparsers.add_parser('work', help='Run several jobs')

    # Worker command
    test_parser = subparsers.add_parser('test', help='Run the Test worker')
    # test_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for worker (host:port)')
    # test_parser.add_argument('-k', '--bucket-binding', required=worker_bucket_required, help='Bucket binding (host:port)')
    # test_parser.add_argument('-j', '--subprocess-jobs', choices=['true', 'false'], help='Set PIPE_WORKER_SUBPROCESS_JOBS environment variable')
    # test_parser.add_argument('-s', '--scopes', help='Set PIPE_WORKER_SCOPES environment variable')
    # test_parser.add_argument('-p', '--postgres', help='Postgres connection (host:port:user:password:database)')
    # test_parser.add_argument('-a', '--all', default=False, action=argparse.BooleanOptionalAction, help='Include all status')

    # Bucket command
    bucket_parser = subparsers.add_parser('bucket', help='Run the bucket')
    # bucket_parser.add_argument('-b', '--binding', required=bucket_binding_required, help='Binding for bucket (host:port)')

    # Status
    status_parser = subparsers.add_parser('status', help='Check the status of the pipeline')
    # status_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for hub (host:port)')
    status_parser.add_argument('-s', '--subscribe', default=False, action=argparse.BooleanOptionalAction, help='Subscribe to status updates')
    status_parser.add_argument('-l', '--log', default=False, action=argparse.BooleanOptionalAction, help='Log subscribed status every 15 minutes(Requires Subscribe option)')

    # Reset Errors
    reset_parser = subparsers.add_parser('reset', help='Reset errors')
    # reset_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for hub (host:port)')
    # reset_parser.add_argument('-w', '--include_working', default='false', choices=['true', 'false'], help='Whether to reset status \'working\'')

    # Delete steps
    delete_parser = subparsers.add_parser('delete', help='Delete steps')
    # delete_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for hub (host:port)')
    # delete_parser.add_argument('-s', '--step_id', help='Step ID to delete')

    # Fetch Errors
    error_fetch_parser = subparsers.add_parser('errors', help='View Error Logs')
    # error_fetch_parser.add_argument('-b', '--binding', required=worker_binding_required,  help='Main binding for hub (host:port)')
    error_fetch_parser.add_argument('-c', '--count', default='10', help='Amount of error logs to view (must be int)')
    error_fetch_parser.add_argument('-e', '--exclude', default=None, help='Exclude a specific strings from error message. Commas exclude multiple messages')

    # Fetch Errors
    run_step_parser = subparsers.add_parser('run-job', help='View Error Logs')
    run_step_parser.add_argument('-j', '--job', required=True, help='The step id')

    # Fetch Errors
    submit_parser = subparsers.add_parser('submit', help='View Error Logs')
    submit_parser.add_argument('-f', '--file', required=True, help='File path to buelon script')
    # submit_parser.add_argument('-k', '--bucket-binding', required=worker_bucket_required, help='Bucket binding (host:port)')
    # submit_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for hub (host:port)')
    # submit_parser.add_argument('-s', '--scope', default=pete.worker.DEFAULT_SCOPES.split(',')[-1])

    # Repair
    repair_parser = subparsers.add_parser('repair', help='Reset errors')
    # repair_parser.add_argument('-b', '--binding', required=worker_binding_required, help='Main binding for hub (host:port)')

    # Repair
    run_parser = subparsers.add_parser('run', help='Run script locally (Alpha Command)')
    run_parser.add_argument('-f', '--file', required=True, help='File path to buelon script')

    # Demo command
    demo_parser = subparsers.add_parser('demo', help='Run the demo')
    # demo_parser.set_defaults(func=run_demo)

    # Example command
    example_parser = subparsers.add_parser('example', help='Run the example')
    # example_parser.set_defaults(func=run_example)

    # Web command
    web_parser = subparsers.add_parser('web', help='Run utility web server')
    web_parser.add_argument('-o', '--open-browser', default=False, action=argparse.BooleanOptionalAction, help='Open Browser')

    joke_parser = subparsers.add_parser('joke', help='Tell me a boo joke')

    # Parse arguments
    args, remaining_args = parser.parse_known_args()
    # Handle the commands
    if args.command == 'init':
        init()
        print('Settings.yaml created at: ')
        print(SETTINGS_PATH)
    elif args.command == 'where':
        print('Settings.yaml path: ')
        print(SETTINGS_PATH)
    elif args.command == 'hub':
        run_hub(args)
    elif args.command == 'worker':
        run_worker(args)
    elif args.command == 'work':
        pete.worker.work()
    elif args.command == 'test':
        run_test(args)
    elif args.command == 'bucket':
        run_bucket(args)
    elif args.command == 'demo':
        run_demo()
    elif args.command == 'web':
        from buelon.web import run
        run(args.open_browser)
    elif args.command == 'example':
        run_example()
    elif args.command == 'upload':
        upload_pipe_code(args.file_path)  # , args.hub_binding, args.lazy)
    elif args.command == 'submit':
        submit_pipe_code(args.file, args.binding, args.bucket_binding, args.scope)
    elif args.command == 'reset':
        pete.hub.reset_errors_from_server()
    elif args.command == 'status':
        display_status(args)
    elif args.command == 'delete':
        pete.hub.cancel_errors_from_server()
    elif args.command == 'errors':
        fetch_errors(args)
    elif args.command == 'repair':
        print('This has been deprecated and does nothing')
    elif args.command == 'run-job':
        # print('This currently does nothing')
        pete.worker.work(args.job)
    elif args.command == 'run':
        if args.file:
            with open(args.file) as f:
                pete.core.pipe_interpreter.run_code(f.read())
    elif args.command == 'joke':
        tell_a_boo_joke()
    else:
        # Handle the case where a file path is given without a command
        if args.binding and remaining_args:
            file_path = remaining_args[0]
            file_path.close()
            # upload_pipe_code(file_path, args.binding)
        elif remaining_args:
            parser.error('Binding is required when uploading pipe code from file. Use -b or --binding.')
        else:
            parser.print_help()
            sys.exit(1)


if __name__ == '__main__':
    cli()



# import argparse
# import os
# import sys
#
# # Import necessary modules from buelon
# import buelon as pete
#
# def run_hub(args):
#     # Set environment variables for hub
#     os.environ['PIPELINE_HOST'], os.environ['PIPELINE_PORT'] = args.main_binding.split(':')
#     os.environ['BUCKET_CLIENT_HOST'], os.environ['BUCKET_CLIENT_PORT'] = args.bucket_binding.split(':')
#
#     # Set PostgreSQL environment variables if provided
#     if args.postgres:
#         os.environ['POSTGRES_HOST'], os.environ['POSTGRES_PORT'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_DATABASE'] = args.postgres.split(':')
#
#     # Run the hub
#     pete.hub.main()
#
# def run_worker(args):
#     # Set environment variables for worker
#     os.environ['PIPE_WORKER_HOST'], os.environ['PIPE_WORKER_PORT'] = args.main_binding.split(':')
#     os.environ['BUCKET_CLIENT_HOST'], os.environ['BUCKET_CLIENT_PORT'] = args.bucket_binding.split(':')
#
#     # Set PIPE_WORKER_SUBPROCESS_JOBS environment variable if provided
#     if args.subprocess_jobs:
#         os.environ['PIPE_WORKER_SUBPROCESS_JOBS'] = args.subprocess_jobs
#
#     # Set PIPE_WORKER_SCOPES environment variable if provided
#     if args.scopes:
#         os.environ['PIPE_WORKER_SCOPES'] = args.scopes
#
#     # Set PostgreSQL environment variables if provided
#     if args.postgres:
#         os.environ['POSTGRES_HOST'], os.environ['POSTGRES_PORT'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_DATABASE'] = args.postgres.split(':')
#
#     # Run the worker
#     pete.worker.main()
#
# def run_bucket(args):
#     # Set environment variables for bucket
#     os.environ['BUCKET_SERVER_HOST'], os.environ['BUCKET_SERVER_PORT'] = args.binding.split(':')
#
#     # Run the bucket
#     pete.bucket.main()
#
# def run_demo():
#     # Run the demo
#     pete.examples.demo.main()
#
# def run_example():
#     # Run the example
#     pete.examples.example.main()
#
# def upload_pipe_code(file_path, binding):
#     os.environ['PIPE_WORKER_HOST'], os.environ['PIPE_WORKER_PORT'] = binding.split(':')
#     pete.hub.upload_pipe_code_from_file(file_path)
#
# def cli():
#     parser = argparse.ArgumentParser(description='Buelon command-line interface')
#     parser.add_argument('-v', '--version', action='version', version='Buelon 1.0.0')
#     subparsers = parser.add_subparsers(title='Commands', dest='command')
#
#     # Check if environment variables are already set
#     hub_binding_required = not ('PIPELINE_HOST' in os.environ and 'PIPELINE_PORT' in os.environ)
#     hub_bucket_required = not ('BUCKET_CLIENT_HOST' in os.environ and 'BUCKET_CLIENT_PORT' in os.environ)
#
#     worker_binding_required = not ('PIPE_WORKER_HOST' in os.environ and 'PIPE_WORKER_PORT' in os.environ)
#     worker_bucket_required = not ('BUCKET_CLIENT_HOST' in os.environ and 'BUCKET_CLIENT_PORT' in os.environ)
#
#     bucket_binding_required = not ('BUCKET_SERVER_HOST' in os.environ and 'BUCKET_SERVER_PORT' in os.environ)
#
#     # Hub command
#     hub_parser = subparsers.add_parser('hub', help='Run the hub')
#     hub_parser.add_argument('-b', '--main-binding', required=hub_binding_required, help='Main binding for hub (host:port)')
#     hub_parser.add_argument('-k', '--bucket-binding', required=hub_bucket_required, help='Bucket binding (host:port)')
#     hub_parser.add_argument('-p', '--postgres', help='Postgres connection (host:port:user:password:database)')
#
#     # Worker command
#     worker_parser = subparsers.add_parser('worker', help='Run the worker')
#     worker_parser.add_argument('-b', '--main-binding', required=worker_binding_required, help='Main binding for worker (host:port)')
#     worker_parser.add_argument('-k', '--bucket-binding', required=worker_bucket_required, help='Bucket binding (host:port)')
#     worker_parser.add_argument('-s', '--subprocess-jobs', choices=['true', 'false'], help='Set PIPE_WORKER_SUBPROCESS_JOBS environment variable')
#     worker_parser.add_argument('--scopes', help='Set PIPE_WORKER_SCOPES environment variable')
#     worker_parser.add_argument('-p', '--postgres', help='Postgres connection (host:port:user:password:database)')
#
#     # Bucket command
#     bucket_parser = subparsers.add_parser('bucket', help='Run the bucket')
#     bucket_parser.add_argument('-b', '--binding', required=bucket_binding_required, help='Binding for bucket (host:port)')
#
#     # Demo command
#     demo_parser = subparsers.add_parser('demo', help='Run the demo')
#     demo_parser.set_defaults(func=run_demo)
#
#     # Example command
#     example_parser = subparsers.add_parser('example', help='Run the example')
#     example_parser.set_defaults(func=run_example)
#
#     # Parse arguments
#     args, remaining_args = parser.parse_known_args()
#
#     # Handle the commands
#     if args.command == 'hub':
#         run_hub(args)
#     elif args.command == 'worker':
#         run_worker(args)
#     elif args.command == 'bucket':
#         run_bucket(args)
#     elif args.command == 'demo':
#         run_demo()
#     elif args.command == 'example':
#         run_example()
#     else:
#         # Handle the case where a file path is given without a command
#         if remaining_args:
#             file_path = remaining_args[-1]
#             if '--binding' in remaining_args or '-b' in remaining_args:
#                 index = remaining_args.index('--binding') + 1 if '--binding' in remaining_args else remaining_args.index('-b') + 1
#                 binding = remaining_args[index]
#                 upload_pipe_code(file_path, binding)
#             else:
#                 parser.error('Binding is required when uploading pipe code from file.')
#         else:
#             parser.print_help()
#             sys.exit(1)
#
# if __name__ == '__main__':
#     cli()
#
