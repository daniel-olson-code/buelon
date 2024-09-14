# Buelon

A scripting language to simply manage a very large amount of i/o heavy workloads. Such as API calls for your ETL, ELT or any program needing Python and/or SQL

## Table of Contents
<!--
- [Features](#features)
-->
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Supported Languages](#supported-languages) <!-- - [Configuration](#configuration) - [Usage](#usage) -->
- [Learn by Example](#learn-by-example) <!-- - [Performance](#performance)   - [Contributing](#contributing) -->
- [Future of Buelon](#plans)
- [License](#license)

<!--
## Features
- Asynchronous execution of code across multiple servers
- Custom scripting language for defining ETL pipelines
- Support for Python, SQLite3, and PostgreSQL
- Efficient handling of APIs with long wait times
- Optimized for I/O-heavy workloads
- Scalable architecture for processing large amounts of data
-->

## Installation

`pip install buelon` That's it!

This will install the cli command `bue`. Check install by running `bue --version` or `bue -v`

### Note:

This package uses Cython and you may need to install `python3-dev` using 
`sudo apt-get install python3-dev` [[more commands and information](https://stackoverflow.com/a/21530768/19907524)]. 
If you would like to use this repository without Cython, 
you may `git clone` since it is not technically dependent on 
these scripts, but they do provide a significant performance boost.  



## Quick Start
1. Get example template: `bue example` (warning: this command will over-write `.env`)
2. Start Bucket server, Hub and 3 workers: `bue demo`
3. Upload script and wait for results: `python3 example.py`

## Production Start

**Security:** Make sure bucket, hub and workers are under 
a private network **only** 
(you will need a web server or something similar
under the same private network
to access this tool using `bue upload`)

1. Run bucket server: `bue bucket -b 0.0.0.0:61535`
2. Run hub: `bue hub -b 0.0.0.0:65432 -k localhost:61535`
3. Run worker(s): `bue worker -b localhost:65432 -k localhost:61535`
4. Upload code: `bue upload  -b localhost:65432 -f ./example.bue`

## Supported Languages
- Python
- SQLite3
- PostgreSQL

<!--
## Configuration
* Setup at least 4 servers on a private network (they can be small, you can technically run all these on one server like `demo.py` does but that's not recommended)
* Create a server running `python bucket.py` or something like `python -c "import c_bucket;c_bucket.main()"` 
* Create a server running `python pipeline.py` or something like `python -c "import c_pipeline;c_pipeline.main()"` 
* Create a server running `python worker.py` or something like `python -c "import c_worker;c_worker.main()"` 
* Edit the `.env` on each server to access the private ip. Change `PIPE_WORKER_HOST` to refer to the server running `pipeline.py` on server running `worker.py` and change `BUCKET_CLIENT_HOST` to refer to the server running `bucket.py` on both the `worker.py` server and the `pipeline.py` server
* Add "worker" servers until desired speed
* Create a server with private and public network access and use this to run `pipeline.upload_pipe_code_from_file` or `pipeline.upload_pipe_code` uploading the script to the server to be run.
* All workers must also have the files necessary to run your code, pip installs and all


* (Optionally) The `PIPE_WORKER_SUBPROCESS_JOBS` value within the `.env` file can be set to `true` or `false`(really anything but true). This configuration lets you run python code in a subprocess or within the "worker" script. Setting it to false gives a very slight performance increase, but requires you restart the server every time you make a change to your project.


## Usage

Pipeline uses a custom scripting language to define ETL processes. Here's how to use it:

### Basic Structure

A Pipeline script consists of steps and pipes. Each step defines a task, and pipes determine the order of execution.

```python
# Step definition
step_name:
    language
    function_or_table_name
    source_file_or_code

# Pipe definition
pipe_name = step1 | step2 | step3

# Execution
pipe_name()
```

### Supported Languages

- python: For Python code
- sqlite3: For SQLite queries
- postgres: For PostgreSQL queries
-->
## Learn by Example

(see below for `example.py` contents)

```python
# IMPORTANT: tabs are 4 spaces. white_space == "    "


# setting scopes is how you make new jobs with errors
# not slow down your servers by setting them to a lower scope.
# And/or how you handle running heavy processes on large machine
# and small process on small machines
$ production-small
!0

# define a job called `accounts`
accounts:
    python  # <-- select the language to be run. currently only python, sqlite3 and postgres are currently available
    accounts  # select the function(for python) or table(for sql) name that will be used
    example.py  # either provide a file or write code directly using the "`" char (see below example)

request:
    python
    request_report
    example.py

status:
    python
    $ testing-small  # <-- "scope" for a single step. A lower scope will be given less priority over higher scopes. See PIPE_WORKER_SCOPES in `.env` file generated by `bue example`
    get_status
    example.py

download:
    python
    !9  # <-- "priority" higher numbers are more important and run first within their scope.
    get_report
    example.py

manipulate_data:
    sqlite3
    some_table  # *vvvv* see below for writing code directly *vvvv*
    `
SELECT
    *,
    CASE
        WHEN sales = 0
        THEN 0.0
        ELSE spend / sales
    END AS acos
FROM some_table
`

## this one's just to show postgres as well
#manipulate_data_again:
#    postgres
#    another_table
#    `
#select
#    *,
#    case
#        when spend = 0
#        then 0.0
#        else sales / spend
#    end AS roas
#from another_table
#`

py_transform:
    python
    $ testing-heavy
    transform_data
    example.py

upload:
    python
    upload_to_db
    example.py


# these are pipes and what will tell the server what order to run the steps
# and also transfer the returned  data between steps
# each step will be run individually and could be run on a different computer each time
accounts_pipe = | accounts  # single pipes currently need a `|` before or behind the value
api_pipe = request | status | download | manipulate_data | py_transform | upload


# currently there are only two syntax's for "running" pipes.
# either by itself:
# pipe()
#
# or in a loop:
# for value in pipe1():
#     pipe2(value)

# # Another Example:
# v = pipe()  # <-- single call
# pipe2(v)

# right not you cannot pass arguments within the pipe being used for the for loop.
# in this case `accounts_pipe()` cannot be `accounts_pipe(some_value)`
for account in accounts_pipe():
    api_pipe(account)
```

#### example.py
```python
import time
import random
import uuid
import logging
from typing import List, Dict, Union

from buelon.core.step import Result, StepStatus

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def accounts(*args) -> List[Dict[str, Union[int, str]]]:
    """Returns a list of sample account dictionaries.

    Returns:
        List[Dict[str, Union[int, str]]]: A list of dictionaries containing account information.
    """
    account_list = [
        {'id': 0, 'name': 'Account 1'},
        {'id': 2, 'name': 'Account 2'},
        {'id': 3, 'name': 'Account 4'},
    ]
    logger.info(f"Retrieved {len(account_list)} accounts")
    return account_list


def request_report(config: Dict[str, Union[int, str]]) -> Dict[str, Union[Dict, uuid.UUID, float]]:
    """Simulates a report request for a given account.

    Args:
        config (Dict[str, Union[int, str]]): A dictionary containing account information.

    Returns:
        Dict[str, Union[Dict, uuid.UUID, float]]: A dictionary with account data and request details.
    """
    account_id = config['id']
    
    request = {
        'report_id': uuid.uuid4(),
        'time': time.time(),
        'account_id': account_id
    }
    
    logger.info(f"Requested report for account ID: {account_id}, Report ID: {request['report_id']}")
    return {
        'account': config,
        'request': request
    }


def get_status(config: Dict[str, Union[Dict, uuid.UUID, float]]) -> Union[Dict, Result]:
    """Checks the status of a report request.

    Args:
        config (Dict[str, Union[Dict, uuid.UUID, float]]): A dictionary containing request information.

    Returns:
        Union[Dict, Result]: Either the input config if successful, or a Result object if pending.
    """
    requested_time = config['request']['time']
    account_id = config['account']['id']
    
    status = 'success' if requested_time + random.randint(10, 15) < time.time() else 'pending'
    
    if status == 'pending':
        logger.info(f"Report status for account ID {account_id} is pending")
        return Result(status=StepStatus.pending)
    
    logger.info(f"Report status for account ID {account_id} is success")
    return config
    

def get_report(config: Dict[str, Union[Dict, uuid.UUID, float]]) -> Union[Dict, Result]:
    """Retrieves a report or simulates an error.

    Args:
        config (Dict[str, Union[Dict, uuid.UUID, float]]): A dictionary containing request configuration.

    Returns:
        Union[Dict, Result]: Either a dictionary with report data or a Result object for reset.

    Raises:
        ValueError: If an unexpected error occurs.
    """
    account_id = config['account']['id']
    
    if random.randint(0, 10) == 0:
        report_data = {'status': 'error', 'msg': 'timeout error'}
    else:
        report_data = [
            {'sales': i * 10, 'spend': i % 10, 'clicks': i * 13}
            for i in range(random.randint(25, 100))
        ]
    
    if not isinstance(report_data, list):
        if isinstance(report_data, dict):
            if (report_data.get('status') == 'error' 
                and report_data.get('msg') == 'timeout error'):
                logger.warning(f"Timeout error for account ID {account_id}. Resetting.")
                return Result(status=StepStatus.reset)
        error_msg = f'Unexpected error: {report_data}'
        logger.error(f"Error getting report for account ID {account_id}: {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(f"Successfully retrieved report for account ID {account_id} with {len(report_data)} rows")
    return {
        'config': config,
        'table_data': report_data
    }


def transform_data(data: Dict[str, Union[Dict, List[Dict]]]) -> None:
    """Transforms the report data by adding account information to each row.

    Args:
        data (Dict[str, Union[Dict, List[Dict]]]): A dictionary containing config and table data.
    """
    config = data['config']
    table_data = data['table_data']
    account_name = config['account']['name']
    
    for row in table_data:
        row['account'] = account_name
    
    logger.info(f"Transformed {len(table_data)} rows of data for account: {account_name}")

    
def upload_to_db(data: Dict[str, Union[Dict, List[Dict]]]) -> None:
    """Handles table upload to database.

    Args:
        data (Dict[str, Union[Dict, List[Dict]]]): A dictionary containing table data to be uploaded.
    """    
    table_data = data['table_data']
    account_name = data['config']['account']['name']
    # Implementation for database upload
    logger.info(f"Uploaded {len(table_data)} rows to the database for account: {account_name}")
```

<!--
### Scopes and Priorities

Use scopes and priorities to control execution:

```python
$ production  # Set default scope


step_name:
    python
    !9  # Set priority (higher numbers run first within their scope)
    $ testing     # Set a lower priority scope
    function_name
    source_file
```

### Writing Code Directly

For short snippets, you can write code directly in the script:

```python
step_name:
    sqlite3
    table_name
    `
    SELECT * FROM table_name
    WHERE condition = 'value'
    `
```

### Defining Pipes

Pipes determine the order of step execution:

```python
single_pipe = | step1  # or `step1 |`
normal_pipe = step1 | step2 | step3
```

### Executing Pipes

There are two ways to execute pipes:

#### Single call

```python
pipe1()
result1 = pipe2()
result2 = pipe3(result1)
pipe4(result2)

pipe5(result1, result2)

# incorrect -> `pipe3(pipe2())`  #  this syntax is currently not supported
# also incorrect, they must be on one line as of now:
# `pipe3(
#   result1
# )`
```

#### Looped execution

```python
for item in pipe1():
    pipe2(item)
# incorrect -> `for item in pipe1(result):`  # syntax not supported for now
```

### Running Your Pipeline

- Save your pipeline script as a .pipe file.
- Use the Pipeline API to upload and run your script:
```python
# example.py
import pipeline

pipeline.upload_pipe_code_from_file('your_script.pipe')
```


## Performance
Pipeline is specifically designed to handle I/O-heavy workloads efficiently. It excels in scenarios such as:

- Making numerous API calls, especially to services with long processing times
- Handling large-scale data transfers between different systems
- Concurrent database operations

For instance, Pipeline is currently being used by an agency to request 30,000 reports daily from the Amazon Ads API, resulting in at least 90,000 API calls per day. This process, which includes pushing data into a PostgreSQL server with over 600 GB of data, is completed within a few hours(adding more workers could make this alot faster). The system's efficiency allows for this level of performance at a cost of under $100, including database expenses, actually the servers requesting the data are about $25.

The asynchronous nature of Pipeline makes it particularly suited for APIs like Amazon Ads, where there are significant wait times between requesting a report and its availability for download. Traditional synchronous ETL processes struggle with such APIs, especially for agencies with numerous profiles.

-->


## Known Defects

Currently the error handling for this scripting language is not the best.
When the script is run it is build into python, 
so it then uses its error handling, which is very good.
Because of the language's current simplicity, this is not marked as a high priority.



## Future Plans

If this projects sees some love, or I just find more free time, I'd like to support more languages. Even compiled languages such as `rust`, `go` and `c++`. Allowing teams that write different languages to work on the same program.

Better bue script errors handling.

Possibly build in `rust` once more mature for better performance.

<!---
your comment goes here
and here

## Contributing
[Contributing guidelines]
-->

## License
* MIT License