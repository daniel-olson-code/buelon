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

1. Run bucket server: `bue bucket -b 0.0.0.0:61535`
2. Run hub: `bue hub -b 0.0.0.0:65432 -k localhost:61535`
3. Run n worker(s): `bue worker -b localhost:65432 -k localhost:61535`
4. Upload code: `bue upload  -b localhost:65432 -f path/to/file.bue`

## Production Start

**Security:** Make sure bucket, hub and workers are under 
a private network **only** 
(you will need a web server or something similar
under the same private network
to access this tool using `bue upload -f path/to/file.bue`)

### With Postgres (Under 1,000,000 Jobs at once)

1. Create a `.env` file
```properties
PIPE_WORKER_SCOPES=production-very-heavy,production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small,default
PIPE_WORKER_SUBPROCESS_JOBS=false
N_WORKER_PROCESSES="25"

USING_POSTGRES_HUB=true
USING_POSTGRES_BUCKET="true"
POSTGRES_HOST="123.45.67.89"
POSTGRES_PORT="5432"
POSTGRES_USER="daniel"
POSTGRES_PASSWORD="Password123"
POSTGRES_DATABASE="my_db"
```

2. Run n worker(s): `bue worker -b localhost:65432 -k localhost:61535`
3. Upload code: `bue upload  -b localhost:65432 -f ./example.bue`

### Without Postgres (Under 10,000 jobs at once)

1. Create a `.env` file
```properties
PIPE_WORKER_SCOPES=production-very-heavy,production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small,default
PIPE_WORKER_SUBPROCESS_JOBS=false
N_WORKER_PROCESSES="15"
PIPE_WORKER_HOST="123.45.67.89"
PIPE_WORKER_PORT="65432"

PIPELINE_HOST="0.0.0.0"
PIPELINE_PORT="65432"

BUCKET_SERVER_HOST="0.0.0.0"
BUCKET_SERVER_PORT="61535"
BUCKET_CLIENT_HOST="123.45.67.89"
BUCKET_CLIENT_PORT="61535"
```
1. Run bucket server: `bue bucket`
2. Run hub: `bue hub`
3. Run n worker(s): `bue worker`
4. Upload code: `bue upload -f ./example.bue`

## Supported Languages
- Python
- SQLite3
- PostgreSQL

## Learn by Example

(see below for `example.py` contents)

```python
# IMPORTANT: tabs are 4 spaces. white_space == "    "
# [Optional] change tab sizes like this
TAB = '    '

# set config values globally
!scope production-small  # job scope [see bellow]
!priority 0  # higher priority jobs are run first
!timeout 20 * 60  # job's max time to run in seconds
!retries 0  # how many times a job can run after error

# setting scopes is how you make new jobs with errors
# not interfere with all servers job queues
# and/or how you handle running heavy processes on large machine
# and small process on small machines

# define a single job called `accounts`
accounts:
    python  # <-- select the language to be run. currently only python, sqlite3 and postgres are available
    accounts  # select the function(for python) or table(for sql) name that will be used
    example.py  # either provide a file or write code directly using the "`" char (see below example)

# or

# define multiple jobs with:
import python (
    request_report 
        as request,
    get_status 
        as status 
        !scope testing-small,
    get_report 
        as download 
        !priority 9
        !timeout 60**2 * 5 / (1 % 2) // (1 + 1 - 1),  # 5 hrs
    transform_data 
        as py_transform 
        !scope production-heavy,
    upload_to_db as upload
) example.py  # <-- file path or using "`" like sql below


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

# these are pipes and what will tell the server what order to run the steps
# and also transfer the returned  data between steps
# each step will be run individually and could be run on a different computer each time
accounts_pipe = | accounts  # single pipes currently need a `|` before or behind the value
# api_pipe = request | status | download | manipulate_data | py_transform | upload
# # or
api_pipe = (
    request | status | download 
    | manipulate_data | py_transform | upload
)


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

## Known Defects

Error handling and logging are currently lacking


## Future Plans

If this projects sees some love, 
or I just find more free time, 
I'd like to support more languages like `node` or `deno` and
even compiled languages such as 
`rust`, `go` and `c++`. 
Allowing teams that write different 
languages to work on the same program.

Web app for logging, execution and worker management

Add a scheduler process to allow scheduled pipelines

<!---
your comment goes here
and here

## Contributing
[Contributing guidelines]
-->

## License
* MIT License