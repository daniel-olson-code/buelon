# Every job in this file gets these unless it overrides them.
# `scope` is how you route work: a worker only pulls jobs whose scope is in its
# own `scopes` list (see `worker.scopes` in .boo/settings.yaml), so a scope with
# failing jobs cannot slow down the rest of your queue.
!scope default
!timeout 20 * 60  # seconds; an arithmetic expression is fine

# step 1: `accounts`
accounts:
    python  # <-- the language. python, sqlite3 and postgres are available
    accounts  # <-- the function name (for python) or table name (for sql)
    example.py  # <-- a file path, or write code inline with the "`" char (see below)

request:
    python
    request_report
    example.py

status:
    python
    !scope default  # <-- override the scope for this one job
    get_status
    example.py

download:
    python
    !priority 9  # <-- higher numbers run first within their scope
    get_report
    example.py

manipulate_data:
    sqlite3
    some_table  # <-- the incoming rows are loaded under this name
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

upload:
    python
    upload_to_db
    example.py


# these are pipes and what will tell the server what order to run the jobs
# and also transfer the returned data between them
# each job will be run individually and could be run on a different computer each time
accounts_pipe = | accounts  # single pipes currently need a `|` before or behind the value
api_pipe = request | status | download | manipulate_data | upload


# currently there are only two syntax's for "running" pipes.
# either by itself:
# pipe()
#
# or in a loop:
# for value in pipe1():
#     pipe2(value)

# # Another Example:
# v = pipe(accounts_pipe)  # <-- single call
# pipe2(v)

# right now you cannot pass arguments within the pipe being used for the for loop.
# in this case `accounts_pipe()` cannot be `accounts_pipe(some_value)`
for account in accounts_pipe():
    api_pipe(account)
