import json

from setuptools import setup, find_packages

# The version literal lives in `version.json` and nowhere else (BUGS.md #26);
# `buelon/_version.py` reads the same file at runtime.
with open('version.json', 'r', encoding='utf-8') as fh:
    version = json.load(fh)['last']

# Requirements for the package.
#
# The base install is exactly what `bue hub`, `bue worker` and `buelon.core` import.
# Everything heavier is an extra (see `extras` below), so a worker box does not have
# to build or download a Postgres driver it will never open. BUGS.md #58.
#
# Deleted here rather than moved, because nothing under `buelon/` imports them:
#   asyncio-pool  -- never imported at all
#   psutil        -- only ever appears in commented-out lines
#   persistqueue  -- `buelon.helpers.persistqueue` is a LOCAL module, not this package
#   redis, kazoo  -- the bucket's redis/ZooKeeper backends were removed outright
#   tqdm          -- its one use was the ZooKeeper bulk_set progress bar
requirements = [
    'orjson',
    'python-dotenv',
    'unsync',
    'PyYAML',
    'bisocket>=0.0.9',
]

# Optional dependency groups.
#
# `bucket` is a deliberate alias for `postgres`: since the redis and ZooKeeper
# backends were removed, Postgres is the bucket's only backend beyond plain files,
# so the two groups install the same thing. It is kept as a separate name because
# `pip install buelon[bucket]` is what someone running `bue bucket` will reach for.
postgres_requirements = [
    'psycopg2-binary',
    'asyncpg',
]

# pydantic is a hard import in web.py (`from pydantic import BaseModel`) but was
# never declared -- it only ever arrived transitively via fastapi. Declared now.
web_requirements = [
    'fastapi',
    'uvicorn',
    'pydantic',
]

extras = {
    'postgres': postgres_requirements,
    'bucket': postgres_requirements,
    'web': web_requirements,
    'all': postgres_requirements + web_requirements,
}

# Read the long description from the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="buelon",
    version=version,
    author="Daniel Olson",
    author_email="daniel@orphos.cloud",
    description="A scripting language to simply manage a very large amount of i/o heavy workloads. Such as API calls "
                "for your ETL, ELT or any program needing Python and/or SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/daniel-olson-code/buelon",
    packages=find_packages(),
    package_data={
        # Dotted package names, not paths -- setuptools silently ignores a key
        # that is not an installed package. `bue example` copies example.bue out
        # of the installed package directory, so a wheel that omits it breaks the
        # command outright (BUGS.md #41).
        'buelon.examples': [
            "example.bue",
        ],
        'buelon.static': [
            "*",
        ],
    },
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras,
    entry_points={
        'console_scripts': [
            'boo=buelon.command_line:cli',
            'bue=buelon.command_line:cli',
            'pete=buelon.command_line:cli'
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.10",
    keywords="buelon etl pipeline asynchronous data-processing api",
)
