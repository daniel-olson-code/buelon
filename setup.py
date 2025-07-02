from setuptools import setup, find_packages, Extension
# from Cython.Build import cythonize
#
# # List of Cython files to compile
# extensions = [
#     Extension("buelon.cython.c_bucket", ["buelon/cython/c_bucket.pyx"]),
#     Extension("buelon.cython.c_worker", ["buelon/cython/c_worker.pyx"]),
#     Extension("buelon.cython.c_hub", ["buelon/cython/c_hub.pyx"])
# ]

# Requirements  for the package
requirements = [
    'Cython',
    'psycopg2-binary',
    'orjson',
    'python-dotenv',
    'asyncio-pool',
    'psutil',
    'unsync',
    'redis',
    # 'persist-queue',
    'persistqueue',
    'PyYAML',
    'kazoo',
    'tqdm',
    'asyncpg'
]

# Read the long description from the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="buelon",
    version="1.0.68-alpha8",
    author="Daniel Olson",
    author_email="daniel@orphos.cloud",
    description="A scripting language to simply manage a very large amount of i/o heavy workloads. Such as API calls "
                "for your ETL, ELT or any program needing Python and/or SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/daniel-olson-code/buelon",
    packages=find_packages(),
    package_data={
        'buelon/examples': [
            "example.pipe",
        ],
        'buelon/cython': [
            "*.pyx",
        ]
    },
    include_package_data=True,
    package_name="buelon",
    # ext_modules=cythonize(extensions),
    install_requires=requirements,
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
