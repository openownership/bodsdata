from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install

install_requires = [
    "click",
    "pandas",
    "fabric",
    "requests",
    "polars",
    "duckdb",
    "alembic",
    "orjson",
    "sqlalchemy",
    "psycopg2-binary",
    "codetiming",
    "openpyxl",
    "boto3",
    "fastavro",
    "google-cloud-bigquery",
    "google-api-python-client",
    "retry",
    "jsonref",
    "ipython",
    "flatterer",
]


setup(
    name="bodsdata",
    version="0.1",
    author="Open Ownership",
    author_email="code@opendataservices.coop",
    py_modules=['bodsdata'],
    url="https://github.com/openownership/bodsdata",
    license="MIT",
    description="Tools for analysing bods data",
    install_requires=install_requires,
)