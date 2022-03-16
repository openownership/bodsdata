import base64
from fabric import Connection
from pathlib import Path
from distutils.command.upload import upload
import pandas
import polars
import duckdb
import csv
import datetime
import functools
import glob
import gzip
import json
import requests 
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
import zipfile
from collections import Counter, deque, defaultdict
from retry import retry
from textwrap import dedent
import ijson
import flatterer

import boto3
import click
import openpyxl
import orjson
import requests
import sqlalchemy as sa
from codetiming import Timer
from fastavro import parse_schema, writer
from google.cloud import bigquery
from google.cloud.bigquery.dataset import AccessEntry
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from jsonref import JsonRef
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


this_path = Path(__file__).parent.absolute()

output_dir = tempfile.gettempdir()
upload_bucket = "oo-bodsdata"

render_host = None
s3_data_location = None


def _first_doc_line(function):
    return function.__doc__.split("\n")[0]


@functools.lru_cache(None)
def get_engine(schema=None, db_uri=None, pool_size=1):
    """Get SQLAlchemy engine

    Will cache engine if all arguments are the same so not expensive to call multiple times.


    Parameters
    ----------
    schema : string, optional
        Postgres schema that all queries will use. Defaults to using public schema.
    db_url : string, optional
        SQLAlchemy database connection string. Will defailt to using `DATABASE_URL` environment variable.
    pool_size : int
       SQLAlchemy connection pool size


    Returns
    -------
    sqlalchemy.Engine
        SQLAlchemy Engine object set up to query specified schema (or public schema)
    """

    if not db_uri:
        db_uri = os.environ["DATABASE_URL"]

    connect_args = {}
    if schema:
        connect_args = {"options": f"-csearch_path={schema}"}

    return sa.create_engine(db_uri, pool_size=pool_size, connect_args=connect_args)


def get_s3_bucket(bucket=None):
    """Get S3 bucket object

    Needs environment variables:

    `AWS_ACCESS_KEY_ID`,
    `AWS_S3_ENDPOINT_URL`,
    `AWS_SECRET_ACCESS_KEY`,
    `AWS_DEFAULT_REGION`,
    `AWS_S3_ENDPOINT_URL`

    Returns
    -------
    s3.Bucket
        s3.Bucket object to interact with S3

    """

    session = boto3.session.Session()
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        return

    s3 = session.resource("s3", endpoint_url=os.environ.get("AWS_S3_ENDPOINT_URL"))
    bucket = s3.Bucket(bucket or os.environ.get("AWS_S3_BUCKET"))
    return bucket


def get_drive_service():
    json_acct_info = orjson.loads(
        base64.b64decode(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    )
    credentials = service_account.Credentials.from_service_account_info(
        json_acct_info
    )

    return build("drive", "v3", credentials=credentials)


def create_table(table, schema, sql, **params):
    """Create table under given schema by supplying SQL

    Parameters
    ----------
    table : string
        Postgres schema to use.
    schema : string
        Postgres schema to use.
    sql : string
        SQL to create table can be parametarized by SQLAlchemy parms that start with a `:` e.g `:param`.
    params : key (string), values (any)
        keys are params found in sql and values are the values to be replaced.
    """
    print(f"creating table {table}")
    t = Timer()
    t.start()
    engine = get_engine(schema)
    with engine.connect() as con:
        con.execute(
            sa.text(
                f"""DROP TABLE IF EXISTS {table};
                    CREATE TABLE {table}
                    AS
                    {sql};"""
            ),
            **params,
        )
    t.stop()


@click.group()
def cli():
    pass


def create_schema(schema):
    """Create Postgres Schema.

    Parameters
    ----------
    schema : string
        Postgres schema to create.
    """
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(
            f"""DROP SCHEMA IF EXISTS {schema} CASCADE;
                create schema {schema};"""
        )


@cli.command("create-schema", help=_first_doc_line(create_schema))
@click.argument("schema")
def _create_schema(schema):
    create_schema(schema)


def rename_schema(schema, new_schema):
    """Rename Postgres Schema.

    Parameters
    ----------
    schema : string
        Postgres schema to rename.
    new_schema : string
        New schema name.
    """
    engine = get_engine()
    drop_schema(new_schema)
    with engine.begin() as connection:
        connection.execute(f"""ALTER SCHEMA "{schema}" RENAME TO "{new_schema}";""")


@cli.command("rename-schema", help=_first_doc_line(rename_schema))
@click.argument("schema")
@click.argument("new_schema")
def _rename_schema(schema, new_schema):
    rename_schema(schema, new_schema)


def drop_schema(schema):
    """Drop Postgres Schema.

    Parameters
    ----------
    schema : string
        Postgres schema to drop.
    """
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(f"""DROP SCHEMA IF EXISTS {schema} CASCADE;""")


@cli.command("drop-schema", help=_first_doc_line(drop_schema))
@click.argument("schema")
def _drop_schema(schema):
    drop_schema(schema)


def get_bigquery_client():
    json_acct_info = orjson.loads(
        base64.b64decode(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    )
    credentials = service_account.Credentials.from_service_account_info(json_acct_info)
    return bigquery.Client(credentials=credentials)


def refresh_bigquery(source):
    print("Refreshing Bigquery")
    client = get_bigquery_client()
    dataset_id = f"{client.project}.{source}"
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"

    dataset = client.create_dataset(dataset, timeout=30)

    access_entries = list(dataset.access_entries)
    access_entries.append(
        AccessEntry("READER", "specialGroup", "allAuthenticatedUsers")
    )
    dataset.access_entries = access_entries

    dataset = client.update_dataset(dataset, ["access_entries"])


def export_bigquery(source, parquet_path, table_name):
    client = get_bigquery_client()
    dataset_id = f"{client.project}.{source}"

    table_id = f"{client.project}.{source}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET
    )

    with open(parquet_path, "rb") as source_file:
        client.load_table_from_file(
            source_file, table_id, job_config=job_config, size=None, timeout=5
        )



def sqlite_zip(source, upload=False):
    print("Making sqlite.zip")
    filepath = f'{output_dir}/{source}/sqlite.zip'
    with zipfile.ZipFile(filepath, 'w', compression=zipfile.ZIP_DEFLATED) as f_zip:
        f_zip.write(
            f'{output_dir}/{source}/sqlite.db',
            arcname=f"sqlite.db",
        )

    if upload:
        bucket_location = f"data/{source}/sqlite.zip"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


def sqlite_gzip(source, upload=False):
    print("Making sqlite.gz")
    filepath = f'{output_dir}/{source}/sqlite.db.gz'
    with open(f'{output_dir}/{source}/sqlite.db', 'rb') as f_in:
        with gzip.open(filepath, 'wb', compresslevel=5) as f_out:
            shutil.copyfileobj(f_in, f_out)

    if upload:
        bucket_location = f"data/{source}/sqlite.db.gz"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


def datapackage(source, upload=False):
    print("Making datapackage")
    filepath = f'{output_dir}/{source}/csv.zip'
    with zipfile.ZipFile(filepath, 'w', compression=zipfile.ZIP_DEFLATED) as f_zip:
        for item in glob.glob(f'{output_dir}/{source}/csv/*.csv'):
            f_zip.write(
                item,
                arcname="/".join(item.split('/')[-2:])
            )
        f_zip.write(
            f'{output_dir}/{source}/datapackage.json',
            arcname="datapackage.json"
        )

    if upload:
        bucket_location = f"data/{source}/csv.zip"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


polars_type_lookup = {
    "date": polars.Utf8,
    "text": polars.Utf8,
    "null": polars.Utf8,
    "number": polars.Float64,
    "boolean": polars.Boolean,
}

duckdb_lookup = {
    "date": "timestamp",
    "text": "text",
    "null": "text",
    "number": "double",
    "boolean": "bool",
}


def polars_generator(source):

    with open(f'{output_dir}/{source}/datapackage.json', 'r') as f:
        datapackage = json.load(f)

    # date Date, Utf8 number Float64 boolean Boolean
    
    for resource in datapackage['resources']:
        field_types = []
        for field in resource['schema']['fields']:
            field_types.append(polars_type_lookup[field["type"]])
        
        yield (resource['name'], polars.read_csv(f'{output_dir}/{source}/{resource["path"]}', dtypes=field_types))


def pandas_generator(source):
    with open(f'{output_dir}/{source}/datapackage.json', 'r') as f:
        datapackage = json.load(f)

    for resource in datapackage['resources']:
        yield (resource['name'], pandas.read_csv(f'{output_dir}/{source}/{resource["path"]}', low_memory=False))


def pandas_dataframe(source):
    return dict(pandas_generator(source))


def polars_dataframe(source):
    return dict(polars_generator(source))


@retry(tries=5)
def upload_s3(filepath, bucket_location):
    bucket = get_s3_bucket(upload_bucket)
    object = bucket.Object(
        bucket_location
    )
    object.upload_file(
        filepath, 
        ExtraArgs={"ACL": "public-read"}
    )


@retry(tries=5)
def create_parquet(source, upload=False):
    print("Creating parquet")
    if upload:
        print("and uploading to bigquery")

    os.makedirs(f'{output_dir}/{source}/parquet')
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='1GB'")

    with open(f'{output_dir}/{source}/datapackage.json', 'r') as f:
        datapackage = json.load(f)

    for resource in datapackage['resources']:
        columns = {}
        for field in resource['schema']['fields']:
            columns[field['name']] = duckdb_lookup[field["type"]]
        
        filepath = f"{output_dir}/{source}/parquet/{resource['name']}.parquet"

        con.execute(f'''
            COPY (
                SELECT 
                    * 
                FROM 
                    read_csv('{output_dir}/{source}/{resource['path']}', header=True, columns = {repr(columns)})
                )    
                TO '{filepath}' (FORMAT 'parquet');  
        ''')

        if upload:
            bucket_location = f"data/{source}/parquet/{resource['name']}.parquet"
            upload_s3(filepath, bucket_location)

            export_bigquery(source, filepath, resource['name'])


def create_avro(source, upload=False, bigquery=False):
    os.makedirs(f'{output_dir}/{source}/avro')
    for table, df in polars_generator(source):
        filepath = f'{output_dir}/{source}/avro/{table.lower()}.avro'
        df.to_avro(filepath, compression='snappy')

        if upload:
            bucket_location = f"data/{source}/avro/{table.lower()}.avro"
            upload_s3(filepath, bucket_location)
            os.unlink(filepath)


def create_pgdump(source, upload=False):
    print("Creating pg_dump")
    filepath = f'{output_dir}/{source}/pgdump.sql.gz'
    with gzip.open(filepath, 'wt+', compresslevel=5) as f:
        f.write(f'create schema {source};\n')
        f.write(f'set search_path TO {source};\n')
        for item in glob.glob(f'{output_dir}/{source}/output_*/postgresql/postgresql_schema.sql'):
            with open(item) as schema_file:
                f.write(schema_file.read())

        with open(f'{output_dir}/{source}/datapackage.json', 'r') as datapackage:
            datapackage = json.load(datapackage)

        for resource in datapackage['resources']:
            f.write(f'COPY {resource["name"].lower()} FROM stdin WITH CSV;\n')
            with open(f'{output_dir}/{source}/{resource["path"]}', 'r') as input_csv:
                for num, line in enumerate(input_csv):
                    if num == 0:
                        continue
                    f.write(line)
            f.write(r'\.')
            f.write('\n\n')

    if upload:
        bucket_location = f"data/{source}/pgdump.sql.gz"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


def create_samples(source, upload=False, size=10):
    print("Creating samples")
    output = {}
    df_output = {}

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='1GB'")

    with open(f'{output_dir}/{source}/datapackage.json', 'r') as f:
        datapackage = json.load(f)

    for resource in datapackage['resources']:
        for resource in datapackage['resources']:
            columns = {}
            for field in resource['schema']['fields']:
                columns[field['name']] = duckdb_lookup[field["type"]]

            #read_csv('{output_dir}/{source}/{resource['path']}', header=True, columns = {repr(columns)})
            df = con.execute(f'''
                SELECT 
                    * 
                FROM 
                    '{output_dir}/{source}/parquet/{resource['name']}.parquet'
                USING SAMPLE {size}
            ''').df()
            df_output[resource["name"]] = df
            output[resource["name"]] = json.loads(df.to_json(orient='split'))

    filepath = f'{output_dir}/{source}/samples.json'
    with open(f'{output_dir}/{source}/samples.json', 'w+') as f:
        json.dump(output, f)

    if upload:
        bucket_location = f"data/{source}/samples.json"
        upload_s3(filepath, bucket_location)
        shutil.rmtree(f'{output_dir}/{source}/parquet')
    
    return df_output
        

def download_file(url, source, name=None):
    print('Downloading File')
    os.makedirs(f'{output_dir}/{source}_download', exist_ok=True)
    if not name:
        name = url.split('/')[-1]

    filename = f'{output_dir}/{source}_download/{name}'

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_files_s3(source, s3_path_pattern, latest=False, bucket="bodsdata"):
    print('Downloading Files')

    os.makedirs(f'{output_dir}/{source}_download', exist_ok=True)
    bucket = get_s3_bucket(bucket)
    items = []

    for item in bucket.objects.all():
        if re.search(s3_path_pattern, item.key):
            items.append(item.key)

    items.sort()

    if latest and items:
        items = [items[-1]]

    for item in items:
        file_name = item.split('/')[-1]
        bucket.download_file(item, f'{output_dir}/{source}_download/{file_name}')


def remove_download(source):
    shutil.rmtree(f'{output_dir}/{source}_download', ignore_errors=True)


def remove_output(source):
    shutil.rmtree(f'{output_dir}/{source}', ignore_errors=True)


def run_flatterer(source, statement_type, sample=None):
    print(f"Flattening {statement_type} to make CSV and SQLite")
    def flatten_iterator():
        for item in glob.glob(f'{output_dir}/{source}_download/*'):
            opener = open
            if item.endswith('.gz'):
                opener = gzip.open
            file_name = item.split('/')[-1]

            with opener(f'{output_dir}/{source}_download/{file_name}') as f:        
                start_of_file = f.read(100).strip()
                path = ''
                if start_of_file == '[':
                    path = 'item'

                f.seek(0)
                for (num, object) in enumerate(ijson.items(f, path, multiple_values=True)):
                    if num % 1000000 == 0 and num:
                        print(f'number of rows processed {num}')
                    if sample and num == sample:
                        return

                    if object["statementType"] == statement_type + 'Statement':
                        yield object

    short_statement_type = statement_type.replace('ownershipOrControl', 'ooc')

    flatterer.flatten(
        flatten_iterator(), 
        f'{output_dir}/{source}/output_{statement_type}', 
        main_table_name='statement',
        force=True, table_prefix=f'{short_statement_type}_', sqlite=True, sqlite_path=f'{output_dir}/{source}/sqlite.db')


def flatten(source, sample):
    run_flatterer(source, 'person', sample)
    run_flatterer(source, 'entity', sample)
    run_flatterer(source, 'ownershipOrControl', sample)

    os.makedirs(f'{output_dir}/{source}/csv', exist_ok=True)

    for item in glob.glob(f'{output_dir}/{source}/*/csv/*.csv'):
        shutil.move(item, f"{output_dir}/{source}/csv/{item.split('/')[-1]}")

    datapackage = {"profile": "tabular-data-package", "resources": []}

    for item in glob.glob(f'{output_dir}/{source}/*/datapackage.json'):
        with open(item) as f:
            datapackage["resources"].extend(json.load(f)['resources'])
    
    with open(f'{output_dir}/{source}/datapackage.json', 'w+') as f:
        json.dump(datapackage, f, indent=2)


def publish_metadata(source, title="", description=""):
    print("publishing metadata")

    out = {"parquet": {},
           "update_date": datetime.datetime.now().isoformat()[:10], 
           "description": description,
           "title": title}

    bucket = get_s3_bucket(upload_bucket)
    bucket_url = f"{bucket.meta.client.meta.endpoint_url}/{bucket.name}"

    all_sources = set()

    for item in sorted(bucket.objects.all(), key=lambda x: x.key.split("/")[-1]):
        item_url = f"{bucket_url}/{item.key}"

        parts = item.key.split("/")
        if parts[0] == 'data' and len(parts) > 2:
            all_sources.add(parts[1])

        if parts[0] != 'data' or parts[1] != source:
            continue

        file_name = parts[-1]

        if file_name.endswith("csv.zip"):
            out["csv"] = item_url
        if file_name.endswith("sqlite.zip"):
            out["sqlite_zip"] = item_url
        if file_name.endswith("sqlite.db.gz"):
            out["sqlite_gzip"] = item_url
        if file_name.endswith("sql.gz"):
            out["pg_dump"] = item_url
        if file_name.endswith("parquet"):
            out["parquet"][file_name] = item_url

    with open(f'{output_dir}/{source}/datapackage.json') as samples_file:
        out['datapackage'] = json.load(samples_file)

    with open(f'{output_dir}/{source}/samples.json') as samples_file:
        out['samples'] = json.load(samples_file)

    filepath = f'{output_dir}/{source}/metadata.json'
    with open(filepath, 'w+') as f:
        json.dump(out, f, indent=2)

    bucket_location = f"data/{source}/metadata.json"
    upload_s3(filepath, bucket_location)

    filepath = f'{output_dir}/all_sources.json'
    with open(filepath, 'w+') as f:
        json.dump(list(all_sources), f, indent=2)

    bucket_location = f"data/all_sources.json"
    upload_s3(filepath, bucket_location)


def publish_datasettes():
    all_sources = requests.get(s3_data_location + 'all_sources.json').json()

    with tempfile.TemporaryDirectory() as tmpdirname:
        private_key = Path(tmpdirname) / 'render_private_key'
        private_key.write_text(os.environ['RENDER_SSH_KEY'])
        
        c = Connection(
        host=render_host,
        connect_kwargs={
            "key_filename": str(private_key),
            }
        )
        for source in all_sources:
            sqlite_gz = s3_data_location + f'{source}/sqlite.db.gz'
            c.run(f'curl {sqlite_gz} | gunzip > /var/data/{source}.db')

        requests.get(os.environ['RENDER_DEPLOY_HOOK'])


def build_website():
    from bodsdataweb.app import app
    from flask_frozen import Freezer
    freezer = Freezer(app)
    freezer.freeze()



if __name__ == "__main__":
    cli()
