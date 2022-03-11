import base64
import pandas
import polars
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
from pathlib import Path
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

EMIT_OBJECT_PATHS = []


def flatten_object(obj, current_path=""):
    for key, value in list(obj.items()):
        if isinstance(value, dict):
            yield from flatten_object(value, f"{current_path}{key}_")
        else:
            yield f"{current_path}{key}", value


def traverse_object(obj, emit_object, full_path=tuple(), no_index_path=tuple()):
    for key, value in list(obj.items()):
        if isinstance(value, list) and value and isinstance(value[0], dict):
            for num, item in enumerate(value):
                if not isinstance(item, dict):
                    item = {"__error": "A non object is in array of objects"}
                yield from traverse_object(
                    item, True, full_path + (key, num), no_index_path + (key,)
                )
            obj.pop(key)
        elif isinstance(value, list):
            if not all(isinstance(item, str) for item in value):
                obj[key] = json.dumps(value)
        elif isinstance(value, dict):
            if no_index_path + (key,) in EMIT_OBJECT_PATHS:
                yield from traverse_object(
                    value, True, full_path + (key,), no_index_path + (key,)
                )
                obj.pop(key)
            else:
                yield from traverse_object(
                    value, False, full_path + (key,), no_index_path + (key,)
                )

    if obj and emit_object:
        yield obj, full_path, no_index_path


@functools.lru_cache(1000)
def path_info(full_path, no_index_path):
    all_paths = []
    for num, part in enumerate(full_path):
        if isinstance(part, int):
            all_paths.append(full_path[: num + 1])

    parent_paths = all_paths[:-1]
    path_key = all_paths[-1] if all_paths else []

    object_key = ".".join(str(key) for key in path_key)
    parent_keys_list = [
        ".".join(str(key) for key in parent_path) for parent_path in parent_paths
    ]
    parent_keys_no_index = [
        "_".join(str(key) for key in parent_path if not isinstance(key, int))
        for parent_path in parent_paths
    ]
    object_type = "_".join(str(key) for key in no_index_path) or "release"
    parent_keys = (dict(zip(parent_keys_no_index, parent_keys_list)),)
    return object_key, parent_keys_list, parent_keys_no_index, object_type, parent_keys


def create_rows(result):
    rows = []
    awards = {}
    parties = {}
    for object, full_path, no_index_path in traverse_object(result.compiled_release, 1):

        (
            object_key,
            parent_keys_list,
            parent_keys_no_index,
            object_type,
            parent_keys,
        ) = path_info(full_path, no_index_path)

        object[
            "_link"
        ] = f'{result.compiled_release_id}{"." if object_key else ""}{object_key}'
        object["_link_release"] = str(result.compiled_release_id)
        for no_index_path, full_path in zip(parent_keys_no_index, parent_keys_list):
            object[
                f"_link_{no_index_path}"
            ] = f"{result.compiled_release_id}.{full_path}"

        row = dict(
            compiled_release_id=result.compiled_release_id,
            object_key=object_key,
            parent_keys=parent_keys,
            object_type=object_type,
            object=object,
        )

        rows.append(row)

    for row in rows:
        object = row["object"]
        try:
            row["object"] = orjson.dumps(dict(flatten_object(object))).decode()
        except TypeError:
            # orjson more strict about ints
            row["object"] = json.dumps(dict(flatten_object(object)))

        row["parent_keys"] = orjson.dumps(row["parent_keys"]).decode()

    return [list(row.values()) for row in rows]


@cli.command("release-objects")
@click.argument("schema")
def _release_objects(schema):
    release_objects(schema)


def release_objects(schema):
    engine = get_engine(schema)
    engine.execute(
        """
        DROP TABLE IF EXISTS _release_objects;
        CREATE TABLE _release_objects(compiled_release_id bigint,
        object_key TEXT, parent_keys JSONB, object_type TEXT, object JSONB);
        """
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        with engine.begin() as connection, Timer():
            connection = connection.execution_options(stream_results=True, max_row_buffer=1000)
            results = connection.execute(
                "SELECT compiled_release_id, compiled_release FROM _compiled_releases"
            )
            paths_csv_file = tmpdirname + "/paths.csv"

            print("Making CSV file")
            with gzip.open(paths_csv_file, "wt", newline="") as csv_file, Timer():
                csv_writer = csv.writer(csv_file)
                for result in results:
                    csv_writer.writerows(create_rows(result))

        print("Uploading Data")
        with engine.begin() as connection, gzip.open(
            paths_csv_file, "rt"
        ) as f, Timer():
            dbapi_conn = connection.connection
            copy_sql = f"COPY {schema}._release_objects FROM STDIN WITH CSV"
            cur = dbapi_conn.cursor()
            cur.copy_expert(copy_sql, f)


def process_schema_object(path, current_name, flattened, obj):
    string_path = ("_".join(path)) or "release"

    properties = obj.get("properties", {})  # an object may have patternProperties only
    current_object = flattened.get(string_path)

    if current_object is None:
        current_object = {}
        flattened[string_path] = current_object

    for name, prop in list(properties.items()):
        prop_type = prop["type"]
        prop_info = dict(
            schema_type=prop["type"],
            description=prop.get("description"),
        )
        if prop_type == "object":
            if path + (name,) in EMIT_OBJECT_PATHS:
                flattened = process_schema_object(
                    path + (name,), tuple(), flattened, prop
                )
            else:
                flattened = process_schema_object(
                    path, current_name + (name,), flattened, prop
                )
        elif prop_type == "array":
            if "object" not in prop["items"]["type"]:
                current_object["_".join(current_name + (name,))] = prop_info
            else:
                flattened = process_schema_object(
                    path + current_name + (name,), tuple(), flattened, prop["items"]
                )
        else:
            current_object["_".join(current_name + (name,))] = prop_info

    return flattened


def link_info(link_name):
    name = link_name[6:]
    if not name:
        doc = "Link to this row that can be found in other tables"
    else:
        doc = f"Link to the {name} row that this row relates to"

    return {"name": link_name, "description": doc, "type": "string"}


@cli.command("schema-analysis")
@click.argument("schema")
def _schema_analysis(schema):
    schema_analysis(schema)


# only accept years 1000-3999
DATE_RE = r'^([1-3]\d{3})-(\d{2})-(\d{2})([T ](\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)((-(\d{2}):(\d{2})|Z)?))?$'


def schema_analysis(schema):

    create_table(
        "_object_type_aggregate",
        schema,
        f"""SELECT
              object_type,
              each.key,
              CASE
                 WHEN jsonb_typeof(value) != 'string'
                     THEN jsonb_typeof(value)
                 WHEN (value ->> 0) ~ '{DATE_RE}'
                     THEN 'datetime'
                 ELSE 'string'
              END value_type,
              count(*)
           FROM
              _release_objects ro, jsonb_each(object) each
           GROUP BY 1,2,3;
        """,
    )

    create_table(
        "_object_type_fields",
        schema,
        """SELECT
              object_type,
              key,
              CASE WHEN
                  count(*) > 1
              THEN 'string'
              ELSE max(value_type) end value_type,
              SUM("count") AS "count"
           FROM
              _object_type_aggregate
           WHERE
              value_type != 'null'
           GROUP BY 1,2;
        """,
    )

    schema_info = process_schema_object(
        tuple(), tuple(), {}, JsonRef.replace_refs(schema)
    )

    with get_engine(schema).begin() as connection:
        result = connection.execute(
            """SELECT object_type, jsonb_object_agg(key, value_type) fields FROM _object_type_fields GROUP BY 1;"""
        )
        result_dict = {row.object_type: row.fields for row in result}

        object_type_order = ["release"]
        for key in schema_info:
            if key in result_dict:
                object_type_order.append(key)
        for key in result_dict:
            if key not in object_type_order:
                object_type_order.append(key)

        object_details = {}

        for object_type in object_type_order:
            fields = result_dict[object_type]

            details = [link_info("_link"), link_info("_link_release")]
            fields_added = set(["_link", "_link_release"])

            for field in fields:
                if field.startswith("_link_") and field not in fields_added:
                    details.append(link_info(field))
                    fields_added.add(field)

            schema_object_detials = schema_info.get(object_type, {})

            for schema_field, field_info in schema_object_detials.items():
                if schema_field not in fields:
                    continue
                detail = {"name": schema_field, "type": fields[schema_field]}
                detail.update(field_info)
                details.append(detail)
                fields_added.add(schema_field)

            for field in sorted(fields):
                if field in fields_added:
                    continue
                details.append(
                    {
                        "name": field,
                        "description": "No Docs as not in OCDS",
                        "type": fields[field],
                    }
                )

            object_details[object_type] = details

        connection.execute(
            """
            DROP TABLE IF EXISTS _object_details;
            CREATE TABLE _object_details(id SERIAL, object_type text, object_details JSONB);
        """
        )

        for object_type, object_details in object_details.items():
            connection.execute(
                sa.text(
                    "insert into _object_details(object_type, object_details) values (:object_type, :object_details)"
                ),
                object_type=object_type,
                object_details=json.dumps(object_details),
            )


def create_field_sql(object_details, sqlite=False):
    fields = []
    lowered_fields = set()
    fields_with_type = []
    for num, item in enumerate(object_details):
        name = item["name"]

        if sqlite and name.lower() in lowered_fields:
            name = f'{name}_{num}'

        type = item["type"]
        if type == "number":
            field = f'"{name}" numeric'
        elif type == "array":
            field = f'"{name}" JSONB'
        elif type == "boolean":
            field = f'"{name}" boolean'
        elif type == "datetime":
            field = f'"{name}" timestamp'
        else:
            field = f'"{name}" TEXT'

        lowered_fields.add(name.lower())
        fields.append(f'"{name}"')
        fields_with_type.append(field)

    return ", ".join(fields), ", ".join(fields_with_type)


@cli.command("postgres-tables")
@click.argument("schema")
def _postgres_tables(schema):
    postgres_tables(schema)


def postgres_tables(schema, drop_release_objects=True):
    with get_engine(schema).begin() as connection:
        result = list(
            connection.execute(
                "SELECT object_type, object_details FROM _object_details order by id"
            )
        )

    for object_type, object_details in result:
        field_sql, as_sql = create_field_sql(object_details)
        table_sql = f"""
           SELECT {field_sql}
           FROM _release_objects, jsonb_to_record(object) AS ({as_sql})
           WHERE object_type = :object_type
        """
        create_table(object_type, schema, table_sql, object_type=object_type)

    if drop_release_objects:
        with get_engine(schema).begin() as connection:
            connection.execute("DROP TABLE IF EXISTS _release_objects")


def generate_object_type_rows(object_detials_results):
    yield ["Sheet Name", "Name", "Docs", "Type", "Schema Type"]

    for object_type, object_details in object_detials_results:
        for field in object_details:
            yield [
                object_type,
                field.get("name"),
                field.get("description"),
                field.get("type"),
                str(field.get("schema_type")),
            ]



@cli.command("export-bigquery")
@click.argument("schema")
@click.argument("name")
@click.argument("date")
def _export_bigquery(schema, name, date):
    export_bigquery(schema, name, date)


def export_bigquery(schema, name, date):

    json_acct_info = orjson.loads(
        base64.b64decode(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    )

    credentials = service_account.Credentials.from_service_account_info(json_acct_info)

    client = bigquery.Client(credentials=credentials)

    with tempfile.TemporaryDirectory() as tmpdirname, get_engine(
        schema
    ).begin() as connection:
        dataset_id = f"{client.project}.{name}"
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

        result = connection.execute(
            "SELECT object_type, object_details FROM _object_details order by id"
        )
        for object_type, object_details in list(result):
            print(f"loading {object_type}")
            result = connection.execute(
                sa.text(
                    f'SELECT to_jsonb("{object_type.lower()}") AS object FROM "{object_type.lower()}"'
                )
            )
            schema = create_avro_schema(object_type, object_details)

            with open(f"{tmpdirname}/{object_type}.avro", "wb") as out:
                writer(
                    out,
                    parse_schema(schema),
                    generate_avro_records(result, object_details),
                    validator=True,
                    codec="deflate",
                )

            table_id = f"{client.project}.{name}.{object_type}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO
            )

            with open(f"{tmpdirname}/{object_type}.avro", "rb") as source_file:
                client.load_table_from_file(
                    source_file, table_id, job_config=job_config, size=None, timeout=5
                )

            bucket = get_s3_bucket()
            if bucket:
                object = bucket.Object(
                    f"{name}/avro/ocdsdata_{name}_{object_type}.avro"
                )
                object.upload_file(
                    f"{tmpdirname}/{object_type}.avro", ExtraArgs={"ACL": "public-read"}
                )
                metadata_object = bucket.Object(
                    f"{name}/metadata/avro_upload_dates/{date}"
                )
                metadata_object.put(ACL="public-read", Body=b"")


def sqlite_zip(source, upload=False):
    filepath = f'{output_dir}/{source}/sqlite.zip'
    with zipfile.ZipFile(filepath, 'w') as f_zip:
        f_zip.write(
            f'{output_dir}/{source}/sqlite.db',
            arcname=f"sqlite.db",
        )

    if upload:
        bucket_location = f"data/{source}/sqlite.zip"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


def sqlite_gzip(source, upload=False):
    filepath = f'{output_dir}/{source}/sqlite.db.gz'
    with open(f'{output_dir}/{source}/sqlite.db', 'rb') as f_in:
        with gzip.open(filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    if upload:
        bucket_location = f"data/{source}/sqlite.db.gz"
        upload_s3(filepath, bucket_location)
        os.unlink(filepath)


def datapackage(source, upload=False):
    filepath = f'{output_dir}/{source}/csv.zip'
    with zipfile.ZipFile(filepath, 'w') as f_zip:
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
    bucket = get_s3_bucket("bodsdata")
    object = bucket.Object(
        bucket_location
    )
    object.upload_file(
        filepath, 
        ExtraArgs={"ACL": "public-read"}
    )
    metadata_object = bucket.Object(
        bucket_location
    )
    metadata_object.put(ACL="public-read", Body=b"")


def create_parquet(source, upload=False):
    os.makedirs(f'{output_dir}/{source}/parquet')
    for table, df in polars_generator(source):
        filepath = f'{output_dir}/{source}/parquet/{table.lower()}.parquet'
        df.to_parquet(filepath)

        if upload:
            bucket_location = f"data/{source}/parquet/{table.lower()}.parquet"
            upload_s3(filepath, bucket_location)
            os.unlink(filepath)


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
    filepath = f'{output_dir}/{source}/pgdump.sql.gz'
    with gzip.open(filepath, 'wt+') as f:
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
    output = {}
    df_output = {}
    for table, df in pandas_generator(source):
        if df.shape[0] > size:
            df_output[table] = df.sample(size)
            output[table] = json.loads(df.sample(size).to_json(orient='split'))
        else:
            df_output[table] = df
            output[table] = json.loads(df.to_json(orient='split'))
    
    filepath = f'{output_dir}/{source}/samples.json'
    with open(f'{output_dir}/{source}/samples.json', 'w+') as f:
        json.dump(output, f)

    if upload:
        bucket_location = f"data/{source}/samples.json"
        upload_s3(filepath, bucket_location)
    
    return df_output


def download_file(url, source, name=None):
    os.makedirs(f'{output_dir}/{source}_download', exist_ok=True)
    if not name:
        name = url.split('/')[-1]

    filename = f'{output_dir}/{source}_download/{name}'

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_files_s3(source, s3_path_prefix, latest=False):

    os.makedirs(f'{output_dir}/{source}_download', exist_ok=True)
    bucket = get_s3_bucket("bodsdata")
    items = []

    for item in bucket.objects.all():
        if item.key.startswith(s3_path_prefix):
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
    def flatten_iterator():
        for item in glob.glob(f'{output_dir}/{source}_download/*'):
            opener = open
            if item.endswith('.gz'):
                opener = gzip.open
            file_name = item.split('/')[-1]

            with opener(f'{output_dir}/{source}_download/{file_name}') as f:        
                for (num, object) in enumerate(ijson.items(f, '', multiple_values=True)):
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


def publish_metadata(source):
    out = {"parquet": {}, "avro": {}, "update_date": datetime.datetime.now().isoformat()[:10]}

    bucket = get_s3_bucket('bodsdata')
    bucket_url = f"{bucket.meta.client.meta.endpoint_url}/{bucket.name}"

    for item in sorted(bucket.objects.all(), key=lambda x: x.key.split("/")[-1]):
        item_url = f"{bucket_url}/{item.key}"

        parts = item.key.split("/")
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
        if file_name.endswith("avro"):
            out["avro"][file_name] = item_url
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


if __name__ == "__main__":
    cli()

