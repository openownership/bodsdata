# Beneficial ownership data analysis tools

Data analysis tools to help analysts, journalists and anyone wanting to examine and dive into beneficial ownership data published in line with the Beneficial Ownership Data Standard.

## Install

```bash
pip install git+https://github.com/openownership/bodsdata.git@main#egg=bodsdata
```

## Usage

To get the initial flattened data you need to run the following, giving an output directory and source name of your choice.

```python
import bodsdata

# Set output directory default to tmp folder if not set.
bodsdata.output_dir = 'my_dir'

# Have to name a source
source = 'my_data_source'

# Downloand individual file or zip file.
bodsdata.download_file('some_url', source=source)

# Flatten all the data into CSV files and an SQLite file.
# this step needs to happen before other functions can work
bodsdata.flatten(source)
```

After the above is run there are various choices to how you can use the data.

```python

# Create dictionary of pandas dataframes
dataframes = bodsdata.pandas_dataframe(source)

# Create dictionary of polars dataframes
dataframes = bodsdata.polars_dataframe(source)

# Create parquet files for spark or other data tools
bodsdata.create_parquet(source)

# Create pg_dump for inserting into postgres database
bodsdata.create_pgdump(source)

# Zip CSV and create datapackage 
bodsdata.datapackage(source)

# zip or gzip sqlite files 
bodsdata.sqlite_zip(source)
bodsdata.sqlite_gzip(source)
````

## Development
To test website locally:

```
cd bodsdataweb
flask run
```

### Change web-site theme

Theme lives at `bodsdataweb/bootswatch/dist/bodsdata/`

Modify `_bootswatch.scss` for big changes and `_variables.scss` for changes to variables.

To build theme:

```
cd bodsdataweb/bootswatch
// for one of build
grunt swatch 
// to watch changes
grunt  watch 
```

### Deploy website

```
bodsdata.build_website(source)
```

You will need the following envirment variables to be set:

```
export AWS_ACCESS_KEY_ID=XXX
export AWS_SECRET_ACCESS_KEY=XXX
export AWS_DEFAULT_REGION=eu-west-1
```
