from flask import Flask, config, abort
from flask import render_template
from flask_bootstrap import Bootstrap5
from functools import lru_cache
import requests
import markdown


app = Flask(__name__)

config = {
   "FREEZER_DESTINATION": "build",
   "FREEZER_STATIC_IGNORE": ['*.scss']
}

app.config.from_mapping(config)

bootstrap = Bootstrap5(app)

base_data_url = 'https://oo-bodsdata.s3.eu-west-1.amazonaws.com/data'


@lru_cache()
def get_metadata():
    all_data = {}
    sources = requests.get(base_data_url + '/all_sources.json').json()
    for source in sources:
        field_tables = {}
        sample_tables = {}
        metadata = requests.get(f'{base_data_url}/{source}/metadata.json').json()
        metadata['description_html'] = markdown.markdown(metadata.get('description', ''))

        for resource in metadata['datapackage']['resources']:
            name = resource['name']
            rows = []
            for num, field in enumerate(resource['schema']['fields']):
                rows.append({
                    "field_name": field['name'], 
                    "field_type": field['type'],
                    "field_count": f'{field["count"]:,}', 
                    "sample_1": metadata['samples'][name]['data'][0][num], 
                    "sample_2": metadata['samples'][name]['data'][1][num], 
                    "sample_3": metadata['samples'][name]['data'][2][num], 
                })
            field_tables[name] = rows

        for name, details in metadata['samples'].items():
            headers = [(header, header) for header in details['columns']]
            data = []

            for row in details['data']:
                data.append(dict(zip(details['columns'], row)))

            sample_tables[name] = {"headers": headers, "data": data}

        metadata['field_tables'] = field_tables
        metadata['sample_tables'] = sample_tables

        all_data[source] = metadata
    
    return all_data
        

@app.route("/")
def home():
    return render_template('home.html', metadata=get_metadata())


ABOUT_TEXT = '''
[Open Ownership](https://www.openownership.org/) believes that information on the true owners of companies is an essential part of a well-functioning economy and society. Public registers of beneficial owners give access to high quality data about who owns, controls, and benefits from companies and their profits. Open Ownership [advocates](https://www.openownership.org/principles/structured-data/) that the utility of beneficial ownership data is enhanced when the data is available in a structured format.

To help with the publication of structured, high-quality, interoperable beneficial ownership data, Open Ownership has developed the [Beneficial Ownership Data Standard](https://standard.openownership.org/) in partnership with the [Open Data Services Co-operative](https://opendataservices.coop/). 

To demonstrate the value of publishers using the [Beneficial Ownership Data Standard](https://standard.openownership.org/) and being able to connect or analyse beneficial data from multiple jurisdictions, we created the [Open Ownership Register](https://register.openownership.org/) which imports data from Denmark, Slovakia and the UK, structures the data in line with [v0.1 of our Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.1.0/) offering it for anyone to [download](https://register.openownership.org/download) under the [Open Data Commons Attribution License](https://opendatacommons.org/licenses/by/1-0/). 

These open source beneficial ownership data analysis tools aim to help analysts, journalists and anyone wanting to examine and dive into beneficial ownership data published in line with the [Beneficial Ownership Data Standard](https://standard.openownership.org/).

This website was created by [Open Data Services Co-operative](https://opendataservices.coop/) in partnership with [Open Ownership](https://www.openownership.org/).

The following open source tools and libraries are used to power the data analysis tools: 

* [Big Query](https://cloud.google.com/bigquery) - Google
* [Data Package](https://specs.frictionlessdata.io/data-package/) - [Frictionless Data](https://frictionlessdata.io/)
* [Datasette](https://datasette.io/) - [Simon Willison](https://simonwillison.net/)
* [Deep Note](https://deepnote.com/)
* [DuckDb](https://github.com/duckdb/duckdb)
* [Flatterer](https://flatterer.opendata.coop/) - [Open Data Services Co-operative](https://opendataservices.coop/)
* [Pandas](https://pandas.pydata.org/)
* [Polars](https://www.pola.rs/)
* [PYPI](https://pypi.org/)
* [SQLite](https://www.sqlite.org/index.html)

The open source code powering this website and the data processing tools is [available on Github](https://github.com/openownership/bodsdata).
'''

@app.route("/about/")
def about():
    return render_template('about.html', metadata=get_metadata(), about=markdown.markdown(ABOUT_TEXT))


@app.route("/source/<source>/")
def source(source):
    metadata = get_metadata()
    if source not in metadata:
        abort(404)

    return render_template('source.html', metadata=metadata[source], source=source)


