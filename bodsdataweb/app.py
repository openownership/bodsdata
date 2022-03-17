from flask import Flask, config, abort
from flask import render_template
from flask_bootstrap import Bootstrap5
from functools import lru_cache
import requests
import markdown


app = Flask(__name__)

config = {
   "FREEZER_DESTINATION": "/tmp/bodsdata-web/"
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


@app.route("/about/")
def about():
    return render_template('about.html', metadata=get_metadata())


@app.route("/source/<source>/")
def source(source):
    metadata = get_metadata()
    if source not in metadata:
        abort(404)

    return render_template('source.html', metadata=metadata[source], source=source)


