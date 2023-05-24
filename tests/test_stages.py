import pytest
from pathlib import Path
from distutils.dir_util import copy_tree
import zipfile
import json

import bodsdata

def setup_directories(source, output_dir):
    """Setup temporary directory structure and copy input data from fixtures"""
    out_dir = Path(output_dir) / source
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(output_dir) / f"{source}_download"
    copy_tree('tests/fixtures/gzip', str(source_dir))
    return out_dir

@pytest.fixture(scope="session")
def output_dir(tmpdir_factory):
    """Fixture to create temporary directory"""
    temp_dir = tmpdir_factory.mktemp("tmp-data")
    return temp_dir

def test_json_zip(output_dir):
    """Test creation of output json.zip file containing merged JSON Lines from input files"""
    source = 'test-source'
    d = setup_directories(source, output_dir)
    bodsdata.output_dir = output_dir
    bodsdata.json_zip(source)
    with zipfile.ZipFile(d / 'json.zip') as test_zip:
        with test_zip.open(f'{source}.json') as output_file:
            data = output_file.readlines()
            print(data)
            assert len(data) == 20
            assert json.loads(data[0].strip())['interestedParty']['describedByPersonStatement'] == '14105856581894595060'
