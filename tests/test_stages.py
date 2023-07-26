import pytest
import tempfile
from pathlib import Path
from distutils.dir_util import copy_tree
import zipfile
import json
import pandas as pd
import gzip
from unittest.mock import patch, Mock
import shutil
import types

import bodsdata

def copy_file(src, dest):
    shutil.copy(src, dest)


class TestDownloadFile:
    """Test download of single file"""
    source = 'test-source0'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    def test_download_files_s3(self, temp_dir, output_dir):
        """Test downloading single input file"""
        with patch('bodsdata.get_s3_bucket') as mock_get_s3_bucket:
            download = 'tests/fixtures/gzip/test-bods-data-1.json.gz'
            mock_bucket = Mock()
            mock_bucket.objects.all.return_value = [types.SimpleNamespace(key=download)]
            mock_bucket.download_file.side_effect = copy_file
            mock_get_s3_bucket.return_value = mock_bucket
            bucket = 'oo-register-v2'
            bodsdata.output_dir = temp_dir
            bodsdata.download_files_s3(s3_path_pattern=download, source=self.source, latest=False, bucket=bucket)
            assert (temp_dir / f"{self.source}_download" / "test-bods-data-1.json.gz").is_file()


class TestDownloadDirectory:
    """Test download of single file"""
    source = 'test-source1'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    def test_download_files_s3(self, temp_dir, output_dir):
        """Test downloading directory of input files"""
        with patch('bodsdata.get_s3_bucket') as mock_get_s3_bucket:
            download = 'tests/fixtures/gzip'
            mock_bucket = Mock()
            mock_bucket.objects.all.return_value = [types.SimpleNamespace(key=str(d)) for d in Path(download).iterdir()]
            mock_bucket.download_file.side_effect = copy_file
            mock_get_s3_bucket.return_value = mock_bucket
            bucket = 'oo-register-v2'
            bodsdata.output_dir = temp_dir
            bodsdata.download_files_s3(s3_path_pattern=download, source=self.source, latest=False, bucket=bucket)
            for d in Path(download).iterdir():
                file_name = str(d).split('/')[-1]
                assert (temp_dir / f"{self.source}_download" / file_name).is_file()


class TestPipeline:
    """Test pipeline stages"""
    source = 'test-source2'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/gzip", str(source_dir))
        return source_dir

    def test_flatten(self, temp_dir, output_dir, source_dir):
        """Test flattening of JSON Lines input files"""
        bodsdata.output_dir = temp_dir
        bodsdata.flatten(self.source, False)
        print(list(output_dir.iterdir()))
        csv_dir = output_dir / "csv"
        with open(csv_dir / "ooc_statement.csv") as ooc_statements:
            data = ooc_statements.readlines()
            print(data)
            assert len(data) == 17
        with open(csv_dir / "person_statement.csv") as person_statements:
            data = person_statements.readlines()
            print(data)
            assert len(data) == 5
        parquet_dir = output_dir / "parquet"
        ooc_statements = pd.read_parquet(parquet_dir / 'ooc_statement.parquet', engine='fastparquet')
        assert ooc_statements.shape[0] == 16
        person_statements = pd.read_parquet(parquet_dir / 'person_statement.parquet', engine='fastparquet')
        assert person_statements.shape[0] == 4
        with open(output_dir / 'datapackage.json') as output_file:
            json_data = json.load(output_file)
            print(json_data)
            assert json_data['profile'] == 'tabular-data-package'
            assert len(json_data['resources']) == 7
            person_statement_resource = [resource for resource in json_data['resources'] if resource['name'] == 'person_statement'][0]
            assert [field for field in person_statement_resource['schema']['fields'] if field['name'] == 'statementID'][0]['count'] == 4
            ooc_statement_resource = [resource for resource in json_data['resources'] if resource['name'] == 'ooc_statement'][0]
            assert [field for field in ooc_statement_resource['schema']['fields'] if field['name'] == 'statementID'][0]['count'] == 16

    def test_json_zip(self, temp_dir, output_dir, source_dir):
        """Test creation of output json.zip file containing merged JSON Lines from input files"""
        bodsdata.output_dir = temp_dir
        bodsdata.json_zip(self.source)
        with zipfile.ZipFile(output_dir / 'json.zip') as test_zip:
            with test_zip.open(f'{self.source}.json') as output_file:
                data = output_file.readlines()
                statements = [json.loads(d.strip()) for d in data]
                assert len(statements) == 20
                assert len([s for s in statements if s["statementType"] == "ownershipOrControlStatement"]) == 16
                assert '14105856581894595060' in [s['interestedParty']['describedByPersonStatement'] for s in statements if
                                                                        s["statementType"] == "ownershipOrControlStatement"]
                assert len([s for s in statements if s["statementType"] == "personStatement"]) == 4
                assert '11262152698769124205' in [s["statementID"] for s in statements if s["statementType"] == "personStatement"]

    def test_sqlite_zip(self, temp_dir, output_dir, source_dir):
        """Test creation of output sqlite.db.gz file"""
        bodsdata.output_dir = temp_dir
        bodsdata.sqlite_zip(self.source)
        with zipfile.ZipFile(output_dir / 'sqlite.zip') as test_zip:
            files = test_zip.namelist()
            print(files)
            assert 'sqlite.db' in files

    def test_sqlite_gzip(self, temp_dir, output_dir, source_dir):
        """Test creation of output sqlite.db.gz file"""
        bodsdata.output_dir = temp_dir
        bodsdata.sqlite_gzip(self.source)
        assert (output_dir / 'sqlite.db.gz').is_file()

    def test_create_samples(self, temp_dir, output_dir, source_dir):
        """Test creation of output samples.json file"""
        bodsdata.output_dir = temp_dir
        bodsdata.create_samples(self.source)
        with open(output_dir / 'samples.json') as output_file:
            json_data = json.load(output_file)
            assert len(json_data['person_statement']['data']) == 4
            assert len(json_data['ooc_statement']['data']) == 10

    def test_create_parquet_zip(self, temp_dir, output_dir, source_dir):
        """Test creation of output parquet.zip file"""
        bodsdata.output_dir = temp_dir
        bodsdata.create_parquet_zip(self.source)
        with zipfile.ZipFile(output_dir / "parquet.zip") as parquet_zip:
            files = parquet_zip.namelist()
            assert 'person_statement.parquet' in files
            assert 'person_names.parquet' in files
            assert 'person_identifiers.parquet' in files
            assert 'person_nationalities.parquet' in files
            assert 'person_addresses.parquet' in files
            assert 'ooc_statement.parquet' in files
            assert 'ooc_interests.parquet' in files

    def test_create_pgdump(self, temp_dir, output_dir, source_dir):
        """Test creation of output parquet.zip file"""
        bodsdata.output_dir = temp_dir
        bodsdata.create_pgdump(self.source)
        assert (output_dir / 'pgdump.sql.gz').is_file()
        with gzip.open(output_dir / 'pgdump.sql.gz','rb') as pgdump:
            data = pgdump.readlines()
            assert len([d for d in data if d.startswith(b'COPY')]) == 7

    def test_datapackage(self, temp_dir, output_dir, source_dir):
        """Test creation of output datapackage file"""
        bodsdata.output_dir = temp_dir
        bodsdata.datapackage(self.source)
        with zipfile.ZipFile(output_dir / "csv.zip") as datapackage_zip:
            files = datapackage_zip.namelist()
            assert 'csv/person_addresses.csv' in files
            assert 'csv/person_nationalities.csv' in files
            assert 'csv/person_names.csv' in files
            assert 'csv/person_identifiers.csv' in files
            assert 'csv/ooc_statement.csv' in files
            assert 'csv/ooc_interests.csv' in files
            assert 'csv/person_statement.csv' in files
            assert 'datapackage.json' in files

    def test_make_datasette_infofile(self, temp_dir, output_dir, source_dir):
        """Test creation of output inspect-data.json file"""
        bodsdata.output_dir = temp_dir
        bodsdata.make_datasette_infofile(self.source, False)
        with open(output_dir / 'inspect-data.json') as output_file:
            json_data = json.load(output_file)
            print(json_data)
            assert json_data['test-source2']['file'] == 'test-source2.db'
            assert json_data['test-source2']['tables']['person_statement']['count'] == 4
            assert json_data['test-source2']['tables']['person_identifiers']['count'] == 8
            assert json_data['test-source2']['tables']['ooc_statement']['count'] == 16
            assert json_data['test-source2']['tables']['ooc_interests']['count'] == 14


class TestConsistencyPass:
    """Test data consistency checks success"""
    source = 'test-source3'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-1", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        bodsdata.check_data_consistency(self.source)


class TestConsistencyFail:
    """Test data consistency checks failure"""
    source = 'test-source4'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-2", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "14516809276187145413" in str(exception)


class TestConsistencyDuplicates:
    """Test data consistency checks with duplicates"""
    source = 'test-source5'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-3", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "11999711770514058937" in str(exception)


class TestConsistencyReference:
    """Test data consistency checks with referencing issue"""
    source = 'test-source6'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-4", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "12167039970933723582" in str(exception)


class TestConsistencyFields:
    """Test data consistency checks with missing required field"""
    source = 'test-source7'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-5", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "11999711770514058937" in str(exception)


class TestConsistencyIgnore:
    """Test data consistency checks ignoring specific number of errors"""
    source = 'test-source8'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-6", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        bodsdata.check_data_consistency(self.source, check_statement_refs=3)


class TestConsistencyIgnoreFail:
    """Test data consistency checks ignoring wrong specific number of errors"""
    source = 'test-source9'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-6", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source, check_statement_refs=2)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "12167039970933723582" in str(exception)


class TestConsistencyVersionPass:
    """Test data consistency checks with duplicates"""
    source = 'test-source10'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-7", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        bodsdata.check_data_consistency(self.source)


class TestConsistencyVersionFail:
    """Test data consistency checks with duplicates"""
    source = 'test-source11'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-7", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source, check_version="0.2")
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "Consistency checks failed: 26 missing" in str(exception)


class TestConsistencyIsComponentFail:
    """Test data consistency checks with duplicates"""
    source = 'test-source12'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-8", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        try:
            bodsdata.check_data_consistency(self.source)
            assert False, "Checks failed to detect issues"
        except AssertionError as exception:
            assert "Consistency checks failed: 10 missing" in str(exception)


class TestConsistencyIsComponentPass:
    """Test data consistency checks with duplicates"""
    source = 'test-source13'

    @pytest.fixture(scope="class")
    def temp_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, temp_dir):
        """Fixture to create subdirectory"""
        output_dir = Path(temp_dir) / self.source
        output_dir.mkdir()
        return output_dir

    @pytest.fixture(scope="class")
    def source_dir(self, temp_dir):
        """Fixture to create and populate source directory"""
        source_dir = Path(temp_dir) / f"{self.source}_download"
        copy_tree("tests/fixtures/checks-8", str(source_dir))
        return source_dir

    def test_check_data_consistency(self, temp_dir, output_dir, source_dir):
        """Test data consistency checks"""
        bodsdata.output_dir = temp_dir
        bodsdata.check_data_consistency(self.source, check_is_component=False)
