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

class TestPipeline:
    """Test full pipeline"""
    source = 'test-pipeline'

    @pytest.fixture(scope="class")
    def test_dir(self, tmp_path_factory):
        """Fixture to create temporary directory"""
        return tmp_path_factory.getbasetemp()

    @pytest.fixture(scope="class")
    def output_dir(self, test_dir):
        """Fixture to create temporary directory"""
        output_dir = Path(test_dir) / self.source
        output_dir.mkdir()
        return output_dir

    def test_pipeline(self, test_dir, output_dir):
        """Test full pipeline without upload"""
        bodsdata.output_dir = test_dir
        title = "Test Pipeline"
        description = ""
        download = 'tests/fixtures/checks-1'
        upload = False
        bucket = "dummy"
        with patch('bodsdata.get_s3_bucket') as mock_get_s3_bucket:
            mock_bucket = Mock()
            mock_bucket.objects.all.return_value = [types.SimpleNamespace(key=str(d)) for d in Path(download).iterdir()]
            mock_bucket.download_file.side_effect = copy_file
            mock_get_s3_bucket.return_value = mock_bucket
            bodsdata.run_pipeline(self.source, title, description, download, upload, bucket)
