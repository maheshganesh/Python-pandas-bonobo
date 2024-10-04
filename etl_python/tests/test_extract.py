"""
Test Data extraction for csv, json format
"""
import pytest
import pandas as pd
import json
from unittest import mock
from unittest.mock import patch
from pandas.testing import assert_frame_equal

import etl_python.src.utils as utils

csv_df = pd.DataFrame({
    'id': [11, 22],
    'title': ['Four Rooms', 'Taxi Driver'],
    'genres': ['Comedy', 'Action']
})

test_json_data = [
    {
        'id': 21,
        'title': 'Four Rooms',
        'genres': {'name': 'Comedy'},
        'production_companies': {'name': 'Company 01'}
    },
    {
        'id': 22,
        'title': 'Avatar',
        'genres': {'name': 'Action'},
        'production_companies': {'name': 'Company 02'}
    }
]

@patch('pandas.read_csv')
def test_extract_csv(mock_read_csv):
    """ Test extraction call for csv file"""
    mock_read_csv.return_value = csv_df

    result = utils.extract_file('file_path.csv', file_format='csv')
    mock_read_csv.assert_called_once_with('file_path.csv')
    # Verify the returned DataFrame
    assert_frame_equal(result.reset_index(drop=True),
                       csv_df.reset_index(drop=True))

def test_extract_file_not_found():
    """ Test file not found exception """
    with pytest.raises(FileNotFoundError) as excinfo:
       utils.extract_file('invalid.json', file_format='json')
    assert 'Error' in str(excinfo.value)

def test_extract_file_format():
    """ Test incorrect file format """
    with pytest.raises(ValueError) as excinfo:
       utils.extract_file('invalid-file', file_format='bak')
    assert 'Error' in str(excinfo.value)


@patch('builtins.open', new_callable=mock.mock_open,
        read_data=json.dumps(test_json_data))
@patch('json.load')
def test_extract_json(mock_json_load, mock_open, monkeypatch):
    """Test extraction from json file"""
    mock_json_load.return_value = test_json_data
    monkeypatch.setattr(utils, "COL_LIST", ['id', 'title', 'genres', 'production_companies'])
    expected_df = pd.DataFrame({
        'id': [21, 22],
        'title': ['Four Rooms', 'Avatar'],
        'genres': ['Comedy', 'Action'],
        'production_companies': ['Company 01', 'Company 02']
    })

    result = utils.extract_file('test.json', 'json')
    # Verify the returned DataFrame
    mock_json_load.assert_called_once()
    assert_frame_equal(result.reset_index(drop=True),
                       expected_df.reset_index(drop=True))
