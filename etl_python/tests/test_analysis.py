import pytest
from unittest.mock import MagicMock

from etl_python.src.utils import high_rated_director


@pytest.fixture
def mock_db_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.get_pool_connection.return_value.__enter__.return_value.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_high_rated_director(mock_db_connection):
    """Test Valid data return from query """
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchall.return_value = [
        (1, 'Christopher Nolan', 'Inception', 8.9),
        (2, 'Quentin Tarantino', 'Pulp Fiction', 8.8),
        (3, 'Peter Jackson', 'The Lord of the Rings: The Return of the King', 8.6),
        (4, 'David Fincher', 'Fight Club', 8.0),
        (5, 'Ridley Scott', 'Gladiator', 7.5)
    ]
    m_results = high_rated_director('movies', mock_conn)

    # assertions
    mock_cursor.close.assert_called_once()
    assert len(m_results) == 5
    assert m_results[0]['rating'] > m_results[1]['rating']


def test_high_rated_director_db_error(mock_db_connection):
    """ Test Db error on fetch from database"""
    mock_conn, mock_cursor = mock_db_connection
    # simulate exception with database query
    mock_cursor.execute.side_effect = Exception("Database Error")

    with pytest.raises(Exception, match="Database Error"):
        high_rated_director('movies', mock_conn)

def test_high_rated_no_data(mock_db_connection):
    """Test No data returned from database"""
    mock_conn, mock_cursor = mock_db_connection
    # Empty data from cursor
    mock_cursor.fetchall.return_value = []

    result = high_rated_director('movies', mock_conn)

    # Check if the result is empty
    assert result == []
