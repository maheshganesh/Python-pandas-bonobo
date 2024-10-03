import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from etl_python.src.utils import clean_movie_data, transform_credits

class TestMovieTransform(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'title': ['Movie A', 'Movie B', 'Movie B', 'Movie C'],
            'tagline': ['TagA', 'TagB', 'TagB', 'TagC'],
            'release_date': ['2020-01-01', '2024-02-01', '2024-05-01', '2024-03-01'],
            'genres': ['Action', 'Comedy', 'Comedy', 'Drama'],
            'production_companies': ['Company A', 'Company B', 'Company B', 'Company C'],
            'vote_count': [100, 150, 150, 200],
            'vote_average': [7.5, 8.2, 8.2, 2.0],
        })

        # Expected output
        self.expected_cleaned_data = pd.DataFrame({
            'id': [1, 2, 3],
            'title': ['Movie A', 'Movie B', 'Movie C'],
            'tagline': ['TagA', 'TagB', 'TagC'],
            'release_date': pd.to_datetime(['2020-01-01', '2024-02-01', '2024-03-01']),
            'genres': ['Action', 'Comedy', 'Drama'],
            'production_companies': ['Company A', 'Company B', 'Company C'],
            'vote_count': [100, 150, 200],
            'vote_average': [7.5, 8.2, 2.0]
        })

    def test_remove_duplicates(self):
        """Test for drop_duplicates"""
        movies_df = clean_movie_data(self.df)
        # Check for uniques on "id" column
        self.assertEqual(movies_df['id'].nunique(), len(movies_df))
        self.assertEqual(len(movies_df), 3)

    def test_required_columns(self):
        """Tests for required columns on dataframe """
        movies_df = clean_movie_data(self.df)
        # Check that only required columns are present
        expected_columns = ['id', 'title', 'tagline', 'release_date', 'genres',
                            'production_companies', 'vote_count', 'vote_average']
        self.assertListEqual(
            list(movies_df.columns),
            expected_columns
        )

    def test_data_correctness(self):
        """Test cleaned data matches the expected output"""
        cleaned_df = clean_movie_data(self.df)
        # Assert that the cleaned dataframe matches the expected dataframe
        assert_frame_equal(cleaned_df.reset_index(drop=True),
                           self.expected_cleaned_data.reset_index(drop=True),
                           check_datetimelike_compat=True
                           )

class TestTransformCredit(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'crew': [
                "[{'name': 'John', 'job': 'Director', 'id': 45}, {'name': 'Jane', 'job': 'Writer', 'id': 105}]",
                "[{'name': 'Armstrong', 'job': 'Director', 'id': 12}]",
                "[{'name': 'Armstrong', 'job': 'Director', 'id': 12}]",
                "[{'name': 'Keith', 'job': 'Director', 'id': 67}]",
            ],
            'cast': ["Cast01", "Cast02", "Cast02", "Cast03"]
        })

    def test_remove_missing_crew_size(self):
        """Test if rows with missing crew sizes are removed """
        credits_df = transform_credits(self.df)
        # Row with id=12 should be de-duplicated
        self.assertEqual(len(credits_df), 3)

    def test_director_value(self):
        """ Test if the director field is return correctly """
        transformed_df = transform_credits(self.df)
        self.assertEqual(transformed_df.iloc[0]['director'], 'John')
        self.assertEqual(transformed_df.iloc[2]['director'], 'Keith')

    def test_columns_list(self):
        """ Test for 'crew' and 'crew_size' columns """
        transformed_df = transform_credits(self.df)
        self.assertNotIn('crew', transformed_df.columns)
        self.assertIn('crew_size', transformed_df.columns)

    def test_crew_size_calculation(self):
       """ Test if crew size is calculated correctly """
       transformed_df = transform_credits(self.df)
       self.assertEqual(transformed_df.iloc[0]['crew_size'], 2)
       self.assertEqual(transformed_df.iloc[1]['crew_size'], 1)

if __name__ == '__main__':
   unittest.main()
