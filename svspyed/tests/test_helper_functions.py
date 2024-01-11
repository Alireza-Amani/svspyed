# ! # need to learn testing properly before 


# import unittest
# from unittest.mock import patch
# import tempfile

# from svspyed.utils.helper_functions import (
#     assert_file_exist,
#     assert_dir_exist,
#     print_dict_nice,
#     get_optimal_set,
#     remove_file_dir,
#     check_start_end_date_format,
#     check_spinup_end_date_format,
#     round_params,
#     get_layering_df,
#     populate_layering_df,
#     check_csv_columns,
# )

# class TestHelperFunctions(unittest.TestCase):
#     '''
#     This class contains unit tests for the helper_functions module.
#     '''

#     def test_assert_file_exist(self):
#         """Test the assert_file_exist function."""

#         # Test existing file
#         with tempfile.NamedTemporaryFile() as temp_file:
#             self.assertIsNone(assert_file_exist(temp_file.name))

#         # Test non-existing file
#         with self.assertRaises(AssertionError):
#             assert_file_exist("non_existent_file.txt")

#     def test_assert_dir_exist(self):
#         """Test the assert_dir_exist function."""

#         # Test existing directory
#         with tempfile.TemporaryDirectory() as temp_dir:
#             self.assertIsNone(assert_dir_exist(temp_dir))

#         # Test non-existing directory
#         with self.assertRaises(AssertionError):
#             assert_dir_exist("non_existent_directory")

#     @patch('builtins.print')
#     def test_print_dict_nice(self, mock_print):
#         """Test the print_dict_nice function."""

#         test_dict = {'key1': 'value1', 'key2': 'value2'}
#         print_dict_nice(test_dict)

#         # Validate print output
#         mock_print.assert_called_with(
#             "key1: value1\nkey2: value2\n"
#         )

#     @patch('pandas.DataFrame')
#     def test_get_optimal_set(self, MockDataFrame):
#         """Test the get_optimal_set function."""

#         mock_df = MockDataFrame()
#         mock_df.columns = ['param_alpha', 'param_beta']

#         mock_df.loc[:, 'param_alpha'].iloc[0] = 'alpha'
#         mock_df.loc[:, 'param_beta'].iloc[0] = 'beta'


#         # Validate returned dictionary
#         self.assertEqual(
#             get_optimal_set(mock_df),
#             {'alpha': 'alpha', 'beta': 'beta'}
#         )

#     def test_remove_file_dir(self):
#         """Test the remove_file_dir function."""
#         # Test file removal (omitted for brevity)
#         # Test directory removal (omitted for brevity)

#     def test_check_start_end_date_format(self):
#         """Test the check_start_end_date_format function."""
#         # Test valid date format (omitted for brevity)
#         # Test invalid date format (omitted for brevity)

#     def test_check_spinup_end_date_format(self):
#         """Test the check_spinup_end_date_format function."""
#         # Test valid date format (omitted for brevity)
#         # Test invalid date format (omitted for brevity)

#     def test_round_params(self):
#         """Test the round_params function."""
#         # Test rounding parameters (omitted for brevity)

#     def test_get_layering_df(self):
#         """Test the get_layering_df function."""
#         # Test DataFrame generation (omitted for brevity)

#     def test_populate_layering_df(self):
#         """Test the populate_layering_df function."""
#         # Test DataFrame population (omitted for brevity)

#     def test_check_csv_columns(self):
#         """Test the check_csv_columns function."""
#         # Test CSV column validation (omitted for brevity)


# if __name__ == '__main__':
#     unittest.main()
