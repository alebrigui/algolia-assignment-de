from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from algolia.operators.local_csv_file_transformer import LocalCSVTransformerOperator

import warnings

# Suppress the specific DeprecationWarning in this module
warnings.filterwarnings(
    "ignore",
    "The dag_concurrency option in \[core\] has been renamed to max_active_tasks_per_dag.*",  # noqa
    DeprecationWarning,
)

DEFAULT_DATE = datetime(2021, 1, 1)


@pytest.fixture()
def input_filepath():
    return "./input.csv"


@pytest.fixture()
def output_filepath():
    return "./output.csv"


@pytest.fixture()
def processing_function():
    def sample_function(df):
        df.processed = True
        return df

    return sample_function


@patch("pandas.DataFrame.to_csv")
@patch("pandas.read_csv")
def test_csv_file_is_processed_correctly(
    mock_read_csv, mock_to_csv, input_filepath, output_filepath, processing_function
):
    """Tests that the LocalCSVTransformerOperator processes the CSV correctly."""

    # GIVEN
    mock_df = MagicMock()

    mock_read_csv.return_value = mock_df

    # WHEN
    operator = LocalCSVTransformerOperator(
        task_id="test",
        input_filepath=input_filepath,
        output_filepath=output_filepath,
        processing_function=processing_function,
    )
    operator.execute({})

    # THEN
    mock_read_csv.assert_called_once_with(input_filepath)
    assert (
        hasattr(mock_df, "processed") and mock_df.processed
    ), "DataFrame wasn't processed."
    mock_df.to_csv.assert_called_once_with(output_filepath, index=False)
