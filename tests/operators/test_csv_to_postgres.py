import os

from unittest.mock import patch, Mock
from algolia.operators.csv_to_postgres import CSVToPostgresOperator


def test_copy_data_to_postgres():
    # GIVEN
    mock_pg_hook = Mock()
    mock_conn = Mock()
    mock_cursor = Mock()

    mock_pg_hook.return_value.get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    csv_data = "col1,col2\n1,test1\n2,test2"
    with open("test.csv", "w") as f:
        f.write(csv_data)

    # WHEN
    op = CSVToPostgresOperator(
        task_id="test_csv_to_postgres", csv_filepath="test.csv", table_name="test_table"
    )

    with patch("algolia.operators.csv_to_postgres.PostgresHook", mock_pg_hook):
        op.execute(context={})

    # THEN
    mock_pg_hook.assert_called_once_with(postgres_conn_id="postgres_default")
    mock_pg_hook.return_value.get_conn.assert_called_once()
    mock_cursor.copy_expert.assert_called_once()

    # Cleanup
    os.remove("test.csv")
