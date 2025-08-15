"""Tests for SQL runner module - TDD approach."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import duckdb
import pandas as pd

from src.analytics.sql_runner import SQLRunner, SQLRunnerError


class TestSQLRunner:
    """Test suite for SQL runner functionality."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock DuckDB connection."""
        return Mock(spec=duckdb.DuckDBPyConnection)

    @pytest.fixture
    def sql_runner(self, mock_connection):
        """Create SQL runner with mock connection."""
        runner = SQLRunner(connection=mock_connection)
        return runner

    def test_load_sql_file(self, sql_runner):
        """Test loading SQL from file."""
        sql_content = "SELECT * FROM bronze.nfl_players;"
        
        with patch("builtins.open", mock_open(read_data=sql_content)):
            result = sql_runner.load_sql_file("test.sql")
            
        assert result == sql_content

    def test_load_sql_file_not_found(self, sql_runner):
        """Test handling of missing SQL file."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(SQLRunnerError, match="SQL file not found"):
                sql_runner.load_sql_file("missing.sql")

    def test_execute_sql(self, sql_runner, mock_connection):
        """Test executing SQL query."""
        sql = "SELECT * FROM bronze.nfl_players LIMIT 5;"
        expected_df = pd.DataFrame({"player_id": [1, 2, 3, 4, 5]})
        
        mock_connection.execute.return_value.df.return_value = expected_df
        
        result = sql_runner.execute_sql(sql)
        
        mock_connection.execute.assert_called_once_with(sql)
        pd.testing.assert_frame_equal(result, expected_df)

    def test_execute_sql_with_params(self, sql_runner, mock_connection):
        """Test executing parameterized SQL query."""
        sql = "SELECT * FROM bronze.nfl_players WHERE position = ?;"
        params = ["RB"]
        expected_df = pd.DataFrame({"player_id": [1, 2], "position": ["RB", "RB"]})
        
        mock_connection.execute.return_value.df.return_value = expected_df
        
        result = sql_runner.execute_sql(sql, params=params)
        
        mock_connection.execute.assert_called_once_with(sql, params)
        pd.testing.assert_frame_equal(result, expected_df)

    def test_run_transformation(self, sql_runner, mock_connection):
        """Test running a transformation (CREATE VIEW)."""
        transformation_name = "player_consistency"
        sql_content = "CREATE OR REPLACE VIEW silver.player_consistency AS SELECT * FROM bronze.nfl_players;"
        
        with patch.object(sql_runner, 'load_sql_file', return_value=sql_content):
            sql_runner.run_transformation("silver", transformation_name)
            
        mock_connection.execute.assert_called_once_with(sql_content)

    def test_run_query(self, sql_runner, mock_connection):
        """Test running a query and returning results."""
        query_name = "top_performers"
        sql_content = "SELECT * FROM silver.player_consistency LIMIT 10;"
        expected_df = pd.DataFrame({"player_id": list(range(10))})
        
        mock_connection.execute.return_value.df.return_value = expected_df
        
        with patch.object(sql_runner, 'load_sql_file', return_value=sql_content):
            result = sql_runner.run_query("analytics", query_name)
            
        pd.testing.assert_frame_equal(result, expected_df)

    def test_get_available_transformations(self, sql_runner):
        """Test listing available transformation files."""
        mock_files = [
            Path("sql/transformations/silver/player_consistency.sql"),
            Path("sql/transformations/silver/weekly_stats.sql"),
        ]
        
        with patch("pathlib.Path.glob", return_value=mock_files):
            result = sql_runner.get_available_transformations("silver")
            
        assert result == ["player_consistency", "weekly_stats"]

    def test_validate_sql(self, sql_runner, mock_connection):
        """Test SQL validation without execution."""
        sql = "SELECT * FROM bronze.nfl_players;"
        
        # DuckDB doesn't have a direct validate, so we use EXPLAIN
        mock_connection.execute.return_value = Mock()
        
        is_valid = sql_runner.validate_sql(sql)
        
        mock_connection.execute.assert_called_once_with(f"EXPLAIN {sql}")
        assert is_valid is True

    def test_validate_sql_invalid(self, sql_runner, mock_connection):
        """Test invalid SQL detection."""
        sql = "INVALID SQL QUERY;"
        
        mock_connection.execute.side_effect = duckdb.ParserException("Invalid SQL")
        
        is_valid = sql_runner.validate_sql(sql)
        
        assert is_valid is False

    def test_execute_with_retry(self, sql_runner, mock_connection):
        """Test retry logic for transient failures."""
        sql = "SELECT * FROM bronze.nfl_players;"
        expected_df = pd.DataFrame({"player_id": [1, 2, 3]})
        
        # First call fails, second succeeds
        mock_connection.execute.side_effect = [
            duckdb.IOException("Database locked"),
            Mock(df=Mock(return_value=expected_df))
        ]
        
        result = sql_runner.execute_sql(sql, retry=True)
        
        assert mock_connection.execute.call_count == 2
        pd.testing.assert_frame_equal(result, expected_df)

    def test_format_results(self, sql_runner):
        """Test formatting query results for display."""
        df = pd.DataFrame({
            "name": ["Player A", "Player B"],
            "avg_points": [15.5, 12.3],
            "consistency_score": [85.2, 78.9]
        })
        
        # Test table format
        table_output = sql_runner.format_results(df, format="table")
        assert "Player A" in table_output
        assert "15.5" in table_output
        
        # Test JSON format
        json_output = sql_runner.format_results(df, format="json")
        assert isinstance(json_output, str)
        assert "Player A" in json_output
        
        # Test CSV format
        csv_output = sql_runner.format_results(df, format="csv")
        assert "name,avg_points,consistency_score" in csv_output