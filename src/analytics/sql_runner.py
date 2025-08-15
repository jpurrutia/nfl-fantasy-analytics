"""SQL Runner module for executing analytics queries and transformations."""

import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

import duckdb
import pandas as pd
from src.utils.league_config import LeagueConfig, ConfigLoader
from src.analytics.league_aware_queries import LeagueAwareQueryBuilder, QueryContext, query_template_manager


class SQLRunnerError(Exception):
    """Custom exception for SQL runner errors."""
    pass


class SQLRunner:
    """Manages SQL query execution and transformations with league awareness."""

    def __init__(
        self, 
        connection: Optional[duckdb.DuckDBPyConnection] = None, 
        db_path: str = "data/nfl_analytics.duckdb",
        league_config: Optional[LeagueConfig] = None,
        league_id: Optional[str] = None
    ):
        """
        Initialize SQL runner with database connection and league context.
        
        Args:
            connection: Optional existing DuckDB connection
            db_path: Path to DuckDB database file
            league_config: Optional league configuration
            league_id: League ID for auto-detection
        """
        self.db_path = db_path
        self.connection = connection or duckdb.connect(db_path)
        self.sql_dir = Path("sql")
        
        # League configuration setup
        self.config_loader = ConfigLoader()
        self.league_config = league_config or self.config_loader.get_league_config(league_id)
        self.query_builder = LeagueAwareQueryBuilder(self.league_config)

    def load_sql_file(self, file_path: str) -> str:
        """
        Load SQL content from file.
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            SQL query string
            
        Raises:
            SQLRunnerError: If file not found or cannot be read
        """
        try:
            with open(file_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            raise SQLRunnerError(f"SQL file not found: {file_path}")
        except Exception as e:
            raise SQLRunnerError(f"Error reading SQL file: {e}")

    def execute_sql(
        self, 
        sql: str, 
        params: Optional[List[Any]] = None,
        retry: bool = False,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query to execute
            params: Optional parameters for parameterized queries
            retry: Whether to retry on transient failures
            max_retries: Maximum number of retry attempts
            
        Returns:
            Query results as pandas DataFrame
            
        Raises:
            SQLRunnerError: If query execution fails
        """
        retries = 0
        last_error = None
        
        while retries <= (max_retries if retry else 0):
            try:
                if params:
                    result = self.connection.execute(sql, params)
                else:
                    result = self.connection.execute(sql)
                return result.df()
            except duckdb.IOException as e:
                # Transient error (e.g., database locked)
                last_error = e
                retries += 1
                if retries <= max_retries and retry:
                    time.sleep(0.5 * retries)  # Exponential backoff
                    continue
                raise SQLRunnerError(f"Database IO error: {e}")
            except Exception as e:
                raise SQLRunnerError(f"Query execution failed: {e}")
        
        raise SQLRunnerError(f"Query failed after {max_retries} retries: {last_error}")

    def run_transformation(self, layer: str, transformation_name: str, **kwargs) -> None:
        """
        Run a SQL transformation (CREATE VIEW/TABLE) with league awareness.
        
        Args:
            layer: Data layer (silver, gold)
            transformation_name: Name of transformation file (without .sql)
            **kwargs: Additional context parameters (season, min_games, etc.)
            
        Raises:
            SQLRunnerError: If transformation fails
        """
        try:
            # Check if this is a league-aware transformation
            if transformation_name == "player_consistency":
                # Use dynamic query generation
                context = QueryContext(
                    league=self.league_config,
                    season=kwargs.get('season'),
                    min_games=kwargs.get('min_games'),
                    position_filter=kwargs.get('position_filter')
                )
                
                # Check cache first
                cache_key = f"{transformation_name}_{context.league.league_id}_{context.season or 'all'}"
                cached_sql = query_template_manager.get_cached_query(
                    context.league.league_id, 
                    cache_key
                )
                
                if cached_sql:
                    sql = cached_sql
                else:
                    # Generate dynamic SQL
                    select_sql = self.query_builder.build_player_consistency_query(context)
                    sql = f"CREATE OR REPLACE VIEW {layer}.player_consistency AS\n{select_sql}"
                    
                    # Cache the generated SQL
                    query_template_manager.cache_query(
                        context.league.league_id,
                        cache_key,
                        sql
                    )
                
                print(f"🏈 Running league-aware transformation for {self.league_config.league_name}")
                print(f"📊 Positions: {', '.join(sorted(self.league_config.all_eligible_positions))}")
                if self.league_config.has_qb_flex:
                    print(f"⚡ QB-eligible flex detected: Enhanced QB analytics")
                
            else:
                # Use static SQL file
                file_path = self.sql_dir / "transformations" / layer / f"{transformation_name}.sql"
                sql = self.load_sql_file(str(file_path))
            
            # Execute the transformation
            self.connection.execute(sql)
            print(f"✅ Transformation '{transformation_name}' completed successfully")
            
        except Exception as e:
            raise SQLRunnerError(f"Transformation failed: {e}")

    def run_query(
        self, 
        category: str, 
        query_name: str,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Run a predefined query and return results.
        
        Args:
            category: Query category (e.g., 'analytics')
            query_name: Name of query file (without .sql)
            params: Optional parameters for query
            
        Returns:
            Query results as DataFrame
            
        Raises:
            SQLRunnerError: If query fails
        """
        file_path = self.sql_dir / "queries" / category / f"{query_name}.sql"
        
        try:
            sql = self.load_sql_file(str(file_path))
            
            # Simple parameter substitution for named parameters
            if params:
                for key, value in params.items():
                    sql = sql.replace(f"{{{key}}}", str(value))
            
            return self.execute_sql(sql)
        except Exception as e:
            raise SQLRunnerError(f"Query '{query_name}' failed: {e}")

    def get_available_transformations(self, layer: str) -> List[str]:
        """
        List available transformations for a given layer.
        
        Args:
            layer: Data layer (silver, gold)
            
        Returns:
            List of transformation names
        """
        path = self.sql_dir / "transformations" / layer
        if not path.exists():
            return []
        
        sql_files = path.glob("*.sql")
        return [f.stem for f in sql_files]

    def validate_sql(self, sql: str) -> bool:
        """
        Validate SQL syntax without executing.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Use EXPLAIN to validate without execution
            self.connection.execute(f"EXPLAIN {sql}")
            return True
        except (duckdb.ParserException, duckdb.BinderException):
            return False
        except Exception:
            return False

    def format_results(
        self, 
        df: pd.DataFrame, 
        format: str = "table",
        max_rows: int = 20
    ) -> str:
        """
        Format DataFrame results for display.
        
        Args:
            df: Results DataFrame
            format: Output format (table, json, csv)
            max_rows: Maximum rows to display
            
        Returns:
            Formatted string representation
        """
        if format == "json":
            return df.head(max_rows).to_json(orient="records", indent=2)
        elif format == "csv":
            return df.head(max_rows).to_csv(index=False)
        else:  # table
            return df.head(max_rows).to_string(index=False)

    def run_league_aware_query(
        self, 
        query_type: str, 
        position: Optional[str] = None,
        season: Optional[int] = None,
        min_games: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Run a league-aware analytics query.
        
        Args:
            query_type: Type of query (consistency, position_summary, league_context)
            position: Optional position filter
            season: Optional season filter
            min_games: Optional minimum games filter
            **kwargs: Additional query parameters
            
        Returns:
            Query results as DataFrame
        """
        # Validate position if provided
        if position and not self.query_builder.validate_position_choice(position):
            available = ', '.join(self.query_builder.get_eligible_positions_for_cli())
            raise SQLRunnerError(
                f"Position '{position}' not valid for this league. "
                f"Available positions: {available}"
            )
        
        # Create query context
        context = QueryContext(
            league=self.league_config,
            season=season,
            min_games=min_games,
            position_filter=position
        )
        
        # Generate appropriate query
        if query_type == "consistency":
            sql = self.query_builder.build_player_consistency_query(context)
        elif query_type == "position_summary":
            sql = self.query_builder.build_position_summary_query(context)
        elif query_type == "league_context":
            sql = self.query_builder.build_league_context_query()
        else:
            raise SQLRunnerError(f"Unknown query type: {query_type}")
        
        return self.execute_sql(sql)

    def get_available_positions(self) -> List[str]:
        """Get list of positions available in this league."""
        return self.query_builder.get_eligible_positions_for_cli()

    def get_league_info(self) -> Dict[str, Any]:
        """Get current league configuration info."""
        return {
            "league_id": self.league_config.league_id,
            "league_name": self.league_config.league_name,
            "scoring_type": self.league_config.scoring_type,
            "league_size": self.league_config.league_size,
            "positions": sorted(list(self.league_config.all_eligible_positions)),
            "has_qb_flex": self.league_config.has_qb_flex,
            "has_kickers": self.league_config.has_kickers,
            "has_defense": self.league_config.has_defense,
            "qb_slots": self.league_config.total_qb_slots,
            "detection_source": self.league_config.detection_source
        }

    def reload_league_config(self, league_id: Optional[str] = None, force_detection: bool = False) -> None:
        """
        Reload league configuration.
        
        Args:
            league_id: Optional new league ID
            force_detection: Force re-detection from ESPN
        """
        if force_detection:
            query_template_manager.clear_cache(league_id or self.league_config.league_id)
        
        self.league_config = self.config_loader.get_league_config(
            league_id or self.league_config.league_id, 
            force_detection=force_detection
        )
        self.query_builder = LeagueAwareQueryBuilder(self.league_config)
        
        print(f"🔄 Reloaded league configuration for {self.league_config.league_name}")

    def get_table_info(self, schema: str = "bronze") -> pd.DataFrame:
        """
        Get information about tables in a schema.
        
        Args:
            schema: Database schema name
            
        Returns:
            DataFrame with table information
        """
        sql = f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
        """
        return self.execute_sql(sql)

    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()