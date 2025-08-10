"""
Database initialization utility for NFL Analytics
"""

import duckdb
import os
import yaml
from pathlib import Path
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseInitializer:
    """Initialize and manage DuckDB database"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize database connection
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.db_path = self.config['database']['path']
        self.conn = None
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if not config_file.exists():
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return {
                'database': {
                    'path': 'data/nfl_analytics.duckdb',
                    'backup_path': 'data/backups/'
                }
            }
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Create database connection"""
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"Connected to database: {self.db_path}")
        return self.conn
    
    def init_schemas(self):
        """Initialize database schemas from SQL files"""
        if not self.conn:
            self.connect()
            
        sql_dir = Path("sql/schemas")
        if not sql_dir.exists():
            logger.error(f"SQL schemas directory not found: {sql_dir}")
            return
        
        # Execute schema files in order
        schema_files = sorted(sql_dir.glob("*.sql"))
        
        for sql_file in schema_files:
            logger.info(f"Executing: {sql_file.name}")
            
            with open(sql_file, 'r') as f:
                sql_content = f.read()
                
            try:
                # Execute each statement separately
                statements = [s.strip() for s in sql_content.split(';') if s.strip()]
                for statement in statements:
                    self.conn.execute(statement)
                logger.info(f"✓ Successfully executed: {sql_file.name}")
                
            except Exception as e:
                logger.error(f"✗ Error executing {sql_file.name}: {e}")
                raise
    
    def verify_setup(self):
        """Verify database setup is complete"""
        if not self.conn:
            self.connect()
            
        try:
            # Check schemas exist
            schemas = self.conn.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('bronze', 'silver', 'gold')
            """).fetchall()
            
            logger.info(f"Found schemas: {[s[0] for s in schemas]}")
            
            # Check tables exist
            tables = self.conn.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('bronze', 'silver', 'gold')
                ORDER BY table_schema, table_name
            """).fetchall()
            
            for schema, table in tables:
                logger.info(f"  - {schema}.{table}")
                
            return True
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Initialize the database"""
    print("\n🏈 NFL Analytics Database Initialization 🏈")
    print("=" * 50)
    
    # Initialize database
    db = DatabaseInitializer()
    
    try:
        # Connect and create schemas
        db.connect()
        db.init_schemas()
        
        # Verify setup
        print("\n✓ Database setup complete!")
        print("\nVerifying installation...")
        db.verify_setup()
        
        print("\n✅ Database ready for use!")
        print(f"📍 Location: {db.db_path}")
        
    except Exception as e:
        print(f"\n❌ Error during initialization: {e}")
        
    finally:
        db.close()


if __name__ == "__main__":
    main()