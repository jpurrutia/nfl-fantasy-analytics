#!/usr/bin/env python3
"""Database migration runner for NFL Analytics.

Simple, lightweight migration system for DuckDB schema changes.
Tracks applied migrations and provides rollback capability.
"""

import duckdb
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationRunner:
    """Manages database schema migrations for DuckDB."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb", migrations_dir: str = "migrations"):
        """Initialize migration runner.
        
        Args:
            db_path: Path to DuckDB database
            migrations_dir: Directory containing migration SQL files
        """
        self.db_path = db_path
        self.migrations_dir = Path(migrations_dir)
        self.conn = duckdb.connect(db_path)
        self._ensure_migrations_table()
    
    def __del__(self):
        """Close database connection."""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def _ensure_migrations_table(self):
        """Create migrations tracking table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                description VARCHAR,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                checksum VARCHAR
            )
        """)
        logger.info("Migration tracking table ready")
    
    def _get_migration_files(self) -> List[Path]:
        """Get all migration SQL files sorted by version number.
        
        Returns:
            List of migration file paths sorted by version
        """
        if not self.migrations_dir.exists():
            logger.warning(f"Migrations directory not found: {self.migrations_dir}")
            return []
        
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        return [f for f in migration_files if f.stem[0:3].isdigit()]
    
    def _parse_migration_file(self, filepath: Path) -> Tuple[int, str, str]:
        """Parse migration file to extract version, name, and content.
        
        Args:
            filepath: Path to migration SQL file
            
        Returns:
            Tuple of (version, name, content)
        """
        filename = filepath.stem
        version = int(filename[:3])
        name = filename[4:] if len(filename) > 3 else filename
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        return version, name, content
    
    def _calculate_checksum(self, content: str) -> str:
        """Calculate MD5 checksum of migration content.
        
        Args:
            content: SQL migration content
            
        Returns:
            MD5 hash string
        """
        return hashlib.md5(content.encode()).hexdigest()
    
    def _is_applied(self, version: int) -> bool:
        """Check if a migration version has been applied.
        
        Args:
            version: Migration version number
            
        Returns:
            True if migration has been applied
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
            [version]
        ).fetchone()
        return result[0] > 0
    
    def _apply_migration(self, filepath: Path) -> bool:
        """Apply a single migration file.
        
        Args:
            filepath: Path to migration SQL file
            
        Returns:
            True if successful, False otherwise
        """
        version, name, content = self._parse_migration_file(filepath)
        
        if self._is_applied(version):
            logger.info(f"Migration {version:03d}_{name} already applied, skipping")
            return True
        
        logger.info(f"Applying migration {version:03d}_{name}")
        
        try:
            # Start transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Execute migration SQL
            statements = [s.strip() for s in content.split(';') if s.strip()]
            for i, statement in enumerate(statements):
                # Skip comments and empty statements
                if statement and not statement.startswith('--') and not statement.startswith('/*'):
                    # Clean statement of inline comments
                    clean_statement = statement.split('--')[0].strip()
                    if clean_statement:
                        try:
                            logger.debug(f"Executing statement {i+1}: {clean_statement[:100]}...")
                            self.conn.execute(clean_statement)
                        except Exception as e:
                            logger.error(f"Failed on statement {i+1}: {clean_statement}")
                            raise
            
            # Record migration
            checksum = self._calculate_checksum(content)
            self.conn.execute("""
                INSERT INTO schema_migrations (version, name, description, checksum)
                VALUES (?, ?, ?, ?)
            """, [version, name, f"Migration from {filepath.name}", checksum])
            
            # Commit transaction
            self.conn.execute("COMMIT")
            logger.info(f"✅ Migration {version:03d}_{name} applied successfully")
            return True
            
        except Exception as e:
            # Rollback on error
            self.conn.execute("ROLLBACK")
            logger.error(f"❌ Migration {version:03d}_{name} failed: {e}")
            return False
    
    def run_migrations(self) -> int:
        """Run all pending migrations.
        
        Returns:
            Number of migrations applied
        """
        migration_files = self._get_migration_files()
        applied_count = 0
        
        logger.info(f"Found {len(migration_files)} migration files")
        
        for filepath in migration_files:
            version, name, _ = self._parse_migration_file(filepath)
            
            if not self._is_applied(version):
                if self._apply_migration(filepath):
                    applied_count += 1
                else:
                    logger.error(f"Migration failed, stopping at version {version}")
                    break
        
        if applied_count == 0:
            logger.info("No new migrations to apply")
        else:
            logger.info(f"Applied {applied_count} migration(s)")
        
        return applied_count
    
    def get_status(self) -> List[dict]:
        """Get status of all migrations.
        
        Returns:
            List of migration status dictionaries
        """
        # Get applied migrations
        applied = self.conn.execute("""
            SELECT version, name, applied_at
            FROM schema_migrations
            ORDER BY version
        """).fetchall()
        
        applied_versions = {row[0]: row for row in applied}
        
        # Get all migration files
        migration_files = self._get_migration_files()
        
        status = []
        for filepath in migration_files:
            version, name, _ = self._parse_migration_file(filepath)
            
            if version in applied_versions:
                applied_at = applied_versions[version][2]
                status.append({
                    'version': version,
                    'name': name,
                    'status': 'applied',
                    'applied_at': applied_at
                })
            else:
                status.append({
                    'version': version,
                    'name': name,
                    'status': 'pending',
                    'applied_at': None
                })
        
        return status
    
    def rollback_last(self) -> bool:
        """Rollback the last applied migration.
        
        Note: This requires a corresponding down migration file.
        
        Returns:
            True if successful
        """
        # Get last applied migration
        last_migration = self.conn.execute("""
            SELECT version, name 
            FROM schema_migrations 
            ORDER BY version DESC 
            LIMIT 1
        """).fetchone()
        
        if not last_migration:
            logger.warning("No migrations to rollback")
            return False
        
        version, name = last_migration
        
        # Look for down migration file
        down_file = self.migrations_dir / f"{version:03d}_{name}_down.sql"
        
        if not down_file.exists():
            logger.error(f"No rollback file found: {down_file}")
            logger.info("Manual rollback may be required")
            return False
        
        logger.info(f"Rolling back migration {version:03d}_{name}")
        
        try:
            # Execute rollback
            with open(down_file, 'r') as f:
                rollback_sql = f.read()
            
            self.conn.execute("BEGIN TRANSACTION")
            
            for statement in rollback_sql.split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    self.conn.execute(statement)
            
            # Remove migration record
            self.conn.execute("DELETE FROM schema_migrations WHERE version = ?", [version])
            
            self.conn.execute("COMMIT")
            logger.info(f"✅ Migration {version:03d}_{name} rolled back successfully")
            return True
            
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"❌ Rollback failed: {e}")
            return False

def main():
    """CLI interface for migration runner."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database migration tool')
    parser.add_argument('--status', action='store_true', help='Show migration status')
    parser.add_argument('--rollback', action='store_true', help='Rollback last migration')
    parser.add_argument('--apply', action='store_true', help='Apply pending migrations')
    
    args = parser.parse_args()
    
    runner = MigrationRunner()
    
    if args.status:
        status = runner.get_status()
        print("\nMigration Status:")
        print("-" * 60)
        for migration in status:
            status_icon = "✅" if migration['status'] == 'applied' else "⏳"
            print(f"{status_icon} {migration['version']:03d}_{migration['name']}: {migration['status']}")
            if migration['applied_at']:
                print(f"    Applied at: {migration['applied_at']}")
    
    elif args.rollback:
        if runner.rollback_last():
            print("Rollback successful")
        else:
            print("Rollback failed")
    
    else:  # Default: apply migrations
        count = runner.run_migrations()
        print(f"Migrations complete: {count} applied")

if __name__ == "__main__":
    main()