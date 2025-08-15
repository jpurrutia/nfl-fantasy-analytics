#!/usr/bin/env python3
"""Hybrid data quality validation framework for NFL analytics database.

This tool combines automated Python validation with interactive SQL exploration.
- Use Python for automated checks and reporting
- Use generated SQL queries for manual data exploration
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Comprehensive data quality validation for NFL analytics database."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        """Initialize with database connection."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # Define validation thresholds
        self.thresholds = {
            'min_players': 1500,  # Minimum number of players expected
            'min_performance_records': 5000,  # Minimum performance records
            'max_null_percentage': 0.1,  # Max 10% null values for critical fields
            'min_weeks': 17,  # Minimum weeks of data for regular season
            'max_fantasy_points': 100,  # Reasonable upper bound for weekly fantasy points
            'min_mapping_rate': 0.8  # At least 80% of test players should be mappable
        }
    
    def __del__(self):
        """Close database connection on cleanup."""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def validate_players_table(self) -> Dict:
        """
        Validate the bronze.players table.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating players table...")
        results = {'table': 'bronze.players', 'checks': []}
        
        # Check 1: Total player count
        total_players = self.conn.execute("SELECT COUNT(*) FROM bronze.players").fetchone()[0]
        results['total_records'] = total_players
        
        check_result = {
            'check': 'Total Player Count',
            'value': total_players,
            'threshold': self.thresholds['min_players'],
            'status': 'PASS' if total_players >= self.thresholds['min_players'] else 'FAIL',
            'message': f"Found {total_players} players (expected ≥ {self.thresholds['min_players']})"
        }
        results['checks'].append(check_result)
        
        # Check 2: Required fields not null
        critical_fields = ['player_id', 'name', 'position', 'team']
        for field in critical_fields:
            null_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM bronze.players WHERE {field} IS NULL
            """).fetchone()[0]
            
            null_percentage = null_count / total_players if total_players > 0 else 1
            
            check_result = {
                'check': f'Null Values - {field}',
                'value': null_percentage,
                'threshold': self.thresholds['max_null_percentage'],
                'status': 'PASS' if null_percentage <= self.thresholds['max_null_percentage'] else 'FAIL',
                'message': f"{null_count} null values ({null_percentage:.1%})"
            }
            results['checks'].append(check_result)
        
        # Check 3: Position distribution
        positions = self.conn.execute("""
            SELECT position, COUNT(*) as count 
            FROM bronze.players 
            GROUP BY position 
            ORDER BY count DESC
        """).fetchall()
        
        # Check if we have major positions
        position_dict = {pos[0]: pos[1] for pos in positions}
        major_positions = ['QB', 'RB', 'WR', 'TE']
        
        for pos in major_positions:
            count = position_dict.get(pos, 0)
            min_expected = 50 if pos in ['QB', 'TE'] else 100  # Lower for QB/TE
            
            check_result = {
                'check': f'Position Count - {pos}',
                'value': count,
                'threshold': min_expected,
                'status': 'PASS' if count >= min_expected else 'FAIL',
                'message': f"Found {count} {pos} players (expected ≥ {min_expected})"
            }
            results['checks'].append(check_result)
        
        return results
    
    def validate_performance_table(self) -> Dict:
        """
        Validate the bronze.player_performance table.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating performance table...")
        results = {'table': 'bronze.player_performance', 'checks': []}
        
        # Check 1: Total record count
        total_records = self.conn.execute("SELECT COUNT(*) FROM bronze.player_performance").fetchone()[0]
        results['total_records'] = total_records
        
        check_result = {
            'check': 'Total Performance Records',
            'value': total_records,
            'threshold': self.thresholds['min_performance_records'],
            'status': 'PASS' if total_records >= self.thresholds['min_performance_records'] else 'FAIL',
            'message': f"Found {total_records} performance records"
        }
        results['checks'].append(check_result)
        
        # Check 2: Week coverage
        week_data = self.conn.execute("""
            SELECT season, MIN(week) as min_week, MAX(week) as max_week, COUNT(DISTINCT week) as week_count
            FROM bronze.player_performance
            GROUP BY season
            ORDER BY season
        """).fetchall()
        
        for season_data in week_data:
            season, min_week, max_week, week_count = season_data
            
            check_result = {
                'check': f'Week Coverage - {season}',
                'value': week_count,
                'threshold': self.thresholds['min_weeks'],
                'status': 'PASS' if week_count >= self.thresholds['min_weeks'] else 'FAIL',
                'message': f"Season {season}: Weeks {min_week}-{max_week} ({week_count} weeks)"
            }
            results['checks'].append(check_result)
        
        # Check 3: Fantasy points sanity check
        fantasy_outliers = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze.player_performance 
            WHERE fantasy_points_ppr > ?
        """, [self.thresholds['max_fantasy_points']]).fetchone()[0]
        
        outlier_percentage = fantasy_outliers / total_records if total_records > 0 else 0
        
        check_result = {
            'check': 'Fantasy Points Outliers',
            'value': outlier_percentage,
            'threshold': 0.01,  # Max 1% outliers
            'status': 'PASS' if outlier_percentage <= 0.01 else 'FAIL',
            'message': f"{fantasy_outliers} records with >100 fantasy points ({outlier_percentage:.1%})"
        }
        results['checks'].append(check_result)
        
        # Check 4: Player ID consistency
        orphaned_records = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze.player_performance p
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.players pl 
                WHERE pl.player_id = p.player_id
            )
        """).fetchone()[0]
        
        check_result = {
            'check': 'Player ID Consistency',
            'value': orphaned_records,
            'threshold': 0,
            'status': 'PASS' if orphaned_records == 0 else 'FAIL',
            'message': f"{orphaned_records} performance records without matching player"
        }
        results['checks'].append(check_result)
        
        return results
    
    def validate_mappings_table(self) -> Dict:
        """
        Validate the bronze.player_mapping table.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating player mappings...")
        results = {'table': 'bronze.player_mapping', 'checks': []}
        
        # Check 1: Total mappings
        total_mappings = self.conn.execute("SELECT COUNT(*) FROM bronze.player_mapping").fetchone()[0]
        results['total_records'] = total_mappings
        
        check_result = {
            'check': 'Total Mappings',
            'value': total_mappings,
            'threshold': 1,  # At least some mappings
            'status': 'PASS' if total_mappings >= 1 else 'FAIL',
            'message': f"Found {total_mappings} player mappings"
        }
        results['checks'].append(check_result)
        
        # Check 2: Platform distribution
        platform_counts = self.conn.execute("""
            SELECT platform, COUNT(*) as count
            FROM bronze.player_mapping
            GROUP BY platform
        """).fetchall()
        
        platform_dict = {p[0]: p[1] for p in platform_counts}
        
        check_result = {
            'check': 'ESPN Mappings',
            'value': platform_dict.get('ESPN', 0),
            'threshold': 1,
            'status': 'PASS' if platform_dict.get('ESPN', 0) >= 1 else 'FAIL',
            'message': f"Found {platform_dict.get('ESPN', 0)} ESPN player mappings"
        }
        results['checks'].append(check_result)
        
        # Check 3: Universal player ID validity
        invalid_mappings = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze.player_mapping pm
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.players p 
                WHERE p.player_id = pm.universal_player_id
            )
        """).fetchone()[0]
        
        check_result = {
            'check': 'Universal Player ID Validity',
            'value': invalid_mappings,
            'threshold': 0,
            'status': 'PASS' if invalid_mappings == 0 else 'FAIL',
            'message': f"{invalid_mappings} mappings with invalid universal player IDs"
        }
        results['checks'].append(check_result)
        
        return results
    
    def validate_data_freshness(self) -> Dict:
        """
        Validate data freshness and timeliness.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating data freshness...")
        results = {'table': 'data_freshness', 'checks': []}
        
        # Check current season/week
        current_date = datetime.now()
        current_year = current_date.year
        
        # NFL season typically starts in September
        if current_date.month >= 9:
            expected_season = current_year
        else:
            expected_season = current_year - 1
        
        latest_data = self.conn.execute("""
            SELECT MAX(season) as latest_season, MAX(week) as latest_week
            FROM bronze.player_performance
            WHERE season = (SELECT MAX(season) FROM bronze.player_performance)
        """).fetchone()
        
        if latest_data and latest_data[0]:
            latest_season, latest_week = latest_data
            
            check_result = {
                'check': 'Current Season Data',
                'value': latest_season,
                'threshold': expected_season,
                'status': 'PASS' if latest_season >= expected_season else 'FAIL',
                'message': f"Latest data: {latest_season} Week {latest_week} (expected ≥ {expected_season})"
            }
            results['checks'].append(check_result)
        
        return results
    
    def generate_quick_check_sql(self) -> str:
        """
        Generate a SQL query for quick interactive data quality check.
        
        Returns:
            SQL query string for manual execution
        """
        return """
-- Quick Data Quality Check - Run this anytime for a health overview
-- Generated by Python data_quality.py tool

WITH table_counts AS (
    SELECT 'nfl_players' as table_name, COUNT(*) as records FROM bronze.nfl_players
    UNION ALL
    SELECT 'nfl_performance', COUNT(*) FROM bronze.nfl_player_performance  
    UNION ALL
    SELECT 'player_mappings', COUNT(*) FROM bronze.player_id_mapping
),

latest_data AS (
    SELECT 
        MAX(season) as current_season,
        MAX(week) as current_week
    FROM bronze.nfl_player_performance
    WHERE season = (SELECT MAX(season) FROM bronze.nfl_player_performance)
),

data_quality_summary AS (
    SELECT 
        COUNT(DISTINCT p.player_id) as total_players,
        COUNT(DISTINCT CASE WHEN p.status IN ('ACT', 'RES') THEN p.player_id END) as active_players,
        COUNT(DISTINCT pp.player_id) as players_with_stats,
        COUNT(DISTINCT pm.universal_player_id) as mapped_players
    FROM bronze.nfl_players p
    LEFT JOIN bronze.nfl_player_performance pp ON p.player_id = pp.player_id
    LEFT JOIN bronze.player_id_mapping pm ON p.player_id = pm.universal_player_id
)

SELECT 
    '=== NFL ANALYTICS DATA QUALITY DASHBOARD ===' as section,
    NULL as metric, NULL as value, NULL as status
UNION ALL
SELECT 'TABLE SIZES', table_name, CAST(records as VARCHAR), '📊' FROM table_counts
UNION ALL  
SELECT 'CURRENT DATA', 'Season/Week', 
       CAST(current_season as VARCHAR) || ' Week ' || CAST(current_week as VARCHAR),
       '📅' FROM latest_data
UNION ALL
SELECT 'DATA COVERAGE', 'Total Players', CAST(total_players as VARCHAR), '👥' FROM data_quality_summary
UNION ALL  
SELECT '', 'Active Players', CAST(active_players as VARCHAR), '⚽' FROM data_quality_summary
UNION ALL
SELECT '', 'Players w/ Stats', CAST(players_with_stats as VARCHAR), '📈' FROM data_quality_summary
UNION ALL
SELECT '', 'ESPN Mapped', CAST(mapped_players as VARCHAR), '🔗' FROM data_quality_summary;
"""
    
    def run_automated_validation(self) -> Dict:
        """
        Run streamlined automated validation for CI/monitoring.
        
        Returns:
            Dictionary with essential validation results
        """
        logger.info("Running automated data quality validation...")
        
        # Critical checks only
        checks = []
        
        # Check 1: Data exists
        player_count = self.conn.execute("SELECT COUNT(*) FROM bronze.nfl_players").fetchone()[0]
        perf_count = self.conn.execute("SELECT COUNT(*) FROM bronze.nfl_player_performance").fetchone()[0]
        
        checks.append({
            'check': 'Data Loaded',
            'status': 'PASS' if player_count > 1000 and perf_count > 3000 else 'FAIL',
            'message': f"{player_count} players, {perf_count} performance records"
        })
        
        # Check 2: No orphaned records
        orphaned = self.conn.execute("""
            SELECT COUNT(*) FROM bronze.nfl_player_performance p
            WHERE NOT EXISTS (SELECT 1 FROM bronze.nfl_players pl WHERE pl.player_id = p.player_id)
        """).fetchone()[0]
        
        checks.append({
            'check': 'Data Integrity',
            'status': 'PASS' if orphaned == 0 else 'FAIL',
            'message': f"{orphaned} orphaned performance records"
        })
        
        # Check 3: Recent data
        latest_data = self.conn.execute("""
            SELECT MAX(season), MAX(week) FROM bronze.nfl_player_performance
            WHERE season = (SELECT MAX(season) FROM bronze.nfl_player_performance)
        """).fetchone()
        
        current_year = datetime.now().year
        expected_season = current_year if datetime.now().month >= 9 else current_year - 1
        
        checks.append({
            'check': 'Data Freshness',
            'status': 'PASS' if latest_data[0] >= expected_season else 'FAIL',
            'message': f"Latest: {latest_data[0]} Week {latest_data[1]}"
        })
        
        overall_status = 'PASS' if all(c['status'] == 'PASS' for c in checks) else 'FAIL'
        
        return {
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status,
            'checks': checks,
            'summary': f"{len([c for c in checks if c['status'] == 'PASS'])}/{len(checks)} checks passed"
        }
    
    def run_full_validation(self) -> Dict:
        """Legacy method - now delegates to automated validation."""
        return self.run_automated_validation()
    
    def print_validation_report(self, results: Dict):
        """
        Print a formatted validation report.
        
        Args:
            results: Validation results dictionary
        """
        print("\n" + "=" * 70)
        print("DATA QUALITY VALIDATION REPORT")
        print("=" * 70)
        print(f"Database: {results['database']}")
        print(f"Timestamp: {results['timestamp']}")
        print(f"Overall Status: {results['overall_status']}")
        
        summary = results['summary']
        print(f"Summary: {summary['passed_checks']}/{summary['total_checks']} checks passed ({summary['pass_rate']:.1%})")
        
        for table in results['tables']:
            if 'error' in table:
                print(f"\n❌ {table['table']}: ERROR - {table['error']}")
                continue
                
            table_name = table['table']
            total_records = table.get('total_records', 'N/A')
            
            print(f"\n📋 {table_name} ({total_records} records)")
            print("-" * 50)
            
            for check in table.get('checks', []):
                status_icon = "✅" if check['status'] == 'PASS' else "❌"
                print(f"{status_icon} {check['check']}: {check['message']}")
        
        print("\n" + "=" * 70)

def main():
    """Main execution function - provides both automated and interactive options."""
    
    validator = DataQualityValidator()
    
    print("=" * 60)
    print("NFL ANALYTICS DATA QUALITY TOOLKIT")
    print("=" * 60)
    print("Choose your validation approach:")
    print("1. Quick automated check (for CI/monitoring)")
    print("2. Generate SQL for interactive exploration")
    print("3. Both")
    print()
    
    choice = input("Enter choice (1-3) or press Enter for option 1: ").strip() or "1"
    
    if choice in ["1", "3"]:
        print("\n🔍 Running automated validation...")
        results = validator.run_automated_validation()
        
        print(f"\n📊 VALIDATION RESULTS - {results['overall_status']}")
        print("-" * 40)
        for check in results['checks']:
            status_icon = "✅" if check['status'] == 'PASS' else "❌"
            print(f"{status_icon} {check['check']}: {check['message']}")
        print(f"\nSummary: {results['summary']}")
    
    if choice in ["2", "3"]:
        print("\n📋 Generated SQL for interactive exploration:")
        print("-" * 40)
        sql_query = validator.generate_quick_check_sql()
        print(sql_query)
        print("\n💡 Copy this SQL and run in your DuckDB CLI or notebook!")
        print("💡 Also try the LeetCode-style challenges in sql/practice/data_quality_challenges.sql")
    
    print("\n" + "=" * 60)
    print("Data quality validation complete! 🎉")
    print("For detailed analysis, use the SQL files in sql/queries/data_quality/")
    print("=" * 60)

if __name__ == "__main__":
    main()