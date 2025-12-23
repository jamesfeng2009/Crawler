#!/usr/bin/env python3
"""
Initialize SQLite database for memory price monitor.
Run this script to create the database and tables.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.utils.logging import setup_logging


def main():
    """Initialize the SQLite database."""
    # Setup logging
    setup_logging(
        log_level="INFO",
        log_file="logs/database_init.log",
        retention_days=7  # 7天日志保留
    )
    
    print("Initializing SQLite database for Memory Price Monitor...")
    
    # Create database manager
    db_manager = SQLiteDatabaseManager("data/memory_price_monitor.db")
    
    try:
        # Initialize database and create tables
        db_manager.initialize()
        
        # Verify database health
        if db_manager.health_check():
            print("✅ Database initialized successfully!")
            
            # Show database stats
            stats = db_manager.get_connection_stats()
            print(f"📊 Database location: {stats['database_path']}")
            print(f"📊 Database size: {stats['database_size_bytes']} bytes")
            print(f"📊 Products table: {stats['products_count']} records")
            print(f"📊 Price records table: {stats['price_records_count']} records")
            print(f"📊 Crawl logs table: {stats['crawl_logs_count']} records")
        else:
            print("❌ Database health check failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()