"""
database.py - MySQL connection and table setup

IMPORTANT: This file serves TWO purposes:

1. ONE-TIME SETUP (run as script):
   Run this ONCE to create database tables and partitions:
   $ python3 database.py
   
   After tables are created, you never need to run this again.

2. RUNTIME MODULE (imported by other scripts):
   This file provides the DatabaseManager class and db_manager instance
   used by loader.py and pipeline_jobs.py for database connections.
   DO NOT DELETE THIS FILE - it's needed for the pipeline to work.

Summary:
- Run ONCE: python3 database.py (creates tables)
- Keep FOREVER: Required by loader.py for database connections
"""
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import logging
from src.database.sources import get_tables, DATA_TABLE, METADATA_TABLE

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT', 3306)
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("Missing database configuration in .env file")
    
    def get_connection(self):
        """Create and return a MySQL connection"""
        try:
            logger.info(f"Connecting to MySQL at {self.host}:{self.port}...")
            connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connection_timeout=10
            )
            
            if connection.is_connected():
                logger.info(f"✓ Connected to MySQL database '{self.database}' at {self.host}")
                return connection
                
        except Error as e:
            logger.error(f"✗ Error connecting to MySQL at {self.host}: {e}")
            raise
    
    def setup_database(self):
        """Create database and tables if they don't exist"""
        try:
            logger.info(f"Setting up database on remote server: {self.host}")
            # First connect without database to create it
            admin_conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            admin_cursor = admin_conn.cursor()
            
            # Create database if not exists
            admin_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"✓ Database '{self.database}' is ready on {self.host}")
            
            admin_cursor.close()
            admin_conn.close()
            
            # Now connect to the database
            conn = self.get_connection()
            cursor = conn.cursor()
            
            data_table, meta_table = get_tables()
            self._create_ticker_tables(cursor, data_table, meta_table)

            conn.commit()
            cursor.close()
            conn.close()
            
            # Add yearly partitions for better performance
            logger.info("Adding yearly partitions...")
            self.add_partitions()
            
            logger.info("✓ Database setup completed successfully on remote server")
            
        except Error as e:
            logger.error(f"✗ Error setting up database on {self.host}: {e}")
            raise

    def _create_ticker_tables(self, cursor, data_table: str, meta_table: str):
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {data_table} (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                
                date DATE NOT NULL,
                
                open DECIMAL(50,25),
                high DECIMAL(50,25),
                low DECIMAL(50,25),
                close DECIMAL(50,25),
                
                volume BIGINT,
                vwap DECIMAL(50,25),
                
                change_value DECIMAL(50,25),
                changePercent DECIMAL(50,25),
                
                unadjustedVolume BIGINT NULL,
                
                adjOpen DOUBLE,
                adjHigh DOUBLE,
                adjLow DOUBLE,
                adjClose DOUBLE,
                adjVolume DOUBLE,
                
                symbol VARCHAR(10) NOT NULL,
                
                UNIQUE KEY uniq_symbol_date (symbol, date),
                INDEX idx_symbol_date (symbol, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        
        cursor.execute(create_table_sql)
        logger.info("Table '%s' created successfully", data_table)
        
        metadata_sql = f"""
            CREATE TABLE IF NOT EXISTS {meta_table} (
                symbol VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100),
                first_date DATE,
                last_date DATE,
                total_rows INT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
        
        cursor.execute(metadata_sql)
        logger.info("Table '%s' created successfully", meta_table)
    
    def add_partitions(self):
        """Add yearly partitions to the data table for better performance"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            self._add_partitions_for_table(cursor, DATA_TABLE)
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Error as e:
            logger.error(f"Error adding partitions: {e}")
            # Don't raise error, partitions are optional for functionality

    def _add_partitions_for_table(self, cursor, table_name: str):
        cursor.execute("""
            SELECT PARTITION_NAME 
            FROM INFORMATION_SCHEMA.PARTITIONS 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = %s
            AND PARTITION_NAME IS NOT NULL
        """, (self.database, table_name))
        
        existing_partitions = cursor.fetchall()
        if existing_partitions:
            logger.info("Table '%s' is already partitioned", table_name)
            return
        
        partition_sql = f"""
            ALTER TABLE {table_name}
            PARTITION BY RANGE (YEAR(date)) (
                PARTITION p1990 VALUES LESS THAN (1991),
                PARTITION p1991 VALUES LESS THAN (1992),
                PARTITION p1992 VALUES LESS THAN (1993),
                PARTITION p1993 VALUES LESS THAN (1994),
                PARTITION p1994 VALUES LESS THAN (1995),
                PARTITION p1995 VALUES LESS THAN (1996),
                PARTITION p1996 VALUES LESS THAN (1997),
                PARTITION p1997 VALUES LESS THAN (1998),
                PARTITION p1998 VALUES LESS THAN (1999),
                PARTITION p1999 VALUES LESS THAN (2000),
                PARTITION p2000 VALUES LESS THAN (2001),
                PARTITION p2001 VALUES LESS THAN (2002),
                PARTITION p2002 VALUES LESS THAN (2003),
                PARTITION p2003 VALUES LESS THAN (2004),
                PARTITION p2004 VALUES LESS THAN (2005),
                PARTITION p2005 VALUES LESS THAN (2006),
                PARTITION p2006 VALUES LESS THAN (2007),
                PARTITION p2007 VALUES LESS THAN (2008),
                PARTITION p2008 VALUES LESS THAN (2009),
                PARTITION p2009 VALUES LESS THAN (2010),
                PARTITION p2010 VALUES LESS THAN (2011),
                PARTITION p2011 VALUES LESS THAN (2012),
                PARTITION p2012 VALUES LESS THAN (2013),
                PARTITION p2013 VALUES LESS THAN (2014),
                PARTITION p2014 VALUES LESS THAN (2015),
                PARTITION p2015 VALUES LESS THAN (2016),
                PARTITION p2016 VALUES LESS THAN (2017),
                PARTITION p2017 VALUES LESS THAN (2018),
                PARTITION p2018 VALUES LESS THAN (2019),
                PARTITION p2019 VALUES LESS THAN (2020),
                PARTITION p2020 VALUES LESS THAN (2021),
                PARTITION p2021 VALUES LESS THAN (2022),
                PARTITION p2022 VALUES LESS THAN (2023),
                PARTITION p2023 VALUES LESS THAN (2024),
                PARTITION p2024 VALUES LESS THAN (2025),
                PARTITION p2025 VALUES LESS THAN (2026),
                PARTITION p2026 VALUES LESS THAN (2027),
                PARTITION p_future VALUES LESS THAN MAXVALUE
            )
        """
        
        cursor.execute(partition_sql)
        logger.info("Yearly partitions added to %s", table_name)
    
    def get_ticker_list(self):
        """Get list of all tickers in database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT DISTINCT symbol FROM {DATA_TABLE} ORDER BY symbol")
            tickers = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return tickers
            
        except Error as e:
            logger.error(f"Error getting ticker list: {e}")
            return []
    
    def get_ticker_stats(self):
        """Get statistics about the database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT ticker) as total_tickers,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM {DATA_TABLE}
            """)
            
            stats = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Error as e:
            logger.error(f"Error getting stats: {e}")
            return {}

# Create a global instance
db_manager = DatabaseManager()

if __name__ == "__main__":
    print("="*70)
    print("DATABASE INITIAL SETUP - Run this ONCE to create tables")
    print("="*70)
    print("\nThis script will:")
    print("  1. Create database 'fmp_api' if it doesn't exist")
    print("  2. Create table 'ticker_data' with DECIMAL(50,25) precision")
    print("  3. Create table 'ticker_metadata'")
    print("  4. Add yearly partitions from 1990-2026")
    print("\nYou only need to run this ONCE for initial setup.")
    print("After tables exist, the pipeline will use them automatically.")
    print("="*70)
    print(f"\nConnecting to: {db_manager.host}:{db_manager.port}")
    print(f"Database: {db_manager.database}")
    print(f"User: {db_manager.user}")
    print()
    
    try:
        db_manager.setup_database()
        
        print("\n" + "="*70)
        print("✓ Database setup complete!")
        print("="*70)
        print(f"Host: {db_manager.host}")
        print(f"Database: {db_manager.database}")
        print("\n⚠️  You do NOT need to run this script again.")
        print("\nNext steps:")
        print("  1. Place your CSV files in the 'raw/' directory")
        print("  2. Run: ./run_pipeline.sh")
        print("     (or manually: python3 pipeline_worker.py & python3 pipeline_watch.py)")
        print("\nTo verify tables were created:")
        print(f"  mysql -h {db_manager.host} -u {db_manager.user} -p {db_manager.database} -e 'SHOW TABLES;'")
        print("="*70)
    except Exception as e:
        print("\n" + "="*70)
        print("✗ Database setup failed!")
        print("="*70)
        print(f"Error: {e}")
        print("\nPlease check:")
        print("  1. Network connectivity to ai-api.umiuni.com")
        print("  2. MySQL credentials in .env file")
        print("  3. Firewall allows port 3306")
        print("  4. User has CREATE DATABASE and CREATE TABLE privileges")
        print("="*70)
        raise