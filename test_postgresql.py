#!/usr/bin/env python3
"""
Test PostgreSQL Connection
This script tests the PostgreSQL connection before running the main scraper.
"""

import os
import psycopg2

def test_connection():
    """Test PostgreSQL connection and basic operations"""
    try:
        print("🔌 Testing PostgreSQL connection...")
        
        # Get database URL from environment or use default
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        print(f"   Using: {database_url[:50]}...")
        
        # Connect to database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        print("✅ Connected successfully!")
        
        # Test basic query
        cursor.execute("SELECT COUNT(*) FROM gas_prices")
        count = cursor.fetchone()[0]
        print(f"✅ Found {count} records in gas_prices table")
        
        # Test table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'gas_prices' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        print("✅ Table structure:")
        for col in columns:
            print(f"   - {col[0]}: {col[1]}")
        
        cursor.close()
        conn.close()
        print("✅ Connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()
