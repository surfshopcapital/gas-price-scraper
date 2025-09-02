#!/usr/bin/env python3
"""
PostgreSQL Connection Test
Simple utility to test database connectivity for the gas scraper.
"""

import os
import psycopg2
from datetime import datetime

def test_connection():
    """
    Test PostgreSQL database connection.
    Returns True if connection is successful, False otherwise.
    """
    try:
        # Get database URL from environment or use default
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway"
        )
        
        print(f"   🔌 Testing connection to database...")
        
        # Attempt to connect
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # Test basic query
        cur.execute("SELECT 1")
        result = cur.fetchone()
        
        # Test gas_prices table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'gas_prices'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if result and table_exists:
            print(f"   ✅ Database connection successful @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return True
        else:
            print(f"   ❌ Database connection failed - table 'gas_prices' not found")
            return False
            
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # Allow running this script directly for testing
    print("🧪 PostgreSQL Connection Test")
    print("=" * 40)
    
    if test_connection():
        print("✅ Connection test passed!")
        exit(0)
    else:
        print("❌ Connection test failed!")
        exit(1)
