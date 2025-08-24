#!/usr/bin/env python3
"""
Database Setup Script for Gas Price Scraper
This script creates the necessary table structure in your Railway PostgreSQL database.
"""

import psycopg2
import sys

# Database connection details from Railway
DATABASE_URL = "postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway"

def create_table():
    """Create the gas_prices table and insert sample data"""
    try:
        print("🔌 Connecting to Railway PostgreSQL database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print("✅ Connected successfully!")
        
        # Create the gas_prices table
        print("📋 Creating gas_prices table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS gas_prices (
            id SERIAL PRIMARY KEY,
            source VARCHAR(100) NOT NULL,
            fuel_type VARCHAR(50) NOT NULL,
            price DECIMAL(10, 4),
            timestamp VARCHAR(200),
            region VARCHAR(100),
            consensus DECIMAL(10, 4),
            surprise DECIMAL(10, 4),
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        print("✅ Table created successfully!")
        
        # Create indexes for better query performance
        print("📊 Creating indexes...")
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_gas_prices_source ON gas_prices(source);",
            "CREATE INDEX IF NOT EXISTS idx_gas_prices_scraped_at ON gas_prices(scraped_at);",
            "CREATE INDEX IF NOT EXISTS idx_gas_prices_timestamp ON gas_prices(timestamp);"
        ]
        
        for index_sql in indexes_sql:
            cursor.execute(index_sql)
        print("✅ Indexes created successfully!")
        
        # Create unique constraint
        print("🔒 Creating unique constraint...")
        unique_sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_gas_prices_unique 
        ON gas_prices(source, fuel_type, timestamp, scraped_at);
        """
        cursor.execute(unique_sql)
        print("✅ Unique constraint created successfully!")
        
        # Insert sample data
        print("📝 Inserting sample data...")
        sample_data_sql = """
        INSERT INTO gas_prices (source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at) VALUES
        ('gasbuddy_fuel_insights', 'regular', 3.147, 'Current as of 2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
        ('aaa_gas_prices', 'regular', 3.152, 'Current as of 2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
        ('marketwatch_rbob_futures', 'futures', 2.456, '2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
        ('marketwatch_wti_futures', 'futures', 78.23, '2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
        ('tradingeconomics_gasoline_stocks', 'stocks', NULL, '2025-08-24', 'United States', -1.5, -0.8, CURRENT_TIMESTAMP),
        ('tradingeconomics_refinery_runs', 'refinery', NULL, '2025-08-24', 'United States', 0.2, 0.1, CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING;
        """
        cursor.execute(sample_data_sql)
        print("✅ Sample data inserted successfully!")
        
        # Verify the table was created
        print("🔍 Verifying table structure...")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'gas_prices' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("📋 Table structure:")
        for col in columns:
            print(f"   - {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        # Count records
        cursor.execute("SELECT COUNT(*) FROM gas_prices;")
        count = cursor.fetchone()[0]
        print(f"📊 Total records in table: {count}")
        
        # Commit changes
        conn.commit()
        print("💾 Changes committed successfully!")
        
        print("\n🎉 Database setup completed successfully!")
        print("Your dashboard should now work without database errors!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()
            print("🔌 Database connection closed.")

if __name__ == "__main__":
    print("🚀 Setting up Gas Price Scraper Database...")
    print("=" * 50)
    create_table()
    print("=" * 50)
    print("✨ Setup complete! You can now refresh your dashboard.")
