#!/usr/bin/env python3
"""
Sample Data Import Script
Imports the Sample_Historical_as_Scrape.csv file into the PostgreSQL database
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
import sys

def import_sample_data():
    """Import sample CSV data into the PostgreSQL database"""
    
    # CSV file path
    csv_path = r"C:\Users\betti\Downloads\Sample_Historical_as_Scrape.csv"
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found at {csv_path}")
        return False
    
    try:
        # Read CSV file
        print(f"📖 Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        print(f"✅ Loaded {len(df)} records from CSV")
        print(f"📊 Columns: {list(df.columns)}")
        print(f"📅 Date range: {df['scraped_at'].min()} to {df['scraped_at'].max()}")
        print(f"🔍 Sources: {df['source'].unique()}")
        
        # Data validation
        required_columns = ['source', 'price', 'timestamp', 'region', 'fuel_type', 'consensus', 'surprise', 'scraped_at']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Error: Missing required columns: {missing_columns}")
            return False
        
        # Convert scraped_at to datetime
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = df['scraped_at'].isna().sum()
        if invalid_dates > 0:
            print(f"⚠️ Warning: {invalid_dates} records have invalid dates")
            df = df.dropna(subset=['scraped_at'])
        
        # Convert price to numeric
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Check for invalid prices
        invalid_prices = df['price'].isna().sum()
        if invalid_prices > 0:
            print(f"⚠️ Warning: {invalid_prices} records have invalid prices")
            df = df.dropna(subset=['price'])
        
        print(f"✅ After validation: {len(df)} valid records")
        
        # Connect to database
        print("🔌 Connecting to database...")
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'gas_prices'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("❌ Error: gas_prices table does not exist")
            print("Please run setup_database.py first to create the table")
            return False
        
        # Check current record count
        cursor.execute("SELECT COUNT(*) FROM gas_prices")
        current_count = cursor.fetchone()[0]
        print(f"📊 Current database records: {current_count}")
        
        # Check for duplicate records (based on source, scraped_at, and price)
        print("🔍 Checking for duplicate records...")
        
        # Create a temporary table for deduplication
        temp_table_name = f"temp_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Insert into temporary table
        cursor.execute(f"""
            CREATE TEMP TABLE {temp_table_name} (
                source VARCHAR(100),
                price DECIMAL(10,6),
                timestamp VARCHAR(200),
                region VARCHAR(50),
                fuel_type VARCHAR(50),
                consensus DECIMAL(10,6),
                surprise DECIMAL(10,6),
                scraped_at TIMESTAMP
            )
        """)
        
        # Prepare data for insertion
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['source'],
                float(row['price']),
                str(row['timestamp']),
                str(row['region']),
                str(row['fuel_type']),
                float(row['consensus']) if pd.notna(row['consensus']) else None,
                float(row['surprise']) if pd.notna(row['surprise']) else None,
                row['scraped_at']
            ))
        
        # Insert into temporary table
        cursor.executemany(f"""
            INSERT INTO {temp_table_name} 
            (source, price, timestamp, region, fuel_type, consensus, surprise, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, data_to_insert)
        
        print(f"✅ Inserted {len(data_to_insert)} records into temporary table")
        
        # Insert into main table, avoiding duplicates
        cursor.execute(f"""
            INSERT INTO gas_prices (source, price, timestamp, region, fuel_type, consensus, surprise, scraped_at)
            SELECT t.source, t.price, t.timestamp, t.region, t.fuel_type, t.consensus, t.surprise, t.scraped_at
            FROM {temp_table_name} t
            WHERE NOT EXISTS (
                SELECT 1 FROM gas_prices p 
                WHERE p.source = t.source 
                AND p.scraped_at = t.scraped_at 
                AND ABS(p.price - t.price) < 0.001
            )
        """)
        
        new_records = cursor.rowcount
        print(f"✅ Inserted {new_records} new records into gas_prices table")
        
        # Check final record count
        cursor.execute("SELECT COUNT(*) FROM gas_prices")
        final_count = cursor.fetchone()[0]
        print(f"📊 Final database records: {final_count}")
        print(f"📈 Added {new_records} new records")
        
        # Show summary by source
        cursor.execute("SELECT source, COUNT(*) FROM gas_prices GROUP BY source ORDER BY COUNT(*) DESC")
        source_counts = cursor.fetchall()
        
        print("\n📊 Records by source:")
        for source, count in source_counts:
            print(f"   {source}: {count}")
        
        # Commit changes
        conn.commit()
        print("✅ Database changes committed successfully")
        
        # Clean up
        cursor.close()
        conn.close()
        
        print("\n🎉 Sample data import completed successfully!")
        print(f"📁 CSV file: {csv_path}")
        print(f"📊 Total records imported: {new_records}")
        print(f"📊 Total database records: {final_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error importing data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting sample data import...")
    print("=" * 50)
    
    success = import_sample_data()
    
    if success:
        print("\n✅ Import completed successfully!")
        print("You can now run the dashboard to test the models with your sample data.")
    else:
        print("\n❌ Import failed. Please check the error messages above.")
        sys.exit(1)
