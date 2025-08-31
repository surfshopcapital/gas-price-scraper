import pandas as pd
import psycopg2
import os
from datetime import datetime
import sys

def import_cleaned_data():
    """
    Import cleaned CSV data into PostgreSQL database, replacing all existing records.
    """
    
    # Database connection parameters
    database_url = os.getenv('DATABASE_URL',
        'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
    
    # CSV file path
    csv_file = "GPT_Rewrite_DB_CLEANED.csv"
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Check if CSV file exists
        if not os.path.exists(csv_file):
            print(f"Error: CSV file '{csv_file}' not found!")
            return
        
        # Read the cleaned CSV data
        print(f"Reading cleaned CSV data from '{csv_file}'...")
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} records from CSV")
        
        # Display sample of data
        print("\nSample of data to be imported:")
        print(df.head())
        print(f"\nColumns: {list(df.columns)}")
        
        # Check for any missing values
        print("\nMissing values per column:")
        print(df.isnull().sum())
        
        # Confirm with user
        print(f"\nAbout to replace ALL existing data in gas_prices table with {len(df)} cleaned records.")
        response = input("Type 'YES' to proceed (this will DELETE all existing data): ")
        
        if response != 'YES':
            print("Operation cancelled.")
            return
        
        # Begin transaction
        print("\nStarting database update...")
        
        # Clear existing data
        print("Clearing existing gas_prices table...")
        cursor.execute("DELETE FROM gas_prices")
        deleted_count = cursor.rowcount
        print(f"Deleted {deleted_count} existing records")
        
        # Prepare data for insertion
        print("Preparing data for insertion...")
        
        # Handle timestamp conversion - convert 'Unknown' to NULL for database
        df['timestamp'] = df['timestamp'].replace('Unknown', None)
        
        # Convert scraped_at to proper datetime format
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        
        # Handle empty strings in numeric fields
        df['consensus'] = pd.to_numeric(df['consensus'], errors='coerce')
        df['surprise'] = pd.to_numeric(df['surprise'], errors='coerce')
        
        # Insert new data
        print("Inserting cleaned data...")
        
        # Prepare INSERT statement
        insert_query = """
        INSERT INTO gas_prices (
            source, fuel_type, price, timestamp, region, scraped_at, 
            consensus, surprise, as_of_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert DataFrame to list of tuples for insertion
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['source'],
                row['fuel_type'] if pd.notna(row['fuel_type']) else None,
                float(row['price']) if pd.notna(row['price']) else None,
                row['timestamp'],
                row['region'] if pd.notna(row['region']) else None,
                row['scraped_at'] if pd.notna(row['scraped_at']) else None,
                row['consensus'],
                row['surprise'],
                row['as_of_date'] if pd.notna(row['as_of_date']) else None
            ))
        
        # Execute batch insert
        cursor.executemany(insert_query, data_to_insert)
        
        # Commit transaction
        conn.commit()
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM gas_prices")
        new_count = cursor.fetchone()[0]
        
        print(f"\n✅ SUCCESS! Database updated successfully.")
        print(f"   - Deleted {deleted_count} old records")
        print(f"   - Inserted {len(data_to_insert)} new records")
        print(f"   - Total records in database: {new_count}")
        
        # Show sample of new data
        print("\nSample of newly inserted data:")
        cursor.execute("SELECT source, fuel_type, price, timestamp, scraped_at FROM gas_prices ORDER BY scraped_at DESC LIMIT 5")
        sample_data = cursor.fetchall()
        for row in sample_data:
            print(f"  {row}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    print("=" * 60)
    print("GAS PRICES DATABASE CLEAN DATA IMPORT")
    print("=" * 60)
    print("This script will:")
    print("1. Connect to your PostgreSQL database")
    print("2. Read the cleaned CSV file: GPT_Rewrite_DB_CLEANED.csv")
    print("3. DELETE ALL existing data in the gas_prices table")
    print("4. Import the cleaned data")
    print("=" * 60)
    
    import_cleaned_data()
