import os
import pandas as pd
import psycopg2
from datetime import datetime

def export_database_to_csv():
    """Export all data from the gas_prices table to a CSV file"""
    
    # Database connection
    database_url = os.getenv('DATABASE_URL',
        'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
    
    try:
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # First, check what columns exist in the table
        print("🔍 Checking table structure...")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'gas_prices'
            ORDER BY ordinal_position
        """)
        
        columns_info = cur.fetchall()
        print("📋 Available columns:")
        for col_name, col_type in columns_info:
            print(f"   {col_name}: {col_type}")
        
        # Build query based on available columns
        available_columns = [col[0] for col in columns_info]
        
        # Base columns that should always exist
        base_columns = ['source', 'fuel_type', 'price', 'timestamp', 'region', 'scraped_at']
        
        # Optional columns
        optional_columns = ['consensus', 'surprise', 'as_of_date']
        
        # Select only columns that exist
        select_columns = []
        for col in base_columns + optional_columns:
            if col in available_columns:
                select_columns.append(col)
        
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM gas_prices
        ORDER BY scraped_at DESC
        """
        
        print(f"\n📊 Fetching data with columns: {', '.join(select_columns)}")
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("❌ No data found in database")
            return
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gas_prices_database_export_{timestamp}.csv"
        
        # Export to CSV
        print(f"💾 Exporting {len(df)} records to {filename}...")
        df.to_csv(filename, index=False)
        
        # Print summary
        print(f"✅ Export completed successfully!")
        print(f"📁 File: {filename}")
        print(f"📊 Total records: {len(df)}")
        print("\n📈 Records by source:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            print(f"   {source}: {count}")
        
        # Show date range
        if not df.empty and 'scraped_at' in df.columns:
            earliest = df['scraped_at'].min()
            latest = df['scraped_at'].max()
            print(f"\n📅 Date range: {earliest} to {latest}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        return None

if __name__ == "__main__":
    export_database_to_csv()
