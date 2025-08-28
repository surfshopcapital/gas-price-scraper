import os
import psycopg2
from datetime import datetime

def clean_corrupted_data():
    """Remove the corrupted RBOB data point from August 26th (5.858)"""
    
    # Database connection
    database_url = os.getenv('DATABASE_URL',
        'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
    
    try:
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # Find the corrupted RBOB data point
        print("🔍 Looking for corrupted RBOB data...")
        cur.execute("""
            SELECT id, price, scraped_at 
            FROM gas_prices 
            WHERE source = 'marketwatch_rbob_futures' 
            AND DATE(scraped_at) = '2025-08-26'
            AND price > 5.0
            ORDER BY scraped_at DESC
        """)
        
        corrupted_rows = cur.fetchall()
        
        if not corrupted_rows:
            print("✅ No corrupted RBOB data found for August 26th")
            return
        
        print(f"⚠️ Found {len(corrupted_rows)} corrupted RBOB records:")
        for row_id, price, scraped_at in corrupted_rows:
            print(f"   ID: {row_id}, Price: {price}, Time: {scraped_at}")
        
        # Remove the corrupted data
        print("\n🗑️ Removing corrupted data...")
        cur.execute("""
            DELETE FROM gas_prices 
            WHERE source = 'marketwatch_rbob_futures' 
            AND DATE(scraped_at) = '2025-08-26'
            AND price > 5.0
        """)
        
        deleted_count = cur.rowcount
        conn.commit()
        
        print(f"✅ Successfully removed {deleted_count} corrupted RBOB records")
        
        # Verify the cleanup
        cur.execute("""
            SELECT COUNT(*) 
            FROM gas_prices 
            WHERE source = 'marketwatch_rbob_futures' 
            AND DATE(scraped_at) = '2025-08-26'
        """)
        
        remaining_count = cur.fetchone()[0]
        print(f"📊 Remaining RBOB records for August 26th: {remaining_count}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error cleaning data: {e}")
        return None

if __name__ == "__main__":
    clean_corrupted_data()
