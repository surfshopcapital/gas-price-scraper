import pandas as pd
import psycopg2
import os
from datetime import datetime

def delete_incorrect_rbob_records():
    """
    Delete the two incorrect RBOB future records with value 3.23 on August 28th.
    """
    
    # Database connection parameters
    database_url = os.getenv('DATABASE_URL',
        'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # First, let's identify the problematic records
        print("Identifying RBOB future records with value 3.23 on August 28th...")
        
        # Query to find the problematic records
        find_query = """
        SELECT id, source, fuel_type, price, timestamp, scraped_at
        FROM gas_prices 
        WHERE source = 'marketwatch_rbob_futures' 
        AND price = 3.23 
        AND DATE(scraped_at) = '2025-08-28'
        ORDER BY scraped_at
        """
        
        cursor.execute(find_query)
        problematic_records = cursor.fetchall()
        
        if not problematic_records:
            print("No RBOB future records found with value 3.23 on August 28th.")
            return
        
        print(f"Found {len(problematic_records)} problematic records:")
        print("-" * 80)
        for record in problematic_records:
            print(f"ID: {record[0]}, Source: {record[1]}, Fuel: {record[2]}, Price: {record[3]}, Timestamp: {record[4]}, Scraped: {record[5]}")
        print("-" * 80)
        
        # Ask for confirmation
        confirm = input(f"\nDo you want to delete these {len(problematic_records)} records? (yes/no): ").lower().strip()
        
        if confirm != 'yes':
            print("Operation cancelled.")
            return
        
        # Delete the problematic records
        print("Deleting problematic records...")
        
        delete_query = """
        DELETE FROM gas_prices 
        WHERE source = 'marketwatch_rbob_futures' 
        AND price = 3.23 
        AND DATE(scraped_at) = '2025-08-28'
        """
        
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        
        # Commit the changes
        conn.commit()
        
        print(f"Successfully deleted {deleted_count} records.")
        
        # Verify deletion
        print("\nVerifying deletion...")
        cursor.execute(find_query)
        remaining_records = cursor.fetchall()
        
        if not remaining_records:
            print("✓ All problematic records have been successfully deleted.")
        else:
            print(f"⚠ Warning: {len(remaining_records)} problematic records still remain.")
            for record in remaining_records:
                print(f"  ID: {record[0]}, Price: {record[3]}, Scraped: {record[5]}")
        
        # Show summary of remaining RBOB records for August 28th
        print("\nSummary of remaining RBOB records for August 28th:")
        summary_query = """
        SELECT price, COUNT(*) as count
        FROM gas_prices 
        WHERE source = 'marketwatch_rbob_futures' 
        AND DATE(scraped_at) = '2025-08-28'
        GROUP BY price
        ORDER BY price
        """
        
        cursor.execute(summary_query)
        summary = cursor.fetchall()
        
        if summary:
            for price, count in summary:
                print(f"  Price ${price}: {count} records")
        else:
            print("  No RBOB records found for August 28th")
        
        conn.close()
        print("\nDatabase connection closed.")
        
    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    print("RBOB Future Records Cleanup Script")
    print("=" * 50)
    delete_incorrect_rbob_records()
