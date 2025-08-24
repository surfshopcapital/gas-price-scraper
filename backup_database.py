#!/usr/bin/env python3
"""
Database Backup Script
Creates daily backups of the gas_prices database in multiple formats.
"""

import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import subprocess
import sys

def backup_to_csv():
    """Backup data to CSV files"""
    try:
        print("📊 Creating CSV backup...")
        
        # Get database connection
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        conn = psycopg2.connect(database_url)
        
        # Get today's data
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Create backup directory
        backup_dir = "backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Backup today's data
        today_query = """
            SELECT * FROM gas_prices 
            WHERE DATE(scraped_at) = %s 
            ORDER BY scraped_at DESC
        """
        today_df = pd.read_sql(today_query, conn, params=(today,))
        
        if not today_df.empty:
            today_filename = f"{backup_dir}/gas_prices_{today.strftime('%Y%m%d')}.csv"
            today_df.to_csv(today_filename, index=False)
            print(f"   ✅ Today's data: {len(today_df)} records → {today_filename}")
        
        # Backup yesterday's data
        yesterday_query = """
            SELECT * FROM gas_prices 
            WHERE DATE(scraped_at) = %s 
            ORDER BY scraped_at DESC
        """
        yesterday_df = pd.read_sql(yesterday_query, conn, params=(yesterday,))
        
        if not yesterday_df.empty:
            yesterday_filename = f"{backup_dir}/gas_prices_{yesterday.strftime('%Y%m%d')}.csv"
            yesterday_df.to_csv(yesterday_filename, index=False)
            print(f"   ✅ Yesterday's data: {len(yesterday_df)} records → {yesterday_filename}")
        
        # Backup all data (monthly)
        all_query = """
            SELECT * FROM gas_prices 
            WHERE scraped_at >= %s 
            ORDER BY scraped_at DESC
        """
        month_start = today.replace(day=1)
        all_df = pd.read_sql(all_query, conn, params=(month_start,))
        
        if not all_df.empty:
            all_filename = f"{backup_dir}/gas_prices_monthly_{today.strftime('%Y%m')}.csv"
            all_df.to_csv(all_filename, index=False)
            print(f"   ✅ Monthly data: {len(all_df)} records → {all_filename}")
        
        conn.close()
        print("✅ CSV backup completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ CSV backup failed: {e}")
        return False

def backup_to_sql():
    """Backup data to SQL file using pg_dump"""
    try:
        print("🗄️ Creating SQL backup...")
        
        # Parse database URL to get connection details
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        
        # Extract components from URL
        if database_url.startswith('postgresql://'):
            parts = database_url.replace('postgresql://', '').split('@')
            if len(parts) == 2:
                user_pass = parts[0].split(':')
                host_port_db = parts[1].split('/')
                
                if len(user_pass) == 2 and len(host_port_db) == 2:
                    username = user_pass[0]
                    password = user_pass[1]
                    host_port = host_port_db[0].split(':')
                    host = host_port[0]
                    port = host_port[1] if len(host_port) > 1 else '5432'
                    database = host_port_db[1]
                    
                    # Create backup directory
                    backup_dir = "backups"
                    if not os.path.exists(backup_dir):
                        os.makedirs(backup_dir)
                    
                    # Create backup filename
                    today = datetime.now().strftime('%Y%m%d')
                    backup_filename = f"{backup_dir}/gas_prices_backup_{today}.sql"
                    
                    # Set environment variable for password
                    env = os.environ.copy()
                    env['PGPASSWORD'] = password
                    
                    # Run pg_dump
                    cmd = [
                        'pg_dump',
                        '-h', host,
                        '-p', port,
                        '-U', username,
                        '-d', database,
                        '-t', 'gas_prices',
                        '-f', backup_filename
                    ]
                    
                    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        print(f"   ✅ SQL backup created: {backup_filename}")
                        return True
                    else:
                        print(f"   ❌ pg_dump failed: {result.stderr}")
                        return False
        
        print("   ⚠️ Could not parse database URL for pg_dump")
        return False
        
    except Exception as e:
        print(f"❌ SQL backup failed: {e}")
        return False

def main():
    """Main backup function"""
    print("💾 Starting database backup process...")
    print("=" * 50)
    
    success_count = 0
    
    # Try CSV backup
    if backup_to_csv():
        success_count += 1
    
    # Try SQL backup (if pg_dump is available)
    if backup_to_sql():
        success_count += 1
    
    print("=" * 50)
    if success_count > 0:
        print(f"✅ Backup completed! {success_count}/2 backup methods succeeded.")
    else:
        print("❌ All backup methods failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
