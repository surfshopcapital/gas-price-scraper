import duckdb
import pandas as pd
from datetime import datetime

def view_gas_data(db_path="gas_prices.duckdb"):
    """View and analyze gas price data from DuckDB"""
    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)
        
        # Get basic statistics
        print("🚗 Hexa Source Gas Price Data Analysis")
        print("=" * 50)
        
        # Check if table exists and has data
        table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gas_prices'").fetchone()
        if not table_exists:
            print("❌ No gas_prices table found. Run the scraper first.")
            return
        
        # Get total count
        total_count = conn.execute("SELECT COUNT(*) FROM gas_prices").fetchone()[0]
        print(f"📊 Total records: {total_count}")
        
        if total_count == 0:
            print("❌ No data found. Run the scraper first.")
            return
        
        # Get data by source
        print("\n📈 Data by Source:")
        print("-" * 30)
        
        source_stats = conn.execute('''
            SELECT source, COUNT(*) as count, 
                   AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price
            FROM gas_prices
            GROUP BY source
            ORDER BY count DESC
        ''').fetchall()
        
        for source, count, avg_price, min_price, max_price in source_stats:
            print(f"{source}: {count} records, Avg: ${avg_price:.3f}, Range: ${min_price:.3f}-${max_price:.3f}")
        
        # Get latest data
        print("\n📈 Latest 15 Gas Prices:")
        print("-" * 60)
        
        latest_data = conn.execute('''
            SELECT price, timestamp, region, source, fuel_type, scraped_at
            FROM gas_prices
            ORDER BY scraped_at DESC
            LIMIT 15
        ''').fetchall()
        
        for price, timestamp, region, source, fuel_type, scraped_at in latest_data:
            print(f"${price:.3f} - {timestamp} ({region}) - {source} - {fuel_type} - {scraped_at}")
        
        # Get price statistics
        print("\n📊 Overall Price Statistics:")
        print("-" * 35)
        
        stats = conn.execute('''
            SELECT 
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                COUNT(*) as total_records
            FROM gas_prices
        ''').fetchall()[0]
        
        min_price, max_price, avg_price, total_records = stats
        print(f"Lowest: ${min_price:.3f}")
        print(f"Highest: ${max_price:.3f}")
        print(f"Average: ${avg_price:.3f}")
        print(f"Total Records: {total_records}")
        
        # Get data by date (last 7 days)
        print("\n📅 Last 7 Days Summary:")
        print("-" * 35)
        
        daily_stats = conn.execute('''
            SELECT 
                DATE(scraped_at) as date,
                COUNT(*) as records,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM gas_prices
            WHERE scraped_at >= DATE('now', '-7 days')
            GROUP BY DATE(scraped_at)
            ORDER BY date DESC
        ''').fetchall()
        
        for date, records, avg_price, min_price, max_price in daily_stats:
            print(f"{date}: {records} records, Avg: ${avg_price:.3f}, Range: ${min_price:.3f}-${max_price:.3f}")
        
        # Export to CSV option
        print("\n💾 Export Options:")
        print("-" * 20)
        print("1. Export all data to CSV")
        print("2. Export last 7 days to CSV")
        print("3. Export by source (GasBuddy only)")
        print("4. Export by source (AAA only)")
        print("5. Export by source (RBOB only)")
        print("6. Export by source (WTI only)")
        print("7. Export by source (Gasoline Stocks only)")
        print("8. Export by source (Refinery Runs only)")
        print("9. Exit")
        
        choice = input("\nEnter your choice (1-9): ").strip()
        
        if choice == "1":
            # Export all data
            all_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(all_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"gas_prices_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} records to {filename}")
            
        elif choice == "2":
            # Export last 7 days
            week_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE scraped_at >= DATE('now', '-7 days')
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(week_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"gas_prices_week_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} records to {filename}")
            
        elif choice == "3":
            # Export GasBuddy data only
            gasbuddy_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'gasbuddy_fuel_insights'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(gasbuddy_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"gasbuddy_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} GasBuddy records to {filename}")
            
        elif choice == "4":
            # Export AAA data only
            aaa_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'aaa_gas_prices'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(aaa_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"aaa_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} AAA records to {filename}")
            
        elif choice == "5":
            # Export RBOB data only
            rbob_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_rbob_futures'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(rbob_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"rbob_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} RBOB records to {filename}")
            
        elif choice == "6":
            # Export WTI data only
            wti_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_wti_futures'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(wti_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"wti_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} WTI records to {filename}")
            
        elif choice == "7":
            # Export Gasoline Stocks data only
            gasoline_stocks_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_gasoline_stocks'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(gasoline_stocks_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'consensus', 'surprise', 'scraped_at'])
            filename = f"gasoline_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} Gasoline Stocks records to {filename}")
            
        elif choice == "8":
            # Export Refinery Runs data only
            refinery_runs_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_refinery_runs'
                ORDER BY scraped_at DESC
            ''').fetchall()
            
            df = pd.DataFrame(refinery_runs_data, columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
            filename = f"refinery_runs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} Refinery Runs records to {filename}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error viewing data: {e}")

if __name__ == "__main__":
    view_gas_data()
