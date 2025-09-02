import os
import io
import psycopg2
import pandas as pd
from datetime import datetime, time
from dateutil import tz

# Configuration
CSV_PATH = "Correct_JKB_Database.csv"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway",
)

TZ_NY = tz.gettz("America/New_York")

def parse_scraped_at(row):
    """Parse scraped_at based on source type"""
    src = str(row.get("source", "")).strip()
    ts_txt = str(row.get("timestamp", "")).strip()
    raw_scraped = row.get("scraped_at", None)
    now_utc = pd.Timestamp.utcnow()

    try:
        if src == "aaa_gas_prices":
            # AAA: Use the date from timestamp and set to 8:00 AM NY time
            import re
            m = re.search(r"(\d{4}-\d{2}-\d{2})", ts_txt)
            if m:
                d = pd.to_datetime(m.group(1), errors="coerce")
                if not pd.isna(d):
                    dt = pd.Timestamp.combine(d.date(), time(8, 0)).replace(tzinfo=TZ_NY).astimezone(tz.UTC)
                    return dt
            return now_utc

        elif src == "gasbuddy_fuel_insights":
            # GasBuddy: Parse the full timestamp format "9/2/2025 12:25:02 AM"
            if pd.isna(raw_scraped) or str(raw_scraped).strip() == "":
                return now_utc
            
            raw_str = str(raw_scraped).strip()
            try:
                dt_local = pd.to_datetime(raw_str, format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
                if not pd.isna(dt_local):
                    if dt_local.tzinfo is None:
                        dt_local = dt_local.tz_localize(TZ_NY)
                    else:
                        dt_local = dt_local.tz_convert(TZ_NY)
                    return dt_local.astimezone(tz.UTC)
            except:
                pass
            return now_utc

        elif src.startswith("marketwatch_"):
            # MarketWatch: Parse the timestamp and convert to UTC
            ts_clean = ts_txt.replace(" EDT", "").replace(" ET", "")
            dt = pd.to_datetime(ts_clean, errors="coerce")
            if not pd.isna(dt):
                dt = dt.tz_localize(TZ_NY).astimezone(tz.UTC)
                return dt
            return now_utc

        elif src.startswith("tradingeconomics_"):
            # TradingEconomics: Use the date and set to midnight NY time
            d = pd.to_datetime(ts_txt, errors="coerce")
            if pd.isna(d):
                return now_utc
            d_local_midnight = pd.Timestamp.combine(d.date(), time(0,0)).replace(tzinfo=TZ_NY)
            return d_local_midnight.astimezone(tz.UTC)

        return now_utc
    except Exception:
        return now_utc

def parse_df(csv_path):
    """Parse and clean the CSV data"""
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Initial rows: {len(df)}")
    
    # Ensure all required columns exist
    required_cols = ["source","fuel_type","price","timestamp","region","consensus","surprise","scraped_at","as_of_date"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    
    # Convert to strings
    df["timestamp"] = df["timestamp"].astype(str)
    df["source"] = df["source"].astype(str)
    df["fuel_type"] = df["fuel_type"].astype(str)
    df["region"] = df["region"].astype(str)
    
    # Normalize fuel_type for GasBuddy
    def normalize_fuel_type(row):
        fuel_type = str(row.get('fuel_type', '')).strip().lower()
        source = str(row.get('source', '')).strip()
        if source == 'gasbuddy_fuel_insights' and fuel_type in ['regular', 'regular_gas']:
            return 'regular_gas'
        return fuel_type
    
    df['fuel_type'] = df.apply(normalize_fuel_type, axis=1)
    
    # Convert numeric columns
    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    df["consensus"] = pd.to_numeric(df["consensus"], errors='coerce')
    df["surprise"] = pd.to_numeric(df["surprise"], errors='coerce')
    
    # Parse scraped_at
    print("Parsing scraped_at timestamps...")
    df["scraped_at"] = df.apply(parse_scraped_at, axis=1)
    
    # Parse as_of_date for AAA
    def parse_as_of_date(row):
        if str(row.get('source', '')).strip() == "aaa_gas_prices":
            import re
            m = re.search(r"(\d{4}-\d{2}-\d{2})", str(row.get('timestamp', '')))
            if m:
                return pd.to_datetime(m.group(1)).date()
        return None
    
    df["as_of_date"] = df.apply(parse_as_of_date, axis=1)
    
    # Remove completely empty rows and rows with NaN/null values
    initial_count = len(df)
    
    # Remove rows where source or fuel_type is NaN/null
    df = df.dropna(subset=['source', 'fuel_type'])
    
    # Remove rows where source or fuel_type is empty string or 'nan'
    df = df[df['source'].str.strip() != '']
    df = df[df['source'].str.lower() != 'nan']
    df = df[df['fuel_type'].str.strip() != '']
    df = df[df['fuel_type'].str.lower() != 'nan']
    
    print(f"Removed {initial_count - len(df)} empty/null rows")
    
    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['source', 'fuel_type', 'scraped_at'], keep='first')
    print(f"Removed {initial_count - len(df)} duplicate rows")
    
    print(f"Final rows: {len(df)}")
    return df[required_cols]

def create_table():
    """Create the gas_prices table"""
    return """
    DROP TABLE IF EXISTS gas_prices CASCADE;
    CREATE TABLE gas_prices (
        id BIGSERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        fuel_type TEXT,
        price NUMERIC(10,4),
        timestamp TEXT,
        region TEXT,
        consensus NUMERIC(10,4),
        surprise NUMERIC(10,4),
        scraped_at TIMESTAMPTZ NOT NULL,
        as_of_date DATE
    );
    CREATE UNIQUE INDEX ux_gas_prices_unique
    ON gas_prices (source, fuel_type, scraped_at);
    """

def import_data(conn, df):
    """Import the DataFrame to the database"""
    print("Importing data to database...")
    
    with conn.cursor() as cur:
        # Create table
        print("Creating table...")
        cur.execute(create_table())
        
        # Import data
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=True, date_format="%Y-%m-%d %H:%M:%S%z")
        buf.seek(0)
        
        cur.copy_expert(
            """
            COPY gas_prices (source,fuel_type,price,timestamp,region,consensus,surprise,scraped_at,as_of_date)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '', QUOTE '"')
            """,
            buf
        )
    
    conn.commit()
    print(f"Successfully imported {len(df)} records")

def main():
    print("Starting import process...")
    print(f"CSV file: {CSV_PATH}")
    
    # Parse the CSV
    df = parse_df(CSV_PATH)
    
    # Connect to database and import
    conn = psycopg2.connect(DATABASE_URL)
    try:
        import_data(conn, df)
        print("Import complete!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
