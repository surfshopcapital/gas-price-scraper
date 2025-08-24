-- Create the gas_prices table for storing gas price data
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

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_gas_prices_source ON gas_prices(source);
CREATE INDEX IF NOT EXISTS idx_gas_prices_scraped_at ON gas_prices(scraped_at);
CREATE INDEX IF NOT EXISTS idx_gas_prices_timestamp ON gas_prices(timestamp);

-- Create a unique constraint to prevent duplicate entries
CREATE UNIQUE INDEX IF NOT EXISTS idx_gas_prices_unique 
ON gas_prices(source, fuel_type, timestamp, scraped_at);

-- Insert some sample data for testing (optional)
INSERT INTO gas_prices (source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at) VALUES
('gasbuddy_fuel_insights', 'regular', 3.147, 'Current as of 2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
('aaa_gas_prices', 'regular', 3.152, 'Current as of 2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
('marketwatch_rbob_futures', 'futures', 2.456, '2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
('marketwatch_wti_futures', 'futures', 78.23, '2025-08-24', 'United States', NULL, NULL, CURRENT_TIMESTAMP),
('tradingeconomics_gasoline_stocks', 'stocks', NULL, '2025-08-24', 'United States', -1.5, -0.8, CURRENT_TIMESTAMP),
('tradingeconomics_refinery_runs', 'refinery', NULL, '2025-08-24', 'United States', 0.2, 0.1, CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;
