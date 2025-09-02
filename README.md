# 🚗 Gas Price Scraper & Dashboard

A comprehensive gas price monitoring system that scrapes data from multiple sources and provides real-time analytics through a Streamlit dashboard.

## 🌟 Features

- **Multi-Source Data Collection**: GasBuddy, AAA, RBOB Futures, WTI Crude, EIA Data
- **Real-Time Dashboard**: Streamlit-based interface with interactive charts
- **Automated Scheduling**: Configurable scraping intervals for different data sources
- **Cloud Deployment**: Railway.app hosting for both scraper and dashboard
- **Data Backup**: Automated daily CSV and SQL backups
- **Excel Export**: Daily and monthly data exports with multiple tabs

## 🏗️ Architecture

### Data Sources
- **GasBuddy**: Real-time fuel insights (every 15 minutes)
- **AAA**: Daily gas price averages (12:01 AM Pacific)
- **RBOB Futures**: MarketWatch data (every 2 hours during market hours)
- **WTI Crude**: MarketWatch futures (every 2 hours during market hours)
- **EIA Data**: Gasoline stocks and refinery runs (daily at 11 AM EST)

### Technology Stack
- **Backend**: Python 3.11, Playwright, PostgreSQL
- **Frontend**: Streamlit, Plotly
- **Scheduling**: Python schedule library
- **Cloud**: Railway.app with Nixpacks
- **Database**: PostgreSQL (cloud-hosted)
- **Data Import/Export**: Pandas, CSV processing

## 🚀 Quick Start

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd gas-scraper
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up database**
   ```bash
   python setup_database.py
   ```

4. **Run the scraper**
   ```bash
   python gas_scraper.py
   ```

5. **Launch dashboard**
   ```bash
   streamlit run dashboard.py
   ```

### Database Management

#### Import CSV Data
To import cleaned CSV data into the database:
```bash
python import_database.py
```

#### Export Database
To export current database to CSV:
```bash
python export_database_csv.py
```

### Cloud Deployment

#### Railway.app Setup

1. **Create Railway project**
   - Connect your GitHub repository
   - Add PostgreSQL database service

2. **Configure environment variables**
   - `DATABASE_URL`: PostgreSQL connection string

3. **Deploy services**
   - **Dashboard Service**: Uses `railway_dashboard.json`
   - **Scraper Service**: Uses `railway.json`

#### Service Configuration

- **Dashboard**: Streamlit app accessible via public URL
- **Scraper**: Background service running continuously

## 📁 Project Structure

```
gas-scraper/
├── gas_scraper.py          # Main scraper logic (Playwright-based)
├── dashboard.py            # Streamlit dashboard
├── import_database.py      # Database import utility for CSV data
├── export_database_csv.py  # Database export utility
├── run_scraper.py          # Scraper launcher for Railway
├── setup_database.py       # Database initialization
├── backup_database.py      # Automated backup system
├── playwright_adapter.py   # Playwright browser automation
├── requirements.txt        # Python dependencies
├── railway.json           # Railway scraper service config
├── railway_dashboard.json # Railway dashboard service config
├── nixpacks_dashboard.toml # Railway dashboard build config
├── Procfile              # Railway process definition
├── Correct_JKB_Database.csv # Cleaned database export
├── data/                 # Historical data files
├── backups/              # Automated backup files
└── README.md             # This file
```

## ⚙️ Configuration

### Scraping Intervals
- **GasBuddy**: Every 10 minutes
- **AAA**: Daily at 3:30 AM EST (7:30 UTC)
- **RBOB/WTI**: Every hour (Sun 6pm-Fri 8pm EST)
- **EIA Data**: Daily at 11:00, 12:00, 13:00, 17:00 EST
- **Backups**: Daily at 11:00 PM UTC
- **Excel Export**: Daily at 10:00 PM UTC

### Database Schema
```sql
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
```

## 🔧 Troubleshooting

### Common Issues

#### Playwright Browser Issues (Railway)
**Problem**: Browser automation failures in cloud environment

**Cause**: Containerized browser processes are less stable than local installations

**Solutions**:
1. **Playwright Configuration**: Optimized browser settings for cloud deployment
2. **Proper Context Management**: Each scraping job creates and destroys its own browser context
3. **Increased Wait Times**: Longer delays for page loading in cloud environment
4. **Error Handling**: Comprehensive retry logic and graceful failure handling

#### Database Import Issues
**Problem**: CSV import failures with timestamp parsing errors

**Solutions**:
1. **Data Validation**: Robust validation of numeric columns before import
2. **Timestamp Parsing**: Source-specific timestamp parsing logic
3. **Duplicate Handling**: Proper deduplication based on unique constraints
4. **Error Logging**: Detailed error messages for troubleshooting

### Local vs Cloud Differences

| Aspect | Local | Railway |
|--------|-------|---------|
| Browser Stability | ✅ High | ⚠️ Medium |
| Playwright Performance | ✅ Fast | ⚠️ Slower |
| Resource Availability | ✅ Unlimited | ⚠️ Limited |
| Network Latency | ✅ Low | ⚠️ Variable |
| Process Management | ✅ Native | ⚠️ Containerized |

## 📊 Data Flow

1. **Scheduler triggers** scraping jobs based on configured intervals
2. **Playwright browser** navigates to target websites
3. **Data extraction** using CSS selectors and page parsing
4. **PostgreSQL storage** with automatic timestamping
5. **Dashboard updates** in real-time from database
6. **Automated backups** to CSV and SQL formats
7. **Excel exports** for daily and monthly reporting
8. **CSV import/export** for data management and migration

## 🚨 Monitoring & Alerts

### Log Levels
- **✅ Success**: Data successfully scraped and stored
- **⚠️ Warning**: Non-critical issues (e.g., missing timestamps)
- **❌ Error**: Critical failures requiring attention
- **🔄 Retry**: Automatic retry attempts for failed jobs

### Health Checks
- Database connection status
- Playwright browser availability
- Scraping success rates
- Backup completion status
- Data import/export functionality

## 🔒 Security & Best Practices

- **Environment Variables**: Sensitive data stored in Railway environment
- **Database Isolation**: Separate database service for data persistence
- **Process Cleanup**: Automatic browser process cleanup after each job
- **Error Handling**: Comprehensive exception handling and logging
- **Resource Management**: Efficient memory and CPU usage

## 📈 Performance Optimization

- **Browser Context Management**: Each scraping job uses fresh browser context
- **Parallel Processing**: Independent scraping jobs don't block each other
- **Memory Management**: Automatic cleanup of browser processes
- **Network Efficiency**: Optimized wait times for different websites

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally and in Railway
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review Railway deployment logs
3. Test locally to isolate cloud vs local issues
4. Check database connectivity and schema

---

**Last Updated**: September 2, 2025  
**Version**: 2.1.0  
**Status**: Production Ready ✅
