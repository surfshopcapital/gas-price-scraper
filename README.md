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
- **Backend**: Python 3.11, Selenium WebDriver, PostgreSQL
- **Frontend**: Streamlit, Plotly
- **Scheduling**: Python schedule library
- **Cloud**: Railway.app with Nixpacks
- **Database**: PostgreSQL (cloud-hosted)

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
├── gas_scraper.py          # Main scraper logic
├── dashboard.py            # Streamlit dashboard
├── run_scraper.py          # Scraper launcher for Railway
├── setup_database.py       # Database initialization
├── backup_database.py      # Automated backup system
├── test_postgresql.py      # Database connection testing
├── requirements.txt        # Python dependencies
├── railway.json           # Railway scraper service config
├── railway_dashboard.json # Railway dashboard service config
├── nixpacks.toml         # Railway build configuration
├── Procfile              # Railway process definition
└── README.md             # This file
```

## ⚙️ Configuration

### Scraping Intervals
- **GasBuddy**: Every 15 minutes
- **AAA**: Daily at 12:01 AM Pacific
- **RBOB/WTI**: Every 2 hours (Mon-Fri market hours)
- **EIA Data**: Daily at 11 AM EST
- **Backups**: Daily at 6 PM EST
- **Excel Export**: Daily at 5 PM EST

### Database Schema
```sql
CREATE TABLE gas_prices (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100),
    fuel_type VARCHAR(100),
    price NUMERIC,
    timestamp VARCHAR(100),
    region VARCHAR(100),
    consensus NUMERIC,
    surprise NUMERIC,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔧 Troubleshooting

### Common Issues

#### DevTools Disconnection (Railway)
**Problem**: `"disconnected: not connected to DevTools"` errors in cloud environment

**Cause**: Containerized Chrome processes are less stable than local installations

**Solutions**:
1. **Use GasBuddy Pattern**: All scraping methods now use the proven GasBuddy approach
2. **Simplified Navigation**: Removed complex retry logic that caused failures
3. **Proper Driver Cleanup**: Each scraping job creates and destroys its own Chrome driver
4. **Increased Wait Times**: Longer delays for page loading in cloud environment

#### Chrome Binary Issues
**Problem**: Chrome/Chromium not found in Railway environment

**Solution**: Nixpacks configuration with system dependencies
```toml
[phases.setup]
nixPkgs = ["python311", "postgresql_16.dev", "gcc", "chromium", "chromedriver", "coreutils", "findutils"]
```

### Local vs Cloud Differences

| Aspect | Local | Railway |
|--------|-------|---------|
| Chrome Stability | ✅ High | ⚠️ Medium |
| DevTools Connection | ✅ Stable | ⚠️ Fragile |
| Resource Availability | ✅ Unlimited | ⚠️ Limited |
| Network Latency | ✅ Low | ⚠️ Variable |
| Process Management | ✅ Native | ⚠️ Containerized |

## 📊 Data Flow

1. **Scheduler triggers** scraping jobs based on configured intervals
2. **Selenium WebDriver** navigates to target websites
3. **Data extraction** using CSS selectors and page parsing
4. **PostgreSQL storage** with automatic timestamping
5. **Dashboard updates** in real-time from database
6. **Automated backups** to CSV and SQL formats
7. **Excel exports** for daily and monthly reporting

## 🚨 Monitoring & Alerts

### Log Levels
- **✅ Success**: Data successfully scraped and stored
- **⚠️ Warning**: Non-critical issues (e.g., missing timestamps)
- **❌ Error**: Critical failures requiring attention
- **🔄 Retry**: Automatic retry attempts for failed jobs

### Health Checks
- Database connection status
- Chrome driver availability
- Scraping success rates
- Backup completion status

## 🔒 Security & Best Practices

- **Environment Variables**: Sensitive data stored in Railway environment
- **Database Isolation**: Separate database service for data persistence
- **Process Cleanup**: Automatic Chrome process cleanup after each job
- **Error Handling**: Comprehensive exception handling and logging
- **Resource Management**: Efficient memory and CPU usage

## 📈 Performance Optimization

- **Driver Reuse**: Each scraping job uses fresh Chrome driver
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

**Last Updated**: August 25, 2025  
**Version**: 2.0.0  
**Status**: Production Ready ✅
