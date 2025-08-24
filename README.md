# ⛽ Gas Price Scraper & Dashboard

A comprehensive web scraping and data visualization system for gas prices and energy market data from multiple sources.

## 🚀 Features

### Data Sources
- **GasBuddy Fuel Insights** - Real-time gas prices (every 10 minutes)
- **AAA Gas Prices** - Daily gas price averages (12:01 AM Pacific)
- **RBOB Futures** - Gasoline futures prices (every 2 hours, Mon-Fri market hours)
- **WTI Crude Futures** - Oil futures prices (every 2 hours, Mon-Fri market hours)
- **Gasoline Stocks Change** - EIA data with consensus & surprise (daily at 11 AM EST)
- **Refinery Crude Runs** - EIA refinery data (daily at 11 AM EST)

### Dashboard Features
- **Real-time Data Visualization** - Interactive charts and tables
- **Multi-source Comparison** - Side-by-side data analysis
- **Historical Trends** - 30-day price history with Plotly charts
- **Responsive Design** - Works on desktop and mobile
- **Auto-refresh** - Updates every 5 minutes

## 📁 Project Structure

```
Gas Scraper/
├── gas_scraper.py          # Main scraping script with scheduling
├── dashboard.py            # Streamlit dashboard application
├── view_data.py            # Command-line data viewer
├── requirements.txt        # Dependencies for scraper
├── requirements_dashboard.txt # Dependencies for dashboard
├── gas_prices.duckdb      # Database (created automatically)
├── README.md              # This file
└── .gitignore             # Git ignore file
```

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone <repository-url>
cd "Gas Scraper"
```

### 2. Create Virtual Environment
```bash
# Using conda (recommended)
conda create -n gas_scraper python=3.9
conda activate gas_scraper

# Or using venv
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
```

### 3. Install Dependencies
```bash
# For the scraper
pip install -r requirements.txt

# For the dashboard
pip install -r requirements_dashboard.txt
```

## 🚀 Usage

### Running the Scraper
```bash
python gas_scraper.py
```

**Menu Options:**
1. **Test GasBuddy** - Test GasBuddy scraping
2. **Test AAA** - Test AAA scraping  
3. **Test RBOB** - Test RBOB futures scraping
4. **Test WTI** - Test WTI futures scraping
5. **Test Gasoline Stocks** - Test EIA gasoline stocks
6. **Test Refinery Runs** - Test EIA refinery runs
7. **Run GasBuddy Job** - Run GasBuddy scraping job
8. **Run AAA Job** - Run AAA scraping job
9. **Run RBOB Job** - Run RBOB scraping job
10. **Export Daily Excel** - Export today's data to Excel
11. **Export Monthly Excel** - Export month's data to Excel
12. **Run Scheduled Jobs** - Start all scheduled scraping jobs

### Running the Dashboard
```bash
streamlit run dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

### Viewing Data
```bash
python view_data.py
```

## 📊 Dashboard Features

### Main Dashboard
- **Overview Metrics** - Total sources, latest updates, database size
- **Latest Data Cards** - Current values for each data source
- **Interactive Charts** - Time series plots with Plotly
- **Data Tables** - Recent records for each source
- **Summary Statistics** - Price ranges and data freshness

### Sidebar Controls
- **Time Range Filter** - 7 days, 30 days, 90 days, or all time
- **Source Filter** - Select which data sources to display
- **Real-time Updates** - Data refreshes every 5 minutes

## 🗄️ Database Schema

The `gas_prices` table contains:
- `id` - Auto-incrementing primary key
- `price` - Numeric price/value
- `timestamp` - Date/time of the data
- `region` - Geographic region
- `source` - Data source identifier
- `fuel_type` - Type of fuel/commodity
- `consensus` - Consensus estimate (for EIA data)
- `surprise` - Actual vs consensus difference
- `scraped_at` - When the data was scraped

## ⏰ Scheduling

### Scraping Schedule
- **GasBuddy**: Every 10 minutes
- **AAA**: Daily at 12:01 AM Pacific (3:01 AM UTC)
- **RBOB/WTI**: Every 2 hours, Monday 1 AM EST to Friday 11 PM EST
- **EIA Data**: Daily at 11 AM EST (4 PM UTC)

### Export Schedule
- **Daily Excel**: Daily at 5 PM EST (10 PM UTC)
- **Monthly Excel**: 1st of month at 5 PM EST (10 PM UTC)

## 🌐 Streamlit Cloud Deployment

### 1. Push to GitHub
```bash
git add .
git commit -m "Add Streamlit dashboard"
git push origin main
```

### 2. Deploy on Streamlit Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Connect your GitHub repository
3. Set the main file path to `dashboard.py`
4. Deploy!

### 3. Environment Variables (if needed)
- Set any API keys or configuration in Streamlit Cloud's secrets management

## 🔧 Configuration

### Database Location
The database file `gas_prices.duckdb` is created automatically in the project directory.

### Chrome Driver
The scraper uses undetected ChromeDriver for web scraping. Make sure you have Chrome installed.

### Location Permissions
The scraper automatically handles geolocation permissions using Chrome DevTools Protocol.

## 📈 Data Export

### Excel Reports
- **Daily Report**: Contains all data scraped today with separate tabs
- **Monthly Report**: Contains all data from the current month
- **Tabs**: GasBuddy, AAA, RBOB, WTI, Gasoline_Stocks, Refinery_Runs

### CSV Export
Use `view_data.py` to export specific data sources to CSV format.

## 🐛 Troubleshooting

### Common Issues
1. **Chrome Driver Errors**: Ensure Chrome is installed and up to date
2. **Database Errors**: Delete `gas_prices.duckdb` to reset the database
3. **Scraping Failures**: Check internet connection and website availability
4. **Dashboard Loading**: Ensure the database exists and contains data

### Debug Mode
Run individual scraping tests using the menu options to isolate issues.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is for educational and research purposes. Please respect the terms of service of the websites being scraped.

## 🙏 Acknowledgments

- **GasBuddy** for fuel price data
- **AAA** for gas price averages
- **MarketWatch** for futures data
- **Trading Economics** for EIA data
- **Streamlit** for the dashboard framework
- **DuckDB** for fast data storage
