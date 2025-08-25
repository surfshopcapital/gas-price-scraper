# 🚀 Railway Deployment Checklist

## ✅ Pre-Deployment Checklist

### 1. Files Updated ✅
- [x] `gas_scraper.py` - Enhanced with Railway-optimized Chrome options
- [x] `requirements.txt` - All dependencies properly specified
- [x] `nixpacks.toml` - Railway build configuration for scraper service
- [x] `nixpacks_dashboard.toml` - Railway build configuration for dashboard service
- [x] `railway.json` - Railway configuration for scraper service
- [x] `railway_dashboard.json` - Railway configuration for dashboard service
- [x] `run_scraper.py` - Entry point for scraper service

### 2. Railway Configuration ✅
- [x] **Scraper Service**: Uses `railway.json` + `nixpacks.toml`
- [x] **Dashboard Service**: Uses `railway_dashboard.json` + `nixpacks_dashboard.toml`
- [x] Both use `NIXPACKS` builder
- [x] Scraper includes `chromium` and `chromedriver` packages
- [x] Dashboard uses lightweight configuration
- [x] Proper Python 3.11 environment
- [x] PostgreSQL development headers included (scraper only)

### 3. Chrome Options for Railway ✅
- [x] `--no-sandbox` - Required for Railway
- [x] `--disable-dev-shm-usage` - Memory optimization
- [x] `--single-process` - Resource optimization
- [x] `--disable-web-security` - Railway compatibility
- [x] Additional stability options added

## 🚀 Deployment Instructions

### Two-Service Setup (Current Configuration)
You have **two separate Railway services** from the same repository:

#### Service 1: Gas Scraper (Background Service)
- **Configuration**: `railway.json` + `nixpacks.toml`
- **Purpose**: Runs continuously, scraping gas price data
- **Start Command**: `python run_scraper.py`
- **Dependencies**: Chrome, PostgreSQL, all Python packages

#### Service 2: Web Dashboard (Web Service)
- **Configuration**: `railway_dashboard.json` + `nixpacks_dashboard.toml`
- **Purpose**: Provides web interface for viewing scraped data
- **Start Command**: `streamlit run dashboard.py`
- **Dependencies**: Lightweight, no Chrome needed

### Deployment Steps
1. **Deploy Scraper Service**:
   - Railway will use `railway.json` → `nixpacks.toml`
   - Service runs in background, scraping data continuously

2. **Deploy Dashboard Service**:
   - Railway will use `railway_dashboard.json` → `nixpacks_dashboard.toml`
   - Service provides web interface accessible via URL

## 🔧 Environment Variables Required

### Scraper Service
- `DATABASE_URL` - Your PostgreSQL connection string

### Dashboard Service
- `PORT` - Railway will set this automatically
- `DATABASE_URL` - Same database for viewing data

## 📊 Monitoring Deployment

### Scraper Service
1. **Check Build Logs**: Ensure Chrome and all packages install correctly
2. **Check Runtime Logs**: Look for Chrome startup messages
3. **Verify Database Connection**: Should see "Connected to PostgreSQL database"
4. **Monitor Scraping Jobs**: Should see job execution logs

### Dashboard Service
1. **Check Build Logs**: Ensure Python packages install correctly
2. **Check Runtime Logs**: Look for Streamlit startup messages
3. **Verify Web Access**: Should be accessible via Railway URL
4. **Test Database Connection**: Should display scraped data

## 🚨 Common Issues & Solutions

### Chrome Crashes (Scraper Service)
- ✅ Already fixed with enhanced Chrome options
- ✅ Single-process mode for Railway compatibility

### DevTools Disconnection (Scraper Service)
- ✅ Already fixed with fresh driver per job strategy
- ✅ Enhanced error handling and retry logic

### Memory Issues (Scraper Service)
- ✅ Memory pressure disabled
- ✅ Old space size increased to 4GB
- ✅ Single process mode for stability

### Dashboard Issues
- ✅ Lightweight configuration for web service
- ✅ No Chrome dependencies
- ✅ Proper Streamlit configuration

## 🎯 Expected Behavior

### After Scraper Deployment:
1. ✅ Database connection established
2. ✅ Chrome driver setup successful
3. ✅ Scraping jobs running on schedule
4. ✅ Data being saved to PostgreSQL
5. ✅ No more DevTools disconnection errors

### After Dashboard Deployment:
1. ✅ Streamlit service started successfully
2. ✅ Web interface accessible via Railway URL
3. ✅ Database connection for viewing data
4. ✅ Real-time data display

## 🔄 Update Process

When you push updates:
1. **Scraper Service**: Railway automatically rebuilds using `railway.json` → `nixpacks.toml`
2. **Dashboard Service**: Railway automatically rebuilds using `railway_dashboard.json` → `nixpacks_dashboard.toml`
3. New Chrome options take effect in scraper
4. Enhanced stability features are active
5. All scraping jobs use fresh driver strategy

## 📝 Notes

- **Scraper Service**: Runs continuously in the background, each scraping job gets a fresh Chrome driver
- **Dashboard Service**: Lightweight web service for viewing scraped data
- **Enhanced Error Handling**: Prevents crashes and DevTools disconnections
- **Railway-Optimized**: Chrome options ensure stability in cloud environment
- **PostgreSQL Connection**: Tested before starting scraper service

Your two-service Railway deployment should now work reliably! 🎉
