import time
import schedule
from datetime import datetime, timezone
# import undetected_chromedriver as uc  # Commented out due to instability
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import random
import psycopg2
import os
import pandas as pd

class GasScraper:
    def __init__(self, db_path="gas_prices.duckdb", headless=True):
        """Initialize the Gas Scraper with DuckDB"""
        self.db_path = db_path
        self.headless = headless
        self.driver = None
        
        # Initialize database
        self.init_database()
        
    def init_database(self):
        """Initialize PostgreSQL database connection (table creation handled by setup_database.py)"""
        try:
            # For Railway deployment, the table is already created by setup_database.py
            # Just verify we can connect to PostgreSQL
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            # Simple test query to verify connection
            cursor.execute("SELECT COUNT(*) FROM gas_prices")
            count = cursor.fetchone()[0]
            print(f"✅ Connected to PostgreSQL database. Found {count} existing records.")
            
            # Table already exists in PostgreSQL
            
            # PostgreSQL uses SERIAL for auto-incrementing
            
            # Add consensus and surprise columns if they don't exist (for existing tables)
            try:
                conn.execute("ALTER TABLE gas_prices ADD COLUMN consensus DOUBLE")
                print("   ✅ Added consensus column")
            except:
                pass  # Column already exists
                
            try:
                conn.execute("ALTER TABLE gas_prices ADD COLUMN surprise DOUBLE")
                print("   ✅ Added surprise column")
            except:
                pass  # Column already exists
            
            conn.close()
            print(f"✅ Database initialized: {self.db_path}")
            
        except Exception as e:
            print(f"❌ Error initializing database: {e}")
    
    def cleanup_chrome_processes(self):
        """Force cleanup of any lingering Chrome processes"""
        try:
            import subprocess
            import os
            
            # Kill any existing Chrome processes on Windows
            if os.name == 'nt':  # Windows
                try:
                    subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], 
                                 capture_output=True, check=False)
                    subprocess.run(['taskkill', '/f', '/im', 'chromedriver.exe'], 
                                 capture_output=True, check=False)
                    time.sleep(1)  # Wait for processes to be killed
                    print("   🧹 Cleaned up Chrome processes")
                except:
                    pass
        except Exception as e:
            print(f"   ⚠️ Chrome cleanup warning: {e}")
    
    def is_driver_responsive(self, driver):
        """Check if Chrome driver is still responsive and connected to DevTools"""
        try:
            if not driver:
                return False
            
            # Try multiple health checks
            try:
                # Check if driver is still alive
                driver.current_url
                # Check if we can execute a simple command
                driver.execute_script("return document.readyState")
                return True
            except Exception as e:
                if any(error in str(e).lower() for error in [
                    "disconnected", "not connected to devtools", "chrome not reachable",
                    "session deleted", "invalid session id", "chrome failed to start"
                ]):
                    print(f"   ⚠️ Driver health check failed: {e}")
                    return False
                else:
                    # Other errors might be recoverable
                    return True
        except Exception as e:
            print(f"   ⚠️ Driver responsiveness check error: {e}")
            return False
    
    def test_driver_connection(self, driver, max_attempts=3):
        """Test if driver can maintain a stable connection to DevTools"""
        try:
            print("   🔍 Testing driver connection stability...")
            
            for attempt in range(max_attempts):
                try:
                    # Test basic operations
                    driver.execute_script("return navigator.userAgent")
                    driver.execute_script("return document.readyState")
                    
                    # Test navigation to a simple page
                    test_url = "data:text/html,<html><body>Test</body></html>"
                    driver.get(test_url)
                    
                    # Verify we can read the page
                    page_source = driver.page_source
                    if "Test" in page_source:
                        print(f"   ✅ Connection test {attempt + 1} passed")
                        return True
                    else:
                        print(f"   ⚠️ Connection test {attempt + 1} failed - page content mismatch")
                        
                except Exception as e:
                    print(f"   ⚠️ Connection test {attempt + 1} failed: {e}")
                    if attempt < max_attempts - 1:
                        print("   🔄 Retrying connection test...")
                        time.sleep(2)
                        continue
                    else:
                        print("   ❌ All connection tests failed")
                        return False
            
            return False
            
        except Exception as e:
            print(f"   ❌ Connection test error: {e}")
            return False
    
    def setup_chrome_driver(self):
        """Set up Chrome driver with CDP capabilities"""
        try:
            print("🔧 Setting up Chrome driver...")
            
            # Force cleanup of any existing driver
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            # Force cleanup of Chrome processes
            self.cleanup_chrome_processes()
            
            # Add a small delay to ensure cleanup is complete
            time.sleep(2)
            
            options = Options()
            
            if self.headless:
                print("   👻 Running in headless mode")
                options.add_argument('--headless')
            else:
                print("   📱 Running in visible mode")
            
            # Chrome options optimized for both local Windows and Railway deployment
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--disable-features=TranslateUI')
            options.add_argument('--disable-hang-monitor')
            options.add_argument('--disable-prompt-on-repost')
            options.add_argument('--disable-client-side-phishing-detection')
            options.add_argument('--disable-component-extensions-with-background-pages')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-sync')
            options.add_argument('--metrics-recording-only')
            options.add_argument('--no-first-run')
            options.add_argument('--safebrowsing-disable-auto-update')
            options.add_argument('--disable-extensions-except')
            options.add_argument('--disable-component-update')
            options.add_argument('--disable-domain-reliability')
            options.add_argument('--disable-ipc-flooding-protection')
            options.add_argument('--memory-pressure-off')
            options.add_argument('--max_old_space_size=4096')
            options.add_argument('--single-process')
            options.add_argument('--no-zygote')
            options.add_argument('--disable-setuid-sandbox')
            
            # Platform-specific options
            import platform
            if platform.system() == 'Linux':  # Railway environment
                options.add_argument('--disable-web-security')
                options.add_argument('--disable-features=VizDisplayCompositor')
                options.add_argument('--disable-ipc-flooding-protection')
                options.add_argument('--memory-pressure-off')
                options.add_argument('--max_old_space_size=4096')
                options.add_argument('--single-process')
                options.add_argument('--no-zygote')
                options.add_argument('--disable-setuid-sandbox')
                options.add_argument('--disable-background-timer-throttling')
                options.add_argument('--disable-backgrounding-occluded-windows')
                options.add_argument('--disable-renderer-backgrounding')
                options.add_argument('--disable-features=TranslateUI')
                options.add_argument('--disable-ipc-flooding-protection')
                options.add_argument('--disable-hang-monitor')
                options.add_argument('--disable-prompt-on-repost')
                options.add_argument('--disable-client-side-phishing-detection')
                options.add_argument('--disable-component-extensions-with-background-pages')
                options.add_argument('--disable-default-apps')
                options.add_argument('--disable-sync')
                options.add_argument('--metrics-recording-only')
                options.add_argument('--no-first-run')
                options.add_argument('--safebrowsing-disable-auto-update')
                options.add_argument('--disable-extensions-except')
                options.add_argument('--disable-component-update')
                options.add_argument('--disable-domain-reliability')
                options.add_argument('--disable-features=VizDisplayCompositor')
                options.add_argument('--disable-ipc-flooding-protection')
                options.add_argument('--memory-pressure-off')
                options.add_argument('--max_old_space_size=4096')
                options.add_argument('--single-process')
            # Windows-specific options are more conservative to avoid crashes
            
            # options.add_argument('--disable-javascript')  # Commented out - needed for dynamic content
            
            # User agent to look more human
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            print("   🚀 Launching Chrome...")
            
            # Find Chrome binary and set up ChromeDriver for Railway environment
            try:
                print("   🔍 Looking for Chrome binary...")
                
                # Use Python's built-in capabilities instead of system commands
                import os
                import glob
                
                print("   🔍 Using Python-based Chrome detection...")
                
                # Method 1: Try to use Chrome directly without specifying binary location
                try:
                    print("   🔍 Attempting to create Chrome driver without binary specification...")
                    self.driver = webdriver.Chrome(options=options)
                    print("   ✅ Chrome driver created successfully without binary specification")
                    return self.driver
                except Exception as e:
                    print(f"   ⚠️ Direct Chrome creation failed: {e}")
                
                # Method 2: Search for Chrome in common nix store patterns using Python
                print("   🔍 Searching for Chrome in nix store patterns...")
                
                # Common nix store patterns for chromium
                nix_patterns = [
                    "/nix/store/*/chromium*/bin/chromium",
                    "/nix/store/*/chromium*/bin/chromium-browser",
                    "/nix/store/*/chromium*/bin/google-chrome",
                    "/nix/store/*/chromium*/bin/google-chrome-stable"
                ]
                
                chrome_found = False
                for pattern in nix_patterns:
                    try:
                        expanded_paths = glob.glob(pattern)
                        print(f"   🔍 Pattern '{pattern}' expanded to: {expanded_paths}")
                        
                        for expanded_path in expanded_paths:
                            # Check if file exists using Python's os.path
                            if os.path.isfile(expanded_path):
                                print(f"   ✅ Found Chrome binary at: {expanded_path}")
                                options.binary_location = expanded_path
                                chrome_found = True
                                break
                        
                        if chrome_found:
                            break
                    except Exception as e:
                        print(f"   ⚠️ Error with pattern '{pattern}': {e}")
                        continue
                
                # Method 3: Try common Linux paths
                if not chrome_found:
                    print("   🔍 Trying common Linux Chrome paths...")
                    common_paths = [
                        "/usr/bin/chromium",
                        "/usr/bin/chromium-browser",
                        "/usr/bin/google-chrome",
                        "/usr/bin/google-chrome-stable",
                        "/snap/bin/chromium",
                        "/opt/google/chrome/chrome"
                    ]
                    
                    for chrome_path in common_paths:
                        if os.path.isfile(chrome_path):
                            print(f"   ✅ Found Chrome at: {chrome_path}")
                            options.binary_location = chrome_path
                            chrome_found = True
                            break
                
                # Method 4: Try to find chromedriver in PATH
                if not chrome_found:
                    print("   🔍 Trying to find chromedriver in system...")
                    try:
                        # Let Selenium find chromedriver automatically
                        self.driver = webdriver.Chrome(options=options)
                        print("   ✅ Chrome driver created with auto-detected chromedriver")
                        return self.driver
                    except Exception as e:
                        print(f"   ⚠️ Auto-detection failed: {e}")
                
                if not chrome_found:
                    print("   ❌ Chrome binary not found in any expected location")
                    
                    # Final fallback: Try webdriver-manager
                    print("   🔄 Trying webdriver-manager as final fallback...")
                    try:
                        from webdriver_manager.chrome import ChromeDriverManager
                        from selenium.webdriver.chrome.service import Service
                        
                        print("   📥 Downloading ChromeDriver automatically...")
                        service = Service(ChromeDriverManager().install())
                        self.driver = webdriver.Chrome(service=service, options=options)
                        print("   ✅ Chrome driver created successfully with webdriver-manager")
                        return self.driver
                    except Exception as e:
                        print(f"   ❌ Webdriver-manager fallback failed: {e}")
                        
                        # Try one more fallback: use system ChromeDriver if available
                        try:
                            print("   🔄 Trying system ChromeDriver as last resort...")
                            self.driver = webdriver.Chrome(options=options)
                            print("   ✅ Chrome driver created with system ChromeDriver")
                            return self.driver
                        except Exception as e2:
                            print(f"   ❌ System ChromeDriver also failed: {e2}")
                            return None
                
                # Create Chrome driver with found binary
                try:
                    self.driver = webdriver.Chrome(options=options)
                    print("   ✅ Chrome driver created successfully")
                except Exception as e:
                    print(f"   ❌ Failed to create Chrome driver with binary: {e}")
                    return None
                
                # Set window size
                self.driver.set_window_size(1920, 1080)
                print("   ✅ Chrome driver setup complete")
                
                return self.driver
                
            except Exception as e:
                print(f"   ❌ ChromeDriver setup failed: {e}")
                return None
            
        except Exception as e:
            print(f"❌ Error setting up Chrome driver: {e}")
            return None

    def create_fresh_chrome_driver(self):
        """Create a completely fresh Chrome driver instance"""
        try:
            print("🔧 Creating fresh Chrome driver...")
            
            # Force cleanup of any existing driver
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            # Force cleanup of Chrome processes
            self.cleanup_chrome_processes()
            
            # Add a small delay to ensure cleanup is complete
            time.sleep(3)
            
            # Create new driver
            return self.setup_chrome_driver()
            
        except Exception as e:
            print(f"❌ Error creating fresh Chrome driver: {e}")
            return None
    
    def get_fresh_driver_for_job(self):
        """Get a fresh Chrome driver for each scraping job to avoid DevTools issues"""
        try:
            print("🔄 Getting fresh driver for new job...")
            
            # Try up to 3 times to get a stable driver
            for attempt in range(3):
                print(f"   🔄 Driver creation attempt {attempt + 1}/3...")
                
                # Always create a fresh driver for each job
                driver = self.create_fresh_chrome_driver()
                
                if driver:
                    print("   ✅ Fresh driver created successfully")
                    
                    # Test the connection before returning
                    if self.test_driver_connection(driver):
                        print("   ✅ Driver connection verified - ready for use")
                        return driver
                    else:
                        print(f"   ⚠️ Driver connection test failed (attempt {attempt + 1})")
                        try:
                            driver.quit()
                        except:
                            pass
                        
                        if attempt < 2:  # Not the last attempt
                            print("   🔄 Retrying with new driver...")
                            time.sleep(3)
                            continue
                        else:
                            print("   ❌ All driver attempts failed connection test")
                            return None
                else:
                    print(f"   ❌ Failed to create fresh driver (attempt {attempt + 1})")
                    if attempt < 2:
                        print("   🔄 Retrying driver creation...")
                        time.sleep(3)
                        continue
                    else:
                        print("   ❌ All driver creation attempts failed")
                        return None
            
            return None
                
        except Exception as e:
            print(f"❌ Error getting fresh driver: {e}")
            return None
    
    def navigate_with_retry(self, driver, url, max_attempts=3):
        """Navigate to URL with retry logic for DevTools disconnections"""
        for attempt in range(max_attempts):
            try:
                print(f"   🚀 Navigation attempt {attempt + 1}/{max_attempts} to {url}")
                
                # Test connection before navigation
                if not self.is_driver_responsive(driver):
                    print(f"   ⚠️ Driver not responsive before navigation (attempt {attempt + 1})")
                    if attempt < max_attempts - 1:
                        print("   🔄 Getting fresh driver for retry...")
                        driver = self.get_fresh_driver_for_job()
                        if not driver:
                            print("   ❌ Failed to get fresh driver")
                            return False
                        time.sleep(2)
                        continue
                    else:
                        print("   ❌ Max navigation retries reached")
                        return False
                
                # Attempt navigation
                driver.get(url)
                
                # Wait for page to load
                time.sleep(5)
                
                # Check if navigation was successful
                if driver.current_url == url or url in driver.current_url:
                    print("   ✅ Navigation successful")
                    return True
                else:
                    print(f"   ⚠️ Navigation redirected to: {driver.current_url}")
                    
            except Exception as e:
                error_msg = str(e).lower()
                if "disconnected" in error_msg or "cannot determine loading status" in error_msg or "unable to receive message" in error_msg:
                    print(f"   ⚠️ Navigation failed due to DevTools disconnection (attempt {attempt + 1}): {e}")
                    if attempt < max_attempts - 1:
                        print("   🔄 Getting fresh driver for retry...")
                        try:
                            driver.quit()
                        except:
                            pass
                        driver = self.get_fresh_driver_for_job()
                        if not driver:
                            print("   ❌ Failed to get fresh driver")
                            return False
                        time.sleep(3)
                        continue
                    else:
                        print("   ❌ Max navigation retries reached")
                        return False
                else:
                    print(f"   ⚠️ Navigation error: {e}")
                    return False
            
    def scrape_gasbuddy(self):
        """Scrape GasBuddy Fuel Insights with fresh driver"""
        driver = None
        try:
            print("🚀 Starting GasBuddy scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Set location permissions and coordinates BEFORE navigating
            print("📍 Setting geolocation permissions and coordinates...")
            
            # US cities with coordinates for randomization
            us_cities = [
                {"name": "New York", "lat": 40.7128, "lng": -74.0060},
                {"name": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
                {"name": "Chicago", "lat": 41.8781, "lng": -87.6298},
                {"name": "Houston", "lat": 29.7604, "lng": -95.3698},
                {"name": "Phoenix", "lat": 33.4484, "lng": -112.0740}
            ]
            
            # Randomly select a US city
            selected_city = random.choice(us_cities)
            print(f"   🏙️ Selected location: {selected_city['name']}")
            
            # Grant geolocation permissions to the target domain FIRST
            try:
                driver.execute_cdp_cmd(
                    "Browser.grantPermissions",
                    {
                        "origin": "https://fuelinsights.gasbuddy.com/",
                        "permissions": ["geolocation"],
                    }
                )
                print("   ✅ Geolocation permissions granted")
            except Exception as e:
                print(f"   ⚠️ Could not grant permissions: {e}")
            
            # Set the geolocation coordinates
            try:
                driver.execute_cdp_cmd(
                    "Emulation.setGeolocationOverride",
                    {
                        "latitude": selected_city['lat'],
                        "longitude": selected_city['lng'],
                        "accuracy": 100,
                    }
                )
                print("   ✅ Geolocation coordinates set")
            except Exception as e:
                print(f"   ⚠️ Could not set coordinates: {e}")
            
            # Small delay to ensure CDP commands are processed
            time.sleep(1)
            
            # Navigate to GasBuddy AFTER setting permissions
            target_url = "https://fuelinsights.gasbuddy.com/"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to GasBuddy after retries")
                return None
            
            # Wait for SPA to load - longer wait since it's an SPA
            print("⏳ Waiting for SPA to load...")
            time.sleep(8)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Try to wait for specific elements to appear
            try:
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                # Wait for the application host to have content
                WebDriverWait(driver, 15).until(
                    lambda driver: len(driver.find_element(By.ID, "applicationHost").text) > 100
                )
                print("   ✅ SPA content loaded")
            except Exception as e:
                print(f"   ⚠️ SPA content may not be fully loaded: {e}")
            
            # Check driver responsiveness again before extraction
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive before data extraction")
                return None
            
            # Extract gas price data
            gas_data = self.extract_gasbuddy_data_from_driver(driver)
            
            if gas_data:
                print("🎉 Successfully extracted GasBuddy data!")
                return gas_data
            else:
                print("❌ Failed to extract GasBuddy data")
                # Try to get page source for debugging
                try:
                    page_source = driver.page_source
                    if "tickingAvgPriceText" in page_source:
                        print("   🔍 Price element ID found in page source")
                    else:
                        print("   ❌ Price element ID not found in page source")
                        print(f"   📄 Page title: {driver.title}")
                        print(f"   🔗 Current URL: {driver.current_url}")
                except Exception as e:
                    print(f"   ⚠️ Could not analyze page: {e}")
                return None
                
        except Exception as e:
            print(f"❌ Error in GasBuddy scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
            
            # Force cleanup of Chrome processes
            self.cleanup_chrome_processes()
    
    def scrape_gasbuddy_with_driver(self, driver):
        """Scrape GasBuddy Fuel Insights using provided driver"""
        try:
            print("🚀 Starting GasBuddy scraping with provided driver...")
            
            # Set location permissions and coordinates BEFORE navigating
            print("📍 Setting geolocation permissions and coordinates...")
            
            # US cities with coordinates for randomization
            us_cities = [
                {"name": "New York", "lat": 40.7128, "lng": -74.0060},
                {"name": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
                {"name": "Chicago", "lat": 41.8781, "lng": -87.6298},
                {"name": "Houston", "lat": 29.7604, "lng": -95.3698},
                {"name": "Phoenix", "lat": 33.4484, "lng": -112.0740}
            ]
            
            # Randomly select a US city
            selected_city = random.choice(us_cities)
            print(f"   🏙️ Selected location: {selected_city['name']}")
            
            # Grant geolocation permissions to the target domain FIRST
            try:
                driver.execute_cdp_cmd(
                    "Browser.grantPermissions",
                    {
                        "origin": "https://fuelinsights.gasbuddy.com/",
                        "permissions": ["geolocation"],
                    }
                )
                print("   ✅ Geolocation permissions granted")
            except Exception as e:
                print(f"   ⚠️ Could not grant permissions: {e}")
            
            # Set the geolocation coordinates
            try:
                driver.execute_cdp_cmd(
                    "Emulation.setGeolocationOverride",
                    {
                        "latitude": selected_city['lat'],
                        "longitude": selected_city['lng'],
                        "accuracy": 100,
                    }
                )
                print("   ✅ Geolocation coordinates set")
            except Exception as e:
                print(f"   ⚠️ Could not set coordinates: {e}")
            
            # Small delay to ensure CDP commands are processed
            time.sleep(1)
            
            # Navigate to GasBuddy AFTER setting permissions
            target_url = "https://fuelinsights.gasbuddy.com/"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for SPA to load - longer wait since it's an SPA
            print("⏳ Waiting for SPA to load...")
            time.sleep(8)
            
            # Try to wait for specific elements to appear
            try:
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                # Wait for the application host to have content
                WebDriverWait(driver, 15).until(
                    lambda driver: len(driver.find_element(By.ID, "applicationHost").text) > 100
                )
                print("   ✅ SPA content loaded")
            except Exception as e:
                print(f"   ⚠️ SPA content may not be fully loaded: {e}")
            
            # Extract gas price data
            gas_data = self.extract_gasbuddy_data_from_driver(driver)
            
            if gas_data:
                print("🎉 Successfully extracted GasBuddy data!")
                return gas_data
            else:
                print("❌ Failed to extract GasBuddy data")
                return None
                
        except Exception as e:
            print(f"❌ Error in GasBuddy scraping: {e}")
            return None
    
    def scrape_aaa(self):
        """Scrape AAA Gas Prices with fresh driver to avoid DevTools issues"""
        driver = None
        try:
            print("🚗 Starting AAA scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Navigate to AAA
            target_url = "https://gasprices.aaa.com/"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to AAA after retries")
                return None
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Try to wait for specific elements to appear
            try:
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                # Wait for the tbody element to have content
                WebDriverWait(driver, 15).until(
                    lambda driver: len(driver.find_element(By.TAG_NAME, "tbody").text) > 50
                )
                print("   ✅ AAA table data loaded")
            except Exception as e:
                print(f"   ⚠️ AAA table may not be fully loaded: {e}")
            
            # Check driver responsiveness again before extraction
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive before data extraction")
                return None
            
            # Extract AAA data
            aaa_data = self.extract_aaa_data_from_driver(driver)
            
            if aaa_data:
                print("🎉 Successfully extracted AAA data!")
                return aaa_data
            else:
                print("❌ Failed to extract AAA data")
                return None
                
        except Exception as e:
            print(f"❌ Error in AAA scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
            
            # Force cleanup of Chrome processes
            self.cleanup_chrome_processes()
    
    def scrape_aaa_with_driver(self, driver):
        """Scrape AAA Gas Prices using provided driver"""
        try:
            print("🚗 Starting AAA scraping with provided driver...")
            
            # Navigate to AAA (simple navigation like GasBuddy)
            target_url = "https://gasprices.aaa.com/"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for page to load (like GasBuddy does)
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Try to wait for specific elements to appear (like GasBuddy does)
            try:
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                # Wait for the tbody element to have content
                WebDriverWait(driver, 15).until(
                    lambda driver: len(driver.find_element(By.TAG_NAME, "tbody").text) > 50
                )
                print("   ✅ AAA table data loaded")
            except Exception as e:
                print(f"   ⚠️ AAA table may not be fully loaded: {e}")
            
            # Extract AAA data
            aaa_data = self.extract_aaa_data_from_driver(driver)
            
            if aaa_data:
                print("🎉 Successfully extracted AAA data!")
                return aaa_data
            else:
                print("❌ Failed to extract AAA data")
                return None
                
        except Exception as e:
            print(f"❌ Error in AAA scraping: {e}")
            return None




    
    def set_location_permissions_and_coordinates(self):
        """Set geolocation permissions and coordinates using CDP"""
        try:
            print("📍 Setting geolocation permissions and coordinates...")
            
            # US cities with coordinates for randomization
            us_cities = [
                {"name": "New York", "lat": 40.7128, "lng": -74.0060},
                {"name": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
                {"name": "Chicago", "lat": 41.8781, "lng": -87.6298},
                {"name": "Houston", "lat": 29.7604, "lng": -95.3698},
                {"name": "Phoenix", "lat": 33.4484, "lng": -112.0740}
            ]
            
            # Randomly select a US city
            selected_city = random.choice(us_cities)
            print(f"   🏙️ Selected location: {selected_city['name']}")
            
            # Grant geolocation permissions to the target domain
            self.driver.execute_cdp_cmd(
                "Browser.grantPermissions",
                {
                    "origin": "https://fuelinsights.gasbuddy.com/",
                    "permissions": ["geolocation"],
                }
            )
            
            # Set the geolocation coordinates
            self.driver.execute_cdp_cmd(
                "Emulation.setGeolocationOverride",
                {
                    "latitude": selected_city['lat'],
                    "longitude": selected_city['lng'],
                    "accuracy": 100,
                }
            )
            
            print("   ✅ Location permissions and coordinates set")
            return selected_city
            
        except Exception as e:
            print(f"❌ Error setting location permissions: {e}")
            return None
    
    def wait_for_spa_load(self):
        """Wait for the SPA to fully load and render"""
        try:
            print("⏳ Waiting for SPA to load...")
            
            # Wait for the page to be ready
            WebDriverWait(self.driver, 10).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # Wait for SPA content to start loading
            time.sleep(5)
            
            # Check if applicationHost has content
            app_host = self.driver.find_element(By.ID, "applicationHost")
            app_content = app_host.text.strip()
            
            if len(app_content) > 1000:
                print("   ✅ SPA content loaded successfully")
                return True
            else:
                print("   ⚠️ SPA content still minimal, waiting longer...")
                time.sleep(10)
                
                # Check again
                app_content = app_host.text.strip()
                if len(app_content) > 1000:
                    print("   ✅ SPA content loaded after additional wait")
                    return True
                else:
                    print("   ❌ SPA content failed to load")
                    return False
                    
        except Exception as e:
            print(f"❌ Error waiting for SPA load: {e}")
            return False
    
    def extract_gasbuddy_data(self):
        """Extract gas price data from GasBuddy"""
        try:
            print("🔍 Extracting GasBuddy data...")
            
            # Look for the gas price element
            price_element = self.driver.find_element(By.ID, "tickingAvgPriceText")
            price_text = price_element.text.strip()
            print(f"   ✅ Found gas price: {price_text}")
            
            # Look for timestamp
            timestamp = "Unknown"
            try:
                timestamp_element = self.driver.find_element(By.CSS_SELECTOR, "div[data-bind*='tickingAvgLastUpdated']")
                timestamp = timestamp_element.text.strip()
                print(f"   🕒 Found timestamp: {timestamp}")
            except NoSuchElementException:
                print("   ⚠️ Timestamp element not found")
            
            # Extract price
            try:
                price = float(price_text)
                print(f"   🎯 Successfully extracted price: ${price}")
                return {
                    'price': price,
                    'timestamp': timestamp,
                    'region': 'United States',
                    'source': 'gasbuddy_fuel_insights',
                    'fuel_type': 'regular'
                }
                
            except ValueError:
                print(f"   ❌ Could not convert '{price_text}' to float")
                return None
                
        except NoSuchElementException:
            print("   ❌ Gas price element not found")
            return None
        except Exception as e:
            print(f"❌ Error extracting GasBuddy data: {e}")
            return None
    
    def extract_gasbuddy_data_from_driver(self, driver):
        """Extract gas price data from GasBuddy using provided driver"""
        try:
            print("🔍 Extracting GasBuddy data...")
            
            # Look for the gas price element
            price_element = driver.find_element(By.ID, "tickingAvgPriceText")
            price_text = price_element.text.strip()
            print(f"   ✅ Found gas price: {price_text}")
            
            # Look for timestamp
            timestamp = "Unknown"
            try:
                timestamp_element = driver.find_element(By.CSS_SELECTOR, "div[data-bind*='tickingAvgLastUpdated']")
                timestamp = timestamp_element.text.strip()
                print(f"   🕒 Found timestamp: {timestamp}")
            except NoSuchElementException:
                print("   ⚠️ Timestamp element not found")
            
            # Extract price
            try:
                # Remove '$' and convert to float
                price = float(price_text.replace('$', ''))
                print(f"   🎯 Successfully extracted gas price: ${price}")
                
                return {
                    'price': price,
                    'timestamp': timestamp,
                    'region': 'United States',
                    'source': 'gasbuddy_fuel_insights',
                    'fuel_type': 'regular_gas'
                }
                
            except ValueError:
                print(f"   ❌ Could not convert price '{price_text}' to float")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting GasBuddy data: {e}")
            return None
    
    def extract_aaa_data(self):
        """Extract gas price data from AAA"""
        try:
            print("🔍 Extracting AAA data...")
            
            # Find the table with the price data
            table = self.driver.find_element(By.TAG_NAME, "tbody")
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            current_price = None
            
            # Extract data from each row - only look for Current Avg.
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    period = cells[0].text.strip()
                    regular_price = cells[1].text.strip()
                    
                    if "Current Avg." in period:
                        current_price = regular_price.replace('$', '').strip()
                        print(f"   ✅ Current Avg.: ${current_price}")
                        break  # Only get the first Current Avg. row
            
            if current_price:
                # Save only the current price
                try:
                    current_float = float(current_price)
                    result = {
                        'price': current_float,
                        'timestamp': f"Current as of {datetime.now().strftime('%Y-%m-%d')}",
                        'region': 'United States',
                        'source': 'aaa_gas_prices',
                        'fuel_type': 'regular'
                    }
                    print(f"   🎯 Current price: ${current_float}")
                    return result
                except ValueError:
                    print(f"   ❌ Could not convert current price '{current_price}' to float")
                    return None
            else:
                print("   ❌ Could not find current price")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting AAA data: {e}")
            return None
    
    def extract_aaa_data_from_driver(self, driver):
        """Extract gas price data from AAA using provided driver"""
        try:
            print("🔍 Extracting AAA data...")
            
            # Find the table with the price data
            table = driver.find_element(By.TAG_NAME, "tbody")
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            current_price = None
            
            # Extract data from each row - only look for Current Avg.
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    period = cells[0].text.strip()
                    regular_price = cells[1].text.strip()
                    
                    if "Current Avg." in period:
                        current_price = regular_price.replace('$', '').strip()
                        print(f"   ✅ Current Avg.: ${current_price}")
                        break  # Only get the first Current Avg. row
            
            if current_price:
                # Save only the current price
                try:
                    current_float = float(current_price)
                    result = {
                        'price': current_float,
                        'timestamp': f"Current as of {datetime.now().strftime('%Y-%m-%d')}",
                        'region': 'United States',
                        'source': 'aaa_gas_prices',
                        'fuel_type': 'regular'
                    }
                    print(f"   🎯 Current price: ${current_float}")
                    return result
                except ValueError:
                    print(f"   ❌ Could not convert current price '{current_price}' to float")
                    return None
            else:
                print("   ❌ Could not find current price")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting AAA data: {e}")
            return None
    
    def scrape_rbob(self):
        """Scrape RBOB Futures from MarketWatch with fresh driver"""
        driver = None
        try:
            print("⛽ Starting RBOB futures scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Navigate to MarketWatch RBOB futures
            target_url = "https://www.marketwatch.com/investing/future/rb.1"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to RBOB after retries")
                return None
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(5)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Extract RBOB data
            rbob_data = self.extract_rbob_data(driver)
            
            if rbob_data:
                print("🎉 Successfully extracted RBOB data!")
                return rbob_data
            else:
                print("❌ Failed to extract RBOB data")
                return None
                
        except Exception as e:
            print(f"❌ Error in RBOB scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
            
            # Force cleanup of Chrome processes
            self.cleanup_chrome_processes()
    
    def scrape_rbob_with_driver(self, driver):
        """Scrape RBOB Futures from MarketWatch using provided driver"""
        try:
            print("⛽ Starting RBOB futures scraping with provided driver...")
            
            # Navigate to MarketWatch RBOB futures
            target_url = "https://www.marketwatch.com/investing/future/rb.1"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(5)
            
            # Extract RBOB data
            rbob_data = self.extract_rbob_data(driver)
            
            if rbob_data:
                print("🎉 Successfully extracted RBOB data!")
                return rbob_data
            else:
                print("❌ Failed to extract RBOB data")
                return None
                
        except Exception as e:
            print(f"❌ Error in RBOB scraping: {e}")
            return None
    
    def extract_rbob_data(self, driver=None):
        """Extract RBOB futures data from MarketWatch"""
        try:
            print("🔍 Extracting RBOB futures data...")
            
            # Use provided driver or fall back to self.driver
            target_driver = driver if driver else self.driver
            if not target_driver:
                print("   ❌ No driver available for extraction")
                return None
            
            # Try multiple selectors for the price element (MarketWatch has changed their structure)
            price_selectors = [
                "bg-quote.value",  # Most precise selector from current HTML
                "h2.intraday__price bg-quote.value",
                "h2.intraday__price .value",
                ".intraday__price .value",
                ".intraday__price",
                "h2.intraday__price",
                ".value",
                "[data-testid='price']",
                ".price"
            ]
            
            price_element = None
            price_text = None
            
            for selector in price_selectors:
                try:
                    price_element = target_driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_element.text.strip()
                    if price_text and price_text != "":
                        print(f"   ✅ Found RBOB price using selector '{selector}': {price_text}")
                        break
                except:
                    continue
            
            if not price_text:
                print("   ❌ Could not find RBOB price with any selector")
                # Try to get page source for debugging
                try:
                    page_source = self.driver.page_source
                    if "RBOB" in page_source:
                        print("   🔍 RBOB content found in page source")
                    else:
                        print("   🔍 RBOB content not found in page source")
                except:
                    pass
                return None
            
            # Try multiple selectors for the timestamp element
            timestamp_selectors = [
                "span.timestamp__time",
                ".intraday__timestamp",
                ".timestamp",
                "[data-testid='timestamp']",
                ".last-updated"
            ]
            
            timestamp_element = None
            timestamp_text = None
            
            for selector in timestamp_selectors:
                try:
                    timestamp_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    timestamp_text = timestamp_element.text.strip()
                    if timestamp_text and timestamp_text != "":
                        print(f"   🕒 Found timestamp using selector '{selector}': {timestamp_text}")
                        break
                except:
                    continue
            
            if not timestamp_text:
                timestamp_text = "Unknown"
                print("   ⚠️ Could not find timestamp, using default")
            
            # Extract price
            try:
                # Clean up price text (remove any non-numeric characters except decimal point)
                clean_price = ''.join(c for c in price_text if c.isdigit() or c == '.')
                price = float(clean_price)
                print(f"   🎯 Successfully extracted RBOB price: ${price}")
                
                # Clean up timestamp text
                timestamp = timestamp_text.replace("Last Updated: ", "").replace("Last Updated:", "").strip()
                
                return {
                    'price': price,
                    'timestamp': timestamp,
                    'region': 'United States',
                    'source': 'marketwatch_rbob_futures',
                    'fuel_type': 'rbob_futures'
                }
                
            except ValueError:
                print(f"   ❌ Could not convert RBOB price '{price_text}' to float")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting RBOB data: {e}")
            return None
    
    def scrape_wti(self):
        """Scrape WTI Crude Oil Futures from MarketWatch with fresh driver"""
        driver = None
        try:
            print("🛢️ Starting WTI crude oil futures scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Navigate to MarketWatch WTI futures
            target_url = "https://www.marketwatch.com/investing/future/cl.1"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to WTI after retries")
                return None
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(5)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Extract WTI data
            wti_data = self.extract_wti_data(driver)
            
            if wti_data:
                print("🎉 Successfully extracted WTI data!")
                return wti_data
            else:
                print("❌ Failed to extract WTI data")
                return None
                
        except Exception as e:
            print(f"❌ Error in WTI scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
    
    def scrape_wti_with_driver(self, driver):
        """Scrape WTI Crude Oil Futures from MarketWatch using provided driver"""
        try:
            print("🛢️ Starting WTI crude oil futures scraping with provided driver...")
            
            # Navigate to MarketWatch WTI futures
            target_url = "https://www.marketwatch.com/investing/future/cl.1"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(5)
            
            # Extract WTI data
            wti_data = self.extract_wti_data(driver)
            
            if wti_data:
                print("🎉 Successfully extracted WTI data!")
                return wti_data
            else:
                print("❌ Failed to extract WTI data")
                return None
                
        except Exception as e:
            print(f"❌ Error in WTI scraping: {e}")
            return None
    
    def extract_wti_data(self, driver=None):
        """Extract WTI crude oil futures data from MarketWatch"""
        try:
            print("🔍 Extracting WTI crude oil futures data...")
            
            # Use provided driver or fall back to self.driver
            target_driver = driver if driver else self.driver
            if not target_driver:
                print("   ❌ No driver available for extraction")
                return None
            
            # Try multiple selectors for the price element (MarketWatch has changed their structure)
            price_selectors = [
                "bg-quote.value",  # Most precise selector from current HTML
                "h2.intraday__price bg-quote.value",
                "h2.intraday__price .value",
                ".intraday__price .value",
                ".intraday__price",
                "h2.intraday__price",
                ".value",
                "[data-testid='price']",
                ".price"
            ]
            
            price_element = None
            price_text = None
            
            for selector in price_selectors:
                try:
                    price_element = target_driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_element.text.strip()
                    if price_text and price_text != "":
                        print(f"   ✅ Found WTI price using selector '{selector}': {price_text}")
                        break
                except:
                    continue
            
            if not price_text:
                print("   ❌ Could not find WTI price with any selector")
                # Try to get page source for debugging
                try:
                    page_source = self.driver.page_source
                    if "WTI" in page_source or "Crude Oil" in page_source:
                        print("   🔍 WTI content found in page source")
                    else:
                        print("   🔍 WTI content not found in page source")
                except:
                    pass
                return None
            
            # Try multiple selectors for the timestamp element
            timestamp_selectors = [
                "span.timestamp__time",
                ".intraday__timestamp",
                ".timestamp",
                "[data-testid='timestamp']",
                ".last-updated"
            ]
            
            timestamp_element = None
            timestamp_text = None
            
            for selector in timestamp_selectors:
                try:
                    timestamp_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    timestamp_text = timestamp_element.text.strip()
                    if timestamp_text and timestamp_text != "":
                        print(f"   🕒 Found timestamp using selector '{selector}': {timestamp_text}")
                        break
                except:
                    continue
            
            if not timestamp_text:
                timestamp_text = "Unknown"
                print("   ⚠️ Could not find timestamp, using default")
            
            # Extract price
            try:
                # Clean up price text (remove any non-numeric characters except decimal point)
                clean_price = ''.join(c for c in price_text if c.isdigit() or c == '.')
                price = float(clean_price)
                print(f"   🎯 Successfully extracted WTI price: ${price}")
                
                # Clean up timestamp text
                timestamp = timestamp_text.replace("Last Updated: ", "").replace("Last Updated:", "").strip()
                
                return {
                    'price': price,
                    'timestamp': timestamp,
                    'region': 'United States',
                    'source': 'marketwatch_wti_futures',
                    'fuel_type': 'wti_futures'
                }
                
            except ValueError:
                print(f"   ❌ Could not convert WTI price '{price_text}' to float")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting WTI data: {e}")
            return None
    
    def scrape_gasoline_stocks(self):
        """Scrape Gasoline Stocks Change from Trading Economics with fresh driver"""
        driver = None
        try:
            print("⛽ Starting Gasoline Stocks Change scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Navigate to Trading Economics Gasoline Stocks Change
            target_url = "https://tradingeconomics.com/united-states/gasoline-stocks-change"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to Gasoline Stocks after retries")
                return None
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Extract Gasoline Stocks data
            stocks_data = self.extract_gasoline_stocks_data(driver)
            
            if stocks_data:
                print("🎉 Successfully extracted Gasoline Stocks data!")
                return stocks_data
            else:
                print("❌ Failed to extract Gasoline Stocks data")
                return None
                
        except Exception as e:
            print(f"❌ Error in Gasoline Stocks scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
    
    def scrape_gasoline_stocks_with_driver(self, driver):
        """Scrape Gasoline Stocks Change from Trading Economics using provided driver"""
        try:
            print("⛽ Starting Gasoline Stocks Change scraping with provided driver...")
            
            # Navigate to Trading Economics Gasoline Stocks Change
            target_url = "https://tradingeconomics.com/united-states/gasoline-stocks-change"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Extract Gasoline Stocks data
            stocks_data = self.extract_gasoline_stocks_data(driver)
            
            if stocks_data:
                print("🎉 Successfully extracted Gasoline Stocks data!")
                return stocks_data
            else:
                print("❌ Failed to extract Gasoline Stocks data")
                return None
                
        except Exception as e:
            print(f"❌ Error in Gasoline Stocks scraping: {e}")
            return None
    
    def extract_gasoline_stocks_data(self, driver=None):
        """Extract Gasoline Stocks Change data from Trading Economics"""
        try:
            print("🔍 Extracting Gasoline Stocks Change data...")
            
            # Use provided driver or fall back to self.driver
            target_driver = driver if driver else self.driver
            if not target_driver:
                print("   ❌ No driver available for extraction")
                return None
            
            # Find all rows with actual data
            actual_elements = target_driver.find_elements(By.CSS_SELECTOR, "td#actual")
            
            if not actual_elements:
                print("   ❌ No actual data elements found")
                return None
            
            # Find the most recent date with actual data
            latest_date = None
            latest_row = None
            latest_actual = None
            latest_consensus = None
            
            for actual_element in actual_elements:
                row = actual_element.find_element(By.XPATH, "./..")  # Go to parent tr
                date_element = row.find_element(By.XPATH, "./td[1]")
                date_text = date_element.text.strip()
                
                # Skip if no actual value
                actual_text = actual_element.text.strip()
                if not actual_text or actual_text == '':
                    continue
                
                # Parse date (format: YYYY-MM-DD)
                try:
                    date_parts = date_text.split('-')
                    if len(date_parts) == 3:
                        year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                        current_date = datetime(year, month, day).date()
                        
                        if latest_date is None or current_date > latest_date:
                            latest_date = current_date
                            latest_row = row
                            latest_actual = actual_text
                            
                            # Get consensus from the same row (7th column)
                            consensus_element = row.find_element(By.XPATH, "./td[7]")
                            latest_consensus = consensus_element.text.strip()
                            
                except (ValueError, IndexError) as e:
                    print(f"   ⚠️ Could not parse date '{date_text}': {e}")
                    continue
            
            if latest_date is None:
                print("   ❌ No valid dates with actual data found")
                return None
            
            print(f"   📅 Found latest date: {latest_date}")
            print(f"   ✅ Found actual value: {latest_actual}")
            print(f"   📊 Found consensus: {latest_consensus}")
            
            # Extract numeric values
            try:
                # Remove 'M' suffix and convert to float
                actual_value = float(latest_actual.replace('M', ''))
                consensus_value = float(latest_consensus.replace('M', '')) if latest_consensus and latest_consensus != '' else 0.0
                
                # Calculate surprise
                surprise = actual_value - consensus_value
                
                print(f"   🎯 Actual: {actual_value}M")
                print(f"   🎯 Consensus: {consensus_value}M")
                print(f"   🎯 Surprise: {surprise}M")
                
                return {
                    'price': actual_value,
                    'timestamp': f"{latest_date.strftime('%Y-%m-%d')}",
                    'region': 'United States',
                    'source': 'tradingeconomics_gasoline_stocks',
                    'fuel_type': 'gasoline_stocks_change',
                    'consensus': consensus_value,
                    'surprise': surprise
                }
                
            except ValueError as e:
                print(f"   ❌ Could not convert values to float: {e}")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting Gasoline Stocks data: {e}")
            return None
    
    def scrape_refinery_runs(self):
        """Scrape Refinery Crude Runs from Trading Economics with fresh driver"""
        driver = None
        try:
            print("🏭 Starting Refinery Crude Runs scraping...")
            
            # Get a fresh driver for this job to avoid DevTools disconnection
            driver = self.get_fresh_driver_for_job()
            if not driver:
                print("❌ Failed to get fresh Chrome driver")
                return None
            
            # Navigate to Trading Economics Refinery Crude Runs
            target_url = "https://tradingeconomics.com/united-states/refinery-crude-runs"
            print(f"🌐 Navigating to: {target_url}")
            
            # Use navigation with retry logic
            if not self.navigate_with_retry(driver, target_url):
                print("❌ Failed to navigate to Refinery Runs after retries")
                return None
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Check if driver is still responsive before proceeding
            if not self.is_driver_responsive(driver):
                print("❌ Driver became unresponsive after navigation")
                return None
            
            # Extract Refinery Runs data
            runs_data = self.extract_refinery_runs_data(driver)
            
            if runs_data:
                print("🎉 Successfully extracted Refinery Crude Runs data!")
                return runs_data
            else:
                print("❌ Failed to extract Refinery Crude Runs data")
                return None
                
        except Exception as e:
            print(f"❌ Error in Refinery Runs scraping: {e}")
            return None
            
        finally:
            # Always clean up the driver
            if driver:
                print("🔒 Closing Chrome driver...")
                try:
                    driver.quit()
                except Exception as e:
                    print(f"⚠️ Warning during driver cleanup: {e}")
                self.driver = None
    
    def scrape_refinery_runs_with_driver(self, driver):
        """Scrape Refinery Crude Runs from Trading Economics using provided driver"""
        try:
            print("🏭 Starting Refinery Crude Runs scraping with provided driver...")
            
            # Navigate to Trading Economics Refinery Crude Runs
            target_url = "https://tradingeconomics.com/united-states/refinery-crude-runs"
            print(f"🌐 Navigating to: {target_url}")
            driver.get(target_url)
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            time.sleep(8)
            
            # Extract Refinery Runs data
            runs_data = self.extract_refinery_runs_data(driver)
            
            if runs_data:
                print("🎉 Successfully extracted Refinery Crude Runs data!")
                return runs_data
            else:
                print("❌ Failed to extract Refinery Crude Runs data")
                return None
                
        except Exception as e:
            print(f"❌ Error in Refinery Runs scraping: {e}")
            return None
    
    def extract_refinery_runs_data(self, driver=None):
        """Extract Refinery Crude Runs data from Trading Economics"""
        try:
            print("🔍 Extracting Refinery Crude Runs data...")
            
            # Use provided driver or fall back to self.driver
            target_driver = driver if driver else self.driver
            if not target_driver:
                print("   ❌ No driver available for extraction")
                return None
            
            # Find all rows with actual data
            actual_elements = target_driver.find_elements(By.CSS_SELECTOR, "td#actual")
            
            if not actual_elements:
                print("   ❌ No actual data elements found")
                return None
            
            # Find the most recent date with actual data
            latest_date = None
            latest_actual = None
            
            for actual_element in actual_elements:
                row = actual_element.find_element(By.XPATH, "./..")  # Go to parent tr
                date_element = row.find_element(By.XPATH, "./td[1]")
                date_text = date_element.text.strip()
                
                # Skip if no actual value
                actual_text = actual_element.text.strip()
                if not actual_text or actual_text == '':
                    continue
                
                # Parse date (format: YYYY-MM-DD)
                try:
                    date_parts = date_text.split('-')
                    if len(date_parts) == 3:
                        year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                        current_date = datetime(year, month, day).date()
                        
                        if latest_date is None or current_date > latest_date:
                            latest_date = current_date
                            latest_actual = actual_text
                            
                except (ValueError, IndexError) as e:
                    print(f"   ⚠️ Could not parse date '{date_text}': {e}")
                    continue
            
            if latest_date is None:
                print("   ❌ No valid dates with actual data found")
                return None
            
            print(f"   📅 Found latest date: {latest_date}")
            print(f"   ✅ Found actual value: {latest_actual}")
            
            # Extract numeric value
            try:
                # Remove 'M' suffix and convert to float
                actual_value = float(latest_actual.replace('M', ''))
                
                print(f"   🎯 Actual: {actual_value}M")
                
                return {
                    'price': actual_value,
                    'timestamp': f"{latest_date.strftime('%Y-%m-%d')}",
                    'region': 'United States',
                    'source': 'tradingeconomics_refinery_runs',
                    'fuel_type': 'refinery_crude_runs'
                }
                
            except ValueError as e:
                print(f"   ❌ Could not convert value to float: {e}")
                return None
                
        except Exception as e:
            print(f"❌ Error extracting Refinery Runs data: {e}")
            return None
    
    def save_to_database(self, gas_data_list):
        """Save gas price data to PostgreSQL"""
        if not gas_data_list:
            return False
        
        # Handle single item or list
        if not isinstance(gas_data_list, list):
            gas_data_list = [gas_data_list]
            
        try:
            # Get database connection from environment variables
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            saved_count = 0
            for gas_data in gas_data_list:
                # Check if consensus and surprise exist (for EIA data)
                consensus = gas_data.get('consensus', None)
                surprise = gas_data.get('surprise', None)
                
                # Insert the data with auto-incrementing ID (PostgreSQL uses SERIAL)
                cursor.execute('''
                    INSERT INTO gas_prices (price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ''', (gas_data['price'], gas_data['timestamp'], gas_data['region'], gas_data['source'], gas_data['fuel_type'], consensus, surprise))
                
                saved_count += 1
                print(f"   ✅ Saved: ${gas_data['price']} from {gas_data['source']}")
                if consensus is not None and surprise is not None:
                    print(f"      📊 Consensus: {consensus}M, Surprise: {surprise}M")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✅ Successfully saved {saved_count} records to database")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            return False
    
    def run_gasbuddy_job(self):
        """Run GasBuddy scraping job"""
        print(f"\n--- Starting GasBuddy scraping job at {datetime.now()} ---")
        
        gas_data = self.scrape_gasbuddy()
        if gas_data:
            self.save_to_database(gas_data)
            print(f"🎯 GasBuddy: ${gas_data['price']} at {gas_data['timestamp']}")
        else:
            print("❌ Failed to scrape GasBuddy data")
    
    def run_gasbuddy_job_with_driver(self, driver):
        """Run GasBuddy scraping job using provided driver"""
        print(f"\n--- Starting GasBuddy scraping job at {datetime.now()} ---")
        
        gas_data = self.scrape_gasbuddy_with_driver(driver)
        if gas_data:
            self.save_to_database(gas_data)
            print(f"🎯 GasBuddy: ${gas_data['price']} at {gas_data['timestamp']}")
        else:
            print("❌ Failed to scrape GasBuddy data")
    
    def run_aaa_job(self):
        """Run AAA scraping job"""
        print(f"\n--- Starting AAA scraping job at {datetime.now()} ---")
        
        aaa_data = self.scrape_aaa()
        if aaa_data:
            self.save_to_database(aaa_data)
            print(f"🎯 AAA: Extracted current price: ${aaa_data['price']}")
        else:
            print("❌ Failed to scrape AAA data")
    
    def run_aaa_job_with_driver(self, driver):
        """Run AAA scraping job using provided driver"""
        print(f"\n--- Starting AAA scraping job at {datetime.now()} ---")
        
        aaa_data = self.scrape_aaa_with_driver(driver)
        if aaa_data:
            self.save_to_database(aaa_data)
            print(f"🎯 AAA: Extracted current price: ${aaa_data['price']}")
        else:
            print("❌ Failed to scrape AAA data")
    
    def run_rbob_job(self):
        """Run RBOB futures scraping job"""
        print(f"\n--- Starting RBOB futures scraping job at {datetime.now()} ---")
        
        rbob_data = self.scrape_rbob()
        if rbob_data:
            self.save_to_database(rbob_data)
            print(f"🎯 RBOB: ${rbob_data['price']} at {rbob_data['timestamp']}")
        else:
            print("❌ Failed to scrape RBOB data")
    
    def run_rbob_job_with_driver(self, driver):
        """Run RBOB futures scraping job using provided driver"""
        print(f"\n--- Starting RBOB futures scraping job at {datetime.now()} ---")
        
        rbob_data = self.scrape_rbob_with_driver(driver)
        if rbob_data:
            self.save_to_database(rbob_data)
            print(f"🎯 RBOB: ${rbob_data['price']} at {rbob_data['timestamp']}")
        else:
            print("❌ Failed to scrape RBOB data")
    
    def run_wti_job(self):
        """Run WTI crude oil futures scraping job"""
        print(f"\n--- Starting WTI crude oil futures scraping job at {datetime.now()} ---")
        
        wti_data = self.scrape_wti()
        if wti_data:
            self.save_to_database(wti_data)
            print(f"🎯 WTI: ${wti_data['price']} at {wti_data['timestamp']}")
        else:
            print("❌ Failed to scrape WTI data")
    
    def run_wti_job_with_driver(self, driver):
        """Run WTI crude oil futures scraping job using provided driver"""
        print(f"\n--- Starting WTI crude oil futures scraping job at {datetime.now()} ---")
        
        wti_data = self.scrape_wti_with_driver(driver)
        if wti_data:
            self.save_to_database(wti_data)
            print(f"🎯 WTI: ${wti_data['price']} at {wti_data['timestamp']}")
        else:
            print("❌ Failed to scrape WTI data")
    
    def run_gasoline_stocks_job(self):
        """Run Gasoline Stocks Change scraping job"""
        print(f"\n--- Starting Gasoline Stocks Change scraping job at {datetime.now()} ---")
        
        stocks_data = self.scrape_gasoline_stocks()
        if stocks_data:
            self.save_to_database(stocks_data)
            print(f"🎯 Gasoline Stocks: {stocks_data['price']}M at {stocks_data['timestamp']}")
        else:
            print("❌ Failed to scrape Gasoline Stocks data")
    
    def run_gasoline_stocks_job_with_driver(self, driver):
        """Run Gasoline Stocks Change scraping job using provided driver"""
        print(f"\n--- Starting Gasoline Stocks Change scraping job at {datetime.now()} ---")
        
        stocks_data = self.scrape_gasoline_stocks_with_driver(driver)
        if stocks_data:
            self.save_to_database(stocks_data)
            print(f"🎯 Gasoline Stocks: {stocks_data['price']}M at {stocks_data['timestamp']}")
        else:
            print("❌ Failed to scrape Gasoline Stocks data")
    
    def run_refinery_runs_job(self):
        """Run Refinery Crude Runs scraping job"""
        print(f"\n--- Starting Refinery Crude Runs scraping job at {datetime.now()} ---")
        
        runs_data = self.scrape_refinery_runs()
        if runs_data:
            self.save_to_database(runs_data)
            print(f"🎯 Refinery Runs: {runs_data['price']}M at {runs_data['timestamp']}")
        else:
            print("❌ Failed to scrape Refinery Runs data")
    
    def run_refinery_runs_job_with_driver(self, driver):
        """Run Refinery Crude Runs scraping job using provided driver"""
        print(f"\n--- Starting Refinery Crude Runs scraping job at {datetime.now()} ---")
        
        runs_data = self.scrape_refinery_runs_with_driver(driver)
        if runs_data:
            self.save_to_database(runs_data)
            print(f"🎯 Refinery Runs: {runs_data['price']}M at {runs_data['timestamp']}")
        else:
            print("❌ Failed to scrape Refinery Runs data")
    
    def run_daily_backup(self):
        """Run daily database backup"""
        print(f"\n--- Starting daily database backup at {datetime.now()} ---")
        
        try:
            # Import and run backup script
            from backup_database import main as backup_main
            backup_main()
            print("✅ Daily backup completed successfully")
        except Exception as e:
            print(f"❌ Daily backup failed: {e}")
    
    def run_all_sources_once(self):
        """Run all data sources once immediately using fresh driver for each job"""
        print(f"\n--- Running all sources once at {datetime.now()} ---")
        
        try:
            print("🔄 Running all sources with fresh driver for each job...")
            
            # Run all jobs using fresh drivers (our new strategy)
            self.run_gasbuddy_job()
            time.sleep(2)  # Short delay between jobs
            
            self.run_aaa_job()
            time.sleep(2)  # Short delay between jobs
            
            self.run_rbob_job()
            time.sleep(2)  # Short delay between jobs
            
            self.run_wti_job()
            time.sleep(2)  # Short delay between jobs
            
            self.run_gasoline_stocks_job()
            time.sleep(2)  # Short delay between jobs
            
            self.run_refinery_runs_job()
            
            print("✅ All sources completed successfully with fresh drivers!")
            
        except Exception as e:
            print(f"❌ Error running all sources: {e}")
    
    def get_latest_prices(self, limit=20):
        """Retrieve the latest gas prices from the database"""
        try:
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                ORDER BY scraped_at DESC
                LIMIT %s
            ''', (limit,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results
            
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return []
    
    def export_daily_excel(self):
        """Export today's data to Excel with 3 tabs (GasBuddy, AAA, RBOB)"""
        try:
            print("📊 Exporting daily data to Excel...")
            
            # Get today's date
            today = datetime.now().date()
            
            # Connect to database
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            
            # Get data for each source for today
            gasbuddy_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'gasbuddy_fuel_insights'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            aaa_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'aaa_gas_prices'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            rbob_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_rbob_futures'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            wti_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_wti_futures'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            gasoline_stocks_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_gasoline_stocks'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            refinery_runs_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_refinery_runs'
                AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
            ''', (today,)).fetchall()
            
            conn.close()
            
            # Create Excel file with 6 tabs
            filename = f"gas_prices_daily_{today.strftime('%Y%m%d')}.xlsx"
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # GasBuddy tab
                if gasbuddy_data:
                    gasbuddy_df = pd.DataFrame(gasbuddy_data, 
                                             columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    gasbuddy_df.to_excel(writer, sheet_name='GasBuddy', index=False)
                    print(f"   ✅ GasBuddy: {len(gasbuddy_data)} records")
                else:
                    # Create empty DataFrame with headers
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='GasBuddy', index=False)
                    print("   ⚠️ GasBuddy: No data for today")
                
                # AAA tab
                if aaa_data:
                    aaa_df = pd.DataFrame(aaa_data, 
                                        columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    aaa_df.to_excel(writer, sheet_name='AAA', index=False)
                    print(f"   ✅ AAA: {len(aaa_data)} record")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='AAA', index=False)
                    print("   ⚠️ AAA: No data for today")
                
                # RBOB tab
                if rbob_data:
                    rbob_df = pd.DataFrame(rbob_data, 
                                         columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    rbob_df.to_excel(writer, sheet_name='RBOB', index=False)
                    print(f"   ✅ RBOB: {len(rbob_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='RBOB', index=False)
                    print("   ⚠️ RBOB: No data for today")
                
                # WTI tab
                if wti_data:
                    wti_df = pd.DataFrame(wti_data, 
                                        columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    wti_df.to_excel(writer, sheet_name='WTI', index=False)
                    print(f"   ✅ WTI: {len(wti_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='WTI', index=False)
                    print("   ⚠️ WTI: No data for today")
                
                # Gasoline Stocks tab
                if gasoline_stocks_data:
                    stocks_df = pd.DataFrame(gasoline_stocks_data, 
                                           columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'consensus', 'surprise', 'scraped_at'])
                    stocks_df.to_excel(writer, sheet_name='Gasoline_Stocks', index=False)
                    print(f"   ✅ Gasoline Stocks: {len(gasoline_stocks_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'consensus', 'surprise', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='Gasoline_Stocks', index=False)
                    print("   ⚠️ Gasoline Stocks: No data for today")
                
                # Refinery Runs tab
                if refinery_runs_data:
                    runs_df = pd.DataFrame(refinery_runs_data, 
                                         columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    runs_df.to_excel(writer, sheet_name='Refinery_Runs', index=False)
                    print(f"   ✅ Refinery Runs: {len(refinery_runs_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='Refinery_Runs', index=False)
                    print("   ⚠️ Refinery Runs: No data for today")
            
            print(f"✅ Daily Excel export completed: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting daily Excel: {e}")
            return None
    
    def check_and_export_monthly(self):
        """Check if it's the 1st of the month and export monthly Excel if so"""
        now = datetime.now()
        if now.day == 1:
            print("📅 First day of month detected - starting monthly Excel export...")
            self.export_monthly_excel()
        else:
            print(f"📅 Not first day of month (current day: {now.day}) - skipping monthly export")
    
    def export_monthly_excel(self):
        """Export current month's data to Excel with 3 tabs"""
        try:
            print("📊 Exporting monthly data to Excel...")
            
            # Get current month
            now = datetime.now()
            current_month = now.replace(day=1).date()
            
            # Connect to database
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            
            # Get data for each source for current month
            gasbuddy_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'gasbuddy_fuel_insights'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            aaa_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'aaa_gas_prices'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            rbob_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_rbob_futures'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            wti_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'marketwatch_wti_futures'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            gasoline_stocks_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_gasoline_stocks'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            refinery_runs_data = conn.execute('''
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_refinery_runs'
                AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
            ''', (current_month,)).fetchall()
            
            conn.close()
            
            # Create Excel file with 6 tabs
            filename = f"gas_prices_monthly_{now.strftime('%Y%m')}.xlsx"
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # GasBuddy tab
                if gasbuddy_data:
                    gasbuddy_df = pd.DataFrame(gasbuddy_data, 
                                             columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    gasbuddy_df.to_excel(writer, sheet_name='GasBuddy', index=False)
                    print(f"   ✅ GasBuddy: {len(gasbuddy_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='GasBuddy', index=False)
                    print("   ⚠️ GasBuddy: No data for this month")
                
                # AAA tab
                if aaa_data:
                    aaa_df = pd.DataFrame(aaa_data, 
                                        columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    aaa_df.to_excel(writer, sheet_name='AAA', index=False)
                    print(f"   ✅ AAA: {len(aaa_data)} record")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='AAA', index=False)
                    print("   ⚠️ AAA: No data for this month")
                
                # RBOB tab
                if rbob_data:
                    rbob_df = pd.DataFrame(rbob_data, 
                                         columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    rbob_df.to_excel(writer, sheet_name='RBOB', index=False)
                    print(f"   ✅ RBOB: {len(rbob_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='RBOB', index=False)
                    print("   ⚠️ RBOB: No data for this month")
                
                # WTI tab
                if wti_data:
                    wti_df = pd.DataFrame(wti_data, 
                                        columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    wti_df.to_excel(writer, sheet_name='WTI', index=False)
                    print(f"   ✅ WTI: {len(wti_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='WTI', index=False)
                    print("   ⚠️ WTI: No data for this month")
                
                # Gasoline Stocks tab
                if gasoline_stocks_data:
                    stocks_df = pd.DataFrame(gasoline_stocks_data, 
                                           columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'consensus', 'surprise', 'scraped_at'])
                    stocks_df.to_excel(writer, sheet_name='Gasoline_Stocks', index=False)
                    print(f"   ✅ Gasoline Stocks: {len(gasoline_stocks_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'consensus', 'surprise', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='Gasoline_Stocks', index=False)
                    print("   ⚠️ Gasoline Stocks: No data for this month")
                
                # Refinery Runs tab
                if refinery_runs_data:
                    runs_df = pd.DataFrame(refinery_runs_data, 
                                         columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    runs_df.to_excel(writer, sheet_name='Refinery_Runs', index=False)
                    print(f"   ✅ Refinery Runs: {len(refinery_runs_data)} records")
                else:
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'source', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name='Refinery_Runs', index=False)
                    print("   ⚠️ Refinery Runs: No data for this month")
            
            print(f"✅ Monthly Excel export completed: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting monthly Excel: {e}")
            return None
    
    def run_scheduled(self):
        """Run the scraper on schedule"""
        print("🚗 Hexa Source Gas Scraper - Scheduled Mode")
        print("=" * 50)
        print("📅 Schedule:")
        print("   • GasBuddy: Every 15 minutes")
        print("   • AAA: Daily at 12:01 AM Pacific")
        print("   • RBOB & WTI: Every 2 hours (Mon 1AM EST - Fri 11PM EST)")
        print("   • EIA Data (Gasoline Stocks & Refinery Runs): Daily at 11 AM EST")
        print("   • Daily Excel Export: 5 PM EST")
        print("   • Monthly Excel Export: 1st of month at 5 PM EST")
        print("   • Daily Database Backup: 6 PM EST")
        print("=" * 50)
        
        # Schedule GasBuddy every 15 minutes
        schedule.every(15).minutes.do(self.run_gasbuddy_job)
        
        # Schedule AAA daily at 12:01 AM Pacific (3:01 AM Eastern)
        schedule.every().day.at("03:01").do(self.run_aaa_job)
        
        # Schedule EIA data daily at 11 AM EST (4 PM UTC)
        # Schedule EIA data scraping (daily at 10:35 AM EST = 3:35 PM UTC)
        schedule.every().day.at("15:35").do(self.run_gasoline_stocks_job)
        schedule.every().day.at("15:35").do(self.run_refinery_runs_job)
        
        # Schedule daily Excel export at 5 PM EST (10 PM UTC)
        schedule.every().day.at("22:00").do(self.export_daily_excel)
        
        # Schedule monthly Excel export on the 1st of each month at 5 PM EST (10 PM UTC)
        # Schedule monthly Excel export on 1st of month at 5 PM EST (10 PM UTC)
        # Check daily at 10 PM UTC, and export if it's the 1st of the month
        schedule.every().day.at("22:00").do(self.check_and_export_monthly)
        
        # Schedule daily database backup at 11 PM UTC (6 PM EST)
        schedule.every().day.at("23:00").do(self.run_daily_backup)
        
        # Schedule RBOB and WTI every 2 hours during market hours
        # Monday 1AM EST = Monday 6AM UTC, Friday 11PM EST = Saturday 4AM UTC
        for hour in range(6, 24, 2):  # 6, 8, 10, 12, 14, 16, 18, 20, 22
            schedule.every().monday.at(f"{hour:02d}:00").do(self.run_rbob_job)
            schedule.every().monday.at(f"{hour:02d}:00").do(self.run_wti_job)
            schedule.every().tuesday.at(f"{hour:02d}:00").do(self.run_rbob_job)
            schedule.every().tuesday.at(f"{hour:02d}:00").do(self.run_wti_job)
            schedule.every().wednesday.at(f"{hour:02d}:00").do(self.run_rbob_job)
            schedule.every().wednesday.at(f"{hour:02d}:00").do(self.run_wti_job)
            schedule.every().thursday.at(f"{hour:02d}:00").do(self.run_rbob_job)
            schedule.every().thursday.at(f"{hour:02d}:00").do(self.run_wti_job)
            schedule.every().friday.at(f"{hour:02d}:00").do(self.run_rbob_job)
            schedule.every().friday.at(f"{hour:02d}:00").do(self.run_wti_job)
        
        # Add early morning and late night for Friday
        schedule.every().friday.at("00:00").do(self.run_rbob_job)
        schedule.every().friday.at("00:00").do(self.run_wti_job)
        schedule.every().friday.at("02:00").do(self.run_rbob_job)
        schedule.every().friday.at("02:00").do(self.run_wti_job)
        schedule.every().friday.at("04:00").do(self.run_rbob_job)
        schedule.every().friday.at("04:00").do(self.run_wti_job)
        
        print("✅ Scheduler started!")
        print("Press Ctrl+C to stop")
        
        # Run all sources once immediately when scheduler starts
        print("\n🚀 Running all sources once immediately...")
        self.run_all_sources_once()
        print("✅ Initial run completed! Now continuing with scheduled jobs...")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping scheduler...")

def main():
    """Main function to run the gas scraper"""
    print("🚗 Hexa Source Gas Scraper - GasBuddy + AAA + RBOB + WTI + EIA Data")
    print("=" * 50)
    
    # Create scraper instance
    scraper = GasScraper(headless=True)
    
    try:
        print("\n🚗 Gas Scraper Options:")
        print("1. Run GasBuddy once immediately")
        print("2. Run AAA once immediately")
        print("3. Run RBOB once immediately")
        print("4. Run WTI once immediately")
        print("5. Run Gasoline Stocks once immediately")
        print("6. Run Refinery Runs once immediately")
        print("7. Run all sources once")
        print("8. Start scheduled mode (GasBuddy every 10 min + AAA daily + RBOB & WTI every 2h + EIA daily)")
        print("9. View latest data")
        print("10. Export daily Excel")
        print("11. Export monthly Excel")
        print("12. Exit")
        
        choice = input("\nEnter your choice (1-12): ").strip()
        
        if choice == "1":
            scraper.run_gasbuddy_job()
        elif choice == "2":
            scraper.run_aaa_job()
        elif choice == "3":
            scraper.run_rbob_job()
        elif choice == "4":
            scraper.run_wti_job()
        elif choice == "5":
            scraper.run_gasoline_stocks_job()
        elif choice == "6":
            scraper.run_refinery_runs_job()
        elif choice == "7":
            print("🔄 Running all sources...")
            scraper.run_gasbuddy_job()
            time.sleep(2)  # Small delay between jobs
            scraper.run_aaa_job()
            time.sleep(2)  # Small delay between jobs
            scraper.run_rbob_job()
            time.sleep(2)  # Small delay between jobs
            scraper.run_wti_job()
            time.sleep(2)  # Small delay between jobs
            scraper.run_gasoline_stocks_job()
            time.sleep(2)  # Small delay between jobs
            scraper.run_refinery_runs_job()
        elif choice == "8":
            scraper.run_scheduled()
        elif choice == "9":
            latest_data = scraper.get_latest_prices(20)
            if latest_data:
                print("\n📊 Latest Gas Prices:")
                for price, timestamp, region, source, fuel_type, scraped_at in latest_data:
                    print(f"${price} - {timestamp} ({region}) - {source} - {fuel_type} - {scraped_at}")
            else:
                print("No data found in database")
        elif choice == "10":
            scraper.export_daily_excel()
        elif choice == "11":
            scraper.export_monthly_excel()
        elif choice == "12":
            print("Exiting...")
        else:
            print("Invalid choice. Exiting...")
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()
