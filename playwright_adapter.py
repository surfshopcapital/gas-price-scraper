from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Dict, Any
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
import random
import time
import re

# ---------- Utilities ----------

def parse_price(text: str) -> float:
    """Clean price text and convert to float"""
    clean = "".join(c for c in (text or "") if c.isdigit() or c == ".")
    return float(clean) if clean else float("nan")

def now_iso() -> str:
    """Get current UTC time in ISO format"""
    from datetime import datetime
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

US_CITIES = [
    {"name": "New York", "lat": 40.7128, "lng": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
    {"name": "Chicago", "lat": 41.8781, "lng": -87.6298},
    {"name": "Houston", "lat": 29.7604, "lng": -95.3698},
    {"name": "Phoenix", "lat": 33.4484, "lng": -112.0740},
]

@dataclass
class JobContextConfig:
    width: int = 1920
    height: int = 1080
    geolocation: Optional[Dict[str, float]] = None
    grant_geo_origin: Optional[str] = None

class PlaywrightAdapter:
    """Launch one browser per process. For each job, create a fresh context+page."""
    
    def __init__(self, headless: bool = True, extra_args: Optional[list[str]] = None):
        self._p = None
        self._browser: Optional[Browser] = None
        self._headless = headless
        self._args = [
            "--no-sandbox", 
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-extensions",
            "--disable-sync",
            "--metrics-recording-only",
            "--no-first-run",
            "--safebrowsing-disable-auto-update",
            "--disable-blink-features=AutomationControlled",  # Hide automation
            "--disable-web-security",
            "--allow-running-insecure-content",
            "--disable-features=VizDisplayCompositor"
        ]
        if extra_args:
            self._args.extend(extra_args)

    def start(self):
        """Start the Playwright browser"""
        if self._browser:
            return
        self._p = sync_playwright().start()
        
        # Try to use system Chrome first, fallback to Chromium
        try:
            print("   🔍 Trying to use system Chrome...")
            self._browser = self._p.chromium.launch(
                headless=self._headless,
                args=self._args,
                channel="chrome"  # Use system Chrome if available
            )
            print("   ✅ Using system Chrome")
        except Exception as e:
            print(f"   ⚠️ System Chrome not available: {e}")
            print("   🔍 Falling back to Chromium...")
            self._browser = self._p.chromium.launch(
                headless=self._headless,
                args=self._args,
            )
            print("   ✅ Using Chromium")

    def stop(self):
        """Stop and cleanup the browser"""
        try:
            if self._browser:
                self._browser.close()
            if self._p:
                self._p.stop()
        finally:
            self._browser = None
            self._p = None

    @contextmanager
    def job_page(self, cfg: Optional[JobContextConfig] = None):
        """Fresh context + page per job. Auto-closes even if exception occurs."""
        assert self._browser, "PlaywrightAdapter.start() was not called."
        cfg = cfg or JobContextConfig()
        
        context_kwargs: Dict[str, Any] = {
            "viewport": {"width": cfg.width, "height": cfg.height},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "locale": "en-US",
            "timezone_id": "America/New_York",
            "permissions": ["geolocation"],
            "extra_http_headers": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        }
        if cfg.geolocation:
            context_kwargs["geolocation"] = {
                "latitude": cfg.geolocation["latitude"],
                "longitude": cfg.geolocation["longitude"],
            }

        context: BrowserContext = self._browser.new_context(**context_kwargs)
        try:
            if cfg.grant_geo_origin:
                context.grant_permissions(["geolocation"], origin=cfg.grant_geo_origin)

            page: Page = context.new_page()
            page.set_default_timeout(30000)  # 30 second timeout
            
            # Add script to hide automation more aggressively
            page.add_init_script("""
                // Hide automation indicators
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override permissions API
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Override geolocation API
                if (navigator.geolocation) {
                    const originalGetCurrentPosition = navigator.geolocation.getCurrentPosition;
                    navigator.geolocation.getCurrentPosition = function(success, error, options) {
                        // Return a mock location
                        success({
                            coords: {
                                latitude: 40.7128,
                                longitude: -74.0060,
                                accuracy: 100
                            },
                            timestamp: Date.now()
                        });
                    };
                }
            """)
            
            # Handle permission popups automatically
            page.on("dialog", lambda dialog: dialog.accept())
            
            # Handle any permission requests
            page.on("permission", lambda permission: permission.grant())
            
            yield page
        finally:
            context.close()

# ---------- Scraper functions ----------

def scrape_gasbuddy_pw(adapter: PlaywrightAdapter) -> Optional[dict]:
    """Scrape GasBuddy Fuel Insights using Playwright"""
    city = random.choice(US_CITIES)
    cfg = JobContextConfig(
        geolocation={"latitude": city["lat"], "longitude": city["lng"]},
        grant_geo_origin="https://fuelinsights.gasbuddy.com",
    )

    with adapter.job_page(cfg) as page:
        print(f"   🌍 Selected location: {city['name']}")
        
        # Navigate to the page
        print("   🌐 Navigating to GasBuddy...")
        page.goto("https://fuelinsights.gasbuddy.com/", wait_until="domcontentloaded")
        
        # Wait for the page to fully load
        print("   ⏳ Waiting for page to load...")
        time.sleep(5)
        
        # Debug: Print page title and some content
        print(f"   📄 Page title: {page.title()}")
        
        # Wait for network to be idle (JavaScript finished loading)
        print("   🔄 Waiting for JavaScript to finish loading...")
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
            print("   ✅ Network idle - JavaScript should be loaded")
        except Exception as e:
            print(f"   ⚠️ Network idle timeout: {e}")
        
        # Additional wait for dynamic content
        print("   🔄 Waiting for dynamic content...")
        time.sleep(5)
        
        # Try a different approach - wait for any content to appear
        print("   🔍 Waiting for any content to appear...")
        try:
            # Wait for any text content to appear on the page
            page.wait_for_function(
                "() => document.body.textContent && document.body.textContent.length > 1000",
                timeout=30000
            )
            print("   ✅ Page content loaded")
        except Exception as e:
            print(f"   ⚠️ Timeout waiting for content: {e}")
        
        # Now try to get the price with multiple approaches
        price = None
        
        # Approach 1: Try the original selector
        print("   🔍 Approach 1: Trying original selector...")
        price_element = page.locator("#tickingAvgPriceText")
        if price_element.count() > 0:
            raw = price_element.inner_text().strip()
            print(f"   📊 Original selector content: '{raw}'")
            if raw and any(c.isdigit() for c in raw):
                price = parse_price(raw)
                if price == price:  # Check for NaN
                    print(f"   ✅ Successfully parsed price: {price}")
                else:
                    price = None
        
        # Approach 2: If no price, try to find any text that looks like a price
        if price is None:
            print("   🔍 Approach 2: Searching for price patterns...")
            try:
                # Get all text from the page
                all_text = page.locator("body").inner_text()
                print(f"   📄 Page text length: {len(all_text)}")
                
                # Look for price patterns
                import re
                price_matches = re.findall(r'\$(\d+\.\d+)', all_text)
                if price_matches:
                    print(f"   🎯 Found price patterns: {price_matches}")
                    # Take the first reasonable-looking price
                    for match in price_matches:
                        try:
                            price_val = float(match)
                            if 1.0 < price_val < 10.0:  # Reasonable range for gas prices
                                price = price_val
                                print(f"   ✅ Found reasonable price: {price}")
                                break
                        except ValueError:
                            continue
                
                if price is None:
                    print("   ❌ No reasonable price patterns found")
                    
            except Exception as e:
                print(f"   ❌ Error in price pattern search: {e}")
        
        if price is None:
            print("   ❌ Could not find price on GasBuddy page")
            return None

        # Try to find timestamp
        try:
            print("   🔍 Looking for timestamp...")
            ts_element = page.locator("div[data-bind*='tickingAvgLastUpdated']")
            if ts_element.count() > 0:
                timestamp = ts_element.first.inner_text().strip()
                print(f"   🕒 Found timestamp: '{timestamp}'")
            else:
                print("   ⚠️ Timestamp not found, using default")
                timestamp = "Unknown"
        except Exception as e:
            print(f"   ⚠️ Error finding timestamp: {e}")
            timestamp = "Unknown"

        return {
            "price": price,
            "timestamp": timestamp,
            "region": "United States",
            "source": "gasbuddy_fuel_insights",
            "fuel_type": "regular_gas",
        }

def scrape_aaa_pw(adapter: PlaywrightAdapter) -> Optional[dict]:
    """Scrape AAA Gas Prices using Playwright"""
    with adapter.job_page() as page:
        page.goto("https://gasprices.aaa.com/", wait_until="domcontentloaded")
        page.wait_for_selector("tbody tr", timeout=15000)

        rows = page.locator("tbody tr")
        n = rows.count()
        current = None
        
        for i in range(n):
            tds = rows.nth(i).locator("td")
            if tds.count() >= 2:
                label = tds.nth(0).inner_text().strip()
                val = tds.nth(1).inner_text().strip()
                if "Current Avg." in label:
                    current = parse_price(val)
                    break

        if current is None or current != current:  # Check for NaN
            return None

        from datetime import datetime
        return {
            "price": current,
            "timestamp": f"Current as of {datetime.utcnow().date().isoformat()}",
            "region": "United States",
            "source": "aaa_gas_prices",
            "fuel_type": "regular",
        }

def scrape_marketwatch_price_pw(
    adapter: PlaywrightAdapter,
    url: str,
    source: str,
    fuel_type: str,
) -> Optional[dict]:
    """Scrape MarketWatch prices (WTI/RBOB) using Playwright"""
    with adapter.job_page() as page:
        print(f"   🌐 Navigating to {url}...")
        page.goto(url, wait_until="domcontentloaded")
        
        # Wait for page to load
        print("   ⏳ Waiting for page to load...")
        time.sleep(5)
        
        # Debug: Print page title
        print(f"   📄 Page title: {page.title()}")
        
        # Try the exact selectors that work with Selenium
        selectors = [
            "bg-quote.value",
            "h2.intraday__price .value",
            ".intraday__price .value",
            "[data-testid='price']",
            ".price",
            ".value",
            ".bgLast",
            ".lastValue",
            ".last-price",
            "span[data-field='last']",
            ".bgLast .value",
            ".last-value"
        ]
        
        price_text = None
        used_selector = None
        
        # Try each selector
        for sel in selectors:
            try:
                print(f"   🔍 Trying selector: {sel}")
                loc = page.locator(sel)
                if loc.count() > 0:
                    txt = loc.first.inner_text().strip()
                    if txt and any(c.isdigit() for c in txt):
                        price_text = txt
                        used_selector = sel
                        print(f"   ✅ Found price with selector: {sel} = '{txt}'")
                        break
                    else:
                        print(f"   ⚠️ Selector {sel} found but no numeric content: '{txt}'")
                else:
                    print(f"   ❌ Selector {sel} not found")
            except Exception as e:
                print(f"   ❌ Error with selector {sel}: {e}")
                continue

        if not price_text:
            print(f"   ❌ Could not find price on {url}")
            return None

        # Try to find timestamp
        ts_selectors = [
            "span.timestamp__time",
            ".intraday__timestamp",
            "[data-testid='timestamp']",
            ".last-updated",
            ".timestamp",
            ".last-update",
            "time"
        ]
        
        ts_text = "Unknown"
        for sel in ts_selectors:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    txt = loc.first.inner_text().strip()
                    if txt:
                        ts_text = txt
                        print(f"   🕒 Found timestamp with selector: {sel} = '{txt}'")
                        break
            except Exception as e:
                print(f"   ⚠️ Error with timestamp selector {sel}: {e}")
                continue

        # Parse the price
        price = parse_price(price_text)
        if price != price:  # Check for NaN
            print(f"   ❌ Could not parse price: {price_text}")
            return None
            
        ts = ts_text.replace("Last Updated:", "").replace("Last Updated: ", "").strip()
        print(f"   🎯 Final result: price={price}, timestamp='{ts}'")

        return {
            "price": price,
            "timestamp": ts or now_iso(),
            "region": "United States",
            "source": source,
            "fuel_type": fuel_type,
        }

def scrape_tradingeconomics_pw(
    adapter: PlaywrightAdapter,
    url: str,
    source: str,
    fuel_type: str,
) -> Optional[dict]:
    """Scrape Trading Economics data using Playwright"""
    with adapter.job_page() as page:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_selector("td#actual", timeout=15000)
        time.sleep(3)

        actual_elements = page.locator("td#actual")
        if actual_elements.count() == 0:
            return None

        # Find the most recent date with actual data
        latest_date = None
        latest_actual = None
        latest_consensus = None

        for i in range(actual_elements.count()):
            actual_element = actual_elements.nth(i)
            row = actual_element.locator("xpath=./..")
            
            date_element = row.locator("td").nth(0)
            date_text = date_element.inner_text().strip()
            
            actual_text = actual_element.inner_text().strip()
            if not actual_text or actual_text == '':
                continue

            # Parse date (format: YYYY-MM-DD)
            try:
                from datetime import datetime
                date_parts = date_text.split('-')
                if len(date_parts) == 3:
                    year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                    current_date = datetime(year, month, day).date()
                    
                    if latest_date is None or current_date > latest_date:
                        latest_date = current_date
                        latest_actual = actual_text
                        
                        # Get consensus from the same row (7th column)
                        consensus_element = row.locator("td").nth(6)
                        if consensus_element.count() > 0:
                            latest_consensus = consensus_element.inner_text().strip()
                        
            except (ValueError, IndexError):
                continue

        if latest_date is None:
            return None

        # Extract numeric values
        try:
            actual_value = float(latest_actual.replace('M', ''))
            consensus_value = float(latest_consensus.replace('M', '')) if latest_consensus and latest_consensus != '' else 0.0
            
            # Calculate surprise
            surprise = actual_value - consensus_value
            
            return {
                'price': actual_value,
                'timestamp': f"{latest_date.strftime('%Y-%m-%d')}",
                'region': 'United States',
                'source': source,
                'fuel_type': fuel_type,
                'consensus': consensus_value,
                'surprise': surprise
            }
            
        except ValueError:
            return None