import os
import time
import random
import schedule
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from contextlib import contextmanager
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from zoneinfo import ZoneInfo

US_CITIES = [
    {"name": "New York", "lat": 40.7128, "lng": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lng": -118.2437},
    {"name": "Chicago", "lat": 41.8781, "lng": -87.6298},
    {"name": "Houston", "lat": 29.7604, "lng": -95.3698},
    {"name": "Phoenix", "lat": 33.4484, "lng": -112.0740},
]

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class GasScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway",
        )
        self._playwright = None
        self._browser = None
        self._init_db()

    # ---------------------------
    # Setup / Teardown Playwright
    # ---------------------------
    def _boot_playwright(self):
        if self._playwright is None:
            self._playwright = sync_playwright().start()
        if self._browser is None:
            # Use chromium for best portability on Railway
            self._browser = self._playwright.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-background-networking",
                    "--disable-extensions",
                    "--disable-sync",
                    "--no-first-run",
                    "--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter,OptimizationHints",
                ],
            )

    def _shutdown_playwright(self):
        try:
            if self._browser:
                self._browser.close()
        except Exception as e:
            print(f"⚠️ Error closing browser: {e}")
        finally:
            self._browser = None
            try:
                if self._playwright:
                    self._playwright.stop()
            except Exception as e:
                print(f"⚠️ Error stopping playwright: {e}")
            finally:
                self._playwright = None

    @contextmanager
    def _fresh_context(self, *, grant_geo=False, city=None):
        """
        Creates a fresh browser context + page per job.
        If grant_geo=True, sets geolocation + geolocation permission for GasBuddy.
        """
        try:
            self._boot_playwright()
            context_kwargs = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": DEFAULT_UA,
                "locale": "en-US",
            }

            if grant_geo:
                if city is None:
                    city = random.choice(US_CITIES)
                context_kwargs.update({
                    "geolocation": {"latitude": city["lat"], "longitude": city["lng"]},
                    "permissions": ["geolocation"],  # applies to all origins in this context
                })

            context = self._browser.new_context(**context_kwargs)
            page = context.new_page()
            page.set_default_timeout(30_000)
            try:
                yield context, page
            finally:
                try:
                    context.close()
                except Exception:
                    pass
        except Exception as e:
            print(f"   ⚠️ Error creating browser context: {e}")
            # Return dummy context/page that will fail gracefully
            yield None, None

    # ---------------------------
    # DB
    # ---------------------------
    def _init_db(self):
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            # sanity check + add columns if missing (Postgres syntax: double precision)
            cur.execute("SELECT 1 FROM gas_prices LIMIT 1;")
            try:
                cur.execute("ALTER TABLE gas_prices ADD COLUMN IF NOT EXISTS consensus double precision;")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE gas_prices ADD COLUMN IF NOT EXISTS surprise double precision;")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE gas_prices ADD COLUMN IF NOT EXISTS as_of_date date;")
            except Exception:
                pass
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ DB OK @ {now_ts()}")
        except Exception as e:
            print(f"❌ Error initializing database: {e}")
    
    def _check_db_connection(self):
        """Check if database is accessible"""
        try:
            conn = psycopg2.connect(self.database_url)
            conn.close()
            return True
        except Exception:
            return False

    def save_to_database(self, rows):
        if not rows:
            return False
        if not isinstance(rows, list):
            rows = [rows]
        
        # Check database connection first
        if not self._check_db_connection():
            print("❌ Database not accessible, skipping save")
            return False
            
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            saved = 0
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO gas_prices
                      (price, timestamp, region, source, fuel_type, consensus, surprise, as_of_date, scraped_at)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        r.get("price"),
                        r.get("timestamp"),
                        r.get("region"),
                        r.get("source"),
                        r.get("fuel_type"),
                        r.get("consensus"),
                        r.get("surprise"),
                        r.get("as_of_date"),   # <<< NEW (None for non-AAA is fine)
                    ),
                )
                saved += 1
            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ Saved {saved} row(s) @ {now_ts()}")
            return True
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            # Try to reconnect and retry once
            try:
                print("   🔄 Attempting to reconnect to database...")
                conn = psycopg2.connect(self.database_url)
                cur = conn.cursor()
                saved = 0
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO gas_prices
                          (price, timestamp, region, source, fuel_type, consensus, surprise, as_of_date, scraped_at)
                        VALUES
                          (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        (
                            r.get("price"),
                            r.get("timestamp"),
                            r.get("region"),
                            r.get("source"),
                            r.get("fuel_type"),
                            r.get("consensus"),
                            r.get("surprise"),
                            r.get("as_of_date"),   # <<< NEW (None for non-AAA is fine)
                        ),
                    )
                    saved += 1
                conn.commit()
                cur.close()
                conn.close()
                print(f"✅ Retry successful: Saved {saved} row(s) @ {now_ts()}")
                return True
            except Exception as retry_e:
                print(f"❌ Retry also failed: {retry_e}")
                return False

    # --- SANITY FILTERS -------------------------------------------------
    def _get_last_price(self, source: str):
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT price FROM gas_prices
                WHERE source=%s
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (source,))
            row = cur.fetchone()
            cur.close(); conn.close()
            return float(row[0]) if row else None
        except Exception:
            return None

    def _get_price_approx_one_week_ago(self, source: str):
        """Most recent price at/just before ~7 days ago."""
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT price FROM gas_prices
                WHERE source=%s AND scraped_at <= (CURRENT_TIMESTAMP - INTERVAL '7 days')
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (source,))
            row = cur.fetchone()
            cur.close(); conn.close()
            return float(row[0]) if row else None
        except Exception:
            return None

    def _is_plausible_change(self, source: str, new_price: float, limit: float = 0.50) -> bool:
        """
        Returns True if the new price is plausible. Reject when absolute pct-change
        vs last and/or ~1-week-ago exceeds 50%.
        """
        def _pct_jump(prev):
            if prev is None: 
                return 0.0
            if prev == 0:
                return 0.0
            return abs((new_price - prev) / prev)

        last = self._get_last_price(source)
        last_week = self._get_price_approx_one_week_ago(source)

        if _pct_jump(last) > limit:
            print(f"⚠️ Sanity filter: {source} jumped {100*_pct_jump(last):.1f}% vs last ({last}); ignoring scrape.")
            return False
        if _pct_jump(last_week) > limit:
            print(f"⚠️ Sanity filter: {source} jumped {100*_pct_jump(last_week):.1f}% vs ~1 week ago ({last_week}); ignoring scrape.")
            return False
        return True

    # ---------------------------
    # Navigation helper (retry)
    # ---------------------------
    def _goto_with_retry(self, page, url, attempts=3, wait_state="domcontentloaded"):
        for i in range(attempts):
            try:
                print(f"   🌐 goto attempt {i+1}/{attempts}: {url}")
                page.goto(url, wait_until=wait_state, timeout=30_000)
                # let SPAs breathe a bit
                page.wait_for_timeout(1500)
                return True
            except PWTimeout as e:
                print(f"   ⚠️ Timeout navigating to {url}: {e}")
            except Exception as e:
                print(f"   ⚠️ Error navigating to {url}: {e}")
            time.sleep(1.5)
        return False

    # ---------------------------
    # GASBUDDY (geolocation + CF)
    # ---------------------------
    def scrape_gasbuddy(self):
        print("🚀 GasBuddy")
        city = random.choice(US_CITIES)
        with self._fresh_context(grant_geo=True, city=city) as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://fuelinsights.gasbuddy.com/"
            if not self._goto_with_retry(page, url, attempts=3, wait_state="load"):
                return None

            # GasBuddy’s SPA sometimes takes longer to populate text; poll the price element.
            # The ID you used is still valid: #tickingAvgPriceText
            try:
                el = page.wait_for_selector("#tickingAvgPriceText", timeout=20_000)
                price_text = el.inner_text().strip()
            except Exception:
                # fallback: look by text marker
                try:
                    el = page.locator("#tickingAvgPriceText").first
                    price_text = el.inner_text().strip()
                except Exception as e:
                    print(f"   ❌ GasBuddy price not found: {e}")
                    return None

            # Timestamp (binding text can vary) — try known container
            timestamp_text = "Unknown"
            try:
                ts_el = page.locator("div[data-bind*='tickingAvgLastUpdated']").first
                if ts_el:
                    timestamp_text = ts_el.inner_text().strip()
            except Exception:
                pass

            try:
                price = float(price_text.replace("$", "").strip())
                data = {
                    "price": price,
                    "timestamp": timestamp_text,
                    "region": "United States",
                    "source": "gasbuddy_fuel_insights",
                    "fuel_type": "regular_gas",
                }
                print(f"   ✅ GasBuddy {price} ({timestamp_text})")
                return data
            except Exception as e:
                print(f"   ❌ GasBuddy parse error: {e}")
                return None

    # ---------------------------
    # AAA
    # ---------------------------
    def scrape_aaa(self):
        print("🚗 AAA")
        with self._fresh_context() as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://gasprices.aaa.com/"
            if not self._goto_with_retry(page, url):
                return None

            # Wait for the table body; ensure it actually has content
            try:
                tbody = page.wait_for_selector("tbody", timeout=20_000)
                text = tbody.inner_text().strip()
                if len(text) < 20:
                    page.wait_for_timeout(4000)
                    text = tbody.inner_text().strip()
            except Exception as e:
                print(f"   ❌ AAA table not ready: {e}")
                return None

            # Find the row whose first cell contains "Current Avg."
            rows = page.locator("tbody tr")
            count = rows.count()
            current_price = None
            for i in range(count):
                tds = rows.nth(i).locator("td")
                if tds.count() >= 2:
                    period = tds.nth(0).inner_text().strip()
                    price = tds.nth(1).inner_text().strip()
                    if "Current Avg." in period:
                        current_price = price.replace("$", "").strip()
                        break

            if not current_price:
                print("   ❌ AAA current price not found")
                return None

            try:
                current_float = float(current_price)
                nyc_now = datetime.now(tz=ZoneInfo("America/New_York"))
                as_of = (nyc_now.date() - timedelta(days=1))  # the day the AAA value represents
                out = {
                    "price": current_float,
                    "timestamp": f"Current as of {nyc_now.strftime('%Y-%m-%d')}",
                    "region": "United States",
                    "source": "aaa_gas_prices",
                    "fuel_type": "regular",
                    "as_of_date": as_of,   # <<< NEW
                }
                print(f"   ✅ AAA {current_float} (as_of={as_of})")
                return out
            except Exception as e:
                print(f"   ❌ AAA parse error: {e}")
                return None

    # ---------------------------
    # MarketWatch (RBOB / WTI)
    # ---------------------------
    def _extract_marketwatch(self, page):
        # Try a few selectors for price + timestamp (MW changes DOM often)
        price_selectors = [
            "bg-quote.value",
            "h2.intraday__price .value",
            ".intraday__price .value",
            ".intraday__price",
            "[data-testid='price']",
            ".price",
        ]
        ts_selectors = [
            "span.timestamp__time",
            ".intraday__timestamp",
            ".timestamp",
            "[data-testid='timestamp']",
            ".last-updated",
        ]

        price_text = None
        for sel in price_selectors:
            try:
                el = page.locator(sel).first
                if el and el.is_visible():
                    txt = el.inner_text().strip()
                    if txt:
                        price_text = txt
                        break
            except Exception:
                continue

        if not price_text:
            return None, "Unknown"

        timestamp_text = "Unknown"
        for sel in ts_selectors:
            try:
                el = page.locator(sel).first
                if el and el.is_visible():
                    t = el.inner_text().strip()
                    if t:
                        timestamp_text = t
                        break
            except Exception:
                continue

        # clean price (remove commas and stray characters)
        clean = "".join(c for c in price_text if (c.isdigit() or c == "."))

        return clean, timestamp_text

    def scrape_rbob(self):
        print("⛽ RBOB")
        with self._fresh_context() as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://www.marketwatch.com/investing/future/rb.1"
            if not self._goto_with_retry(page, url):
                return None
            page.wait_for_timeout(2000)
            price_text, ts = self._extract_marketwatch(page)
            if not price_text:
                print("   ❌ RBOB price not found")
                return None
            try:
                price = float(price_text)
                out = {
                    "price": price,
                    "timestamp": ts.replace("Last Updated:", "").strip(),
                    "region": "United States",
                    "source": "marketwatch_rbob_futures",
                    "fuel_type": "rbob_futures",
                }
                print(f"   ✅ RBOB {price} ({out['timestamp']})")
                return out
            except Exception as e:
                print(f"   ❌ RBOB parse error: {e}")
                return None

    def scrape_wti(self):
        print("🛢️ WTI")
        with self._fresh_context() as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://www.marketwatch.com/investing/future/cl.1"
            if not self._goto_with_retry(page, url):
                return None
            page.wait_for_timeout(2000)
            price_text, ts = self._extract_marketwatch(page)
            if not price_text:
                print("   ❌ WTI price not found")
                return None
            try:
                price = float(price_text)
                out = {
                    "price": price,
                    "timestamp": ts.replace("Last Updated:", "").strip(),
                    "region": "United States",
                    "source": "marketwatch_wti_futures",
                    "fuel_type": "wti_futures",
                }
                print(f"   ✅ WTI {price} ({out['timestamp']})")
                return out
            except Exception as e:
                print(f"   ❌ WTI parse error: {e}")
                return None

    # ---------------------------
    # TradingEconomics (Gasoline Stocks / Refinery Runs)
    # ---------------------------
    def _extract_te_rows(self, page):
        # Find rows that have actual data (not empty actual cells)
        rows = page.locator("tbody tr")
        return rows

    def scrape_gasoline_stocks(self):
        print("⛽ TE Gasoline Stocks")
        with self._fresh_context() as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://tradingeconomics.com/united-states/gasoline-stocks-change"
            if not self._goto_with_retry(page, url, wait_state="load"):
                return None
            page.wait_for_timeout(2000)

            try:
                rows = self._extract_te_rows(page)
                count = rows.count()
            except Exception as e:
                print(f"   ❌ TE rows not found: {e}")
                return None

            # Find the most recent historical date (closest to today but not in the future)
            today = datetime.now().date()
            best_row = None
            best_date_diff = float('inf')
            
            for i in range(count):
                row = rows.nth(i)
                tds = row.locator("td")
                # expected: [calendar_date, gmt_time, hidden_ref, reference, actual, previous, consensus, teforecast]
                if tds.count() < 7:
                    continue
                
                # Get the actual value from the 5th column (index 4)
                actual_cell = tds.nth(4)
                actual_text = actual_cell.inner_text().strip()
                
                # Skip rows where actual is empty
                if not actual_text or actual_text == "":
                    continue
                
                # Get calendar date from 1st column (index 0)
                date_text = tds.nth(0).inner_text().strip()
                
                try:
                    # Parse the date (format: "2025-08-20")
                    row_date = datetime.strptime(date_text, "%Y-%m-%d").date()
                    
                    # Skip future dates
                    if row_date > today:
                        continue
                    
                    # Calculate days difference from today
                    date_diff = abs((today - row_date).days)
                    
                    # If this is closer to today than our current best, update
                    if date_diff < best_date_diff:
                        best_date_diff = date_diff
                        
                        # Get consensus from 7th column (index 6)
                        consensus_cell = tds.nth(6)
                        consensus_text = consensus_cell.inner_text().strip()
                        
                        # Get previous from 6th column (index 5)
                        previous_cell = tds.nth(5)
                        previous_text = previous_cell.inner_text().strip()
                        
                        best_row = (date_text, actual_text, consensus_text, previous_text)
                        print(f"   📅 Found better row: {date_text} (diff: {date_diff} days) - Actual: {actual_text}, Consensus: {consensus_text}, Previous: {previous_text}")
                        
                except ValueError as e:
                    print(f"   ⚠️ Could not parse date '{date_text}': {e}")
                    continue

            if not best_row:
                print("   ❌ No TE actual rows with valid historical dates")
                return None

            date_text, actual_text, consensus_text, previous_text = best_row
            try:
                # Clean the actual value (remove 'M' and convert to float)
                actual_val = float(actual_text.replace("M", "").strip())
                
                # Clean consensus value if it exists
                consensus_val = 0.0
                if consensus_text and consensus_text.strip():
                    consensus_val = float(consensus_text.replace("M", "").strip())
                
                # Calculate surprise
                surprise = actual_val - consensus_val
                
                out = {
                    "price": actual_val,
                    "timestamp": date_text,
                    "region": "United States",
                    "source": "tradingeconomics_gasoline_stocks",
                    "fuel_type": "gasoline_stocks_change",
                    "consensus": consensus_val,
                    "surprise": surprise,
                }
                print(f"   ✅ TE Gasoline Stocks {actual_val}M ({date_text}) - Most recent historical data")
                print(f"      📊 Consensus: {consensus_val}M, Surprise: {surprise}M")
                return out
            except Exception as e:
                print(f"   ❌ TE parse error: {e}")
                print(f"      Raw values - Actual: '{actual_text}', Consensus: '{consensus_text}'")
                return None

    def scrape_refinery_runs(self):
        print("🏭 TE Refinery Runs")
        with self._fresh_context() as (ctx, page):
            if ctx is None or page is None:
                print("   ❌ Failed to create browser context")
                return None
                
            url = "https://tradingeconomics.com/united-states/refinery-crude-runs"
            if not self._goto_with_retry(page, url, wait_state="load"):
                return None
            page.wait_for_timeout(2000)

            try:
                rows = self._extract_te_rows(page)
                count = rows.count()
            except Exception as e:
                print(f"   ❌ TE rows not found: {e}")
                return None

            # Find the most recent historical date (closest to today but not in the future)
            today = datetime.now().date()
            best_row = None
            best_date_diff = float('inf')
            
            for i in range(count):
                row = rows.nth(i)
                tds = row.locator("td")
                if tds.count() < 7:
                    continue
                
                # Get the actual value from the 5th column (index 4)
                actual_cell = tds.nth(4)
                actual_text = actual_cell.inner_text().strip()
                
                # Skip rows where actual is empty
                if not actual_text or actual_text == "":
                    continue
                
                # Get calendar date from 1st column (index 0)
                date_text = tds.nth(0).inner_text().strip()
                
                try:
                    # Parse the date (format: "2025-08-20")
                    row_date = datetime.strptime(date_text, "%Y-%m-%d").date()
                    
                    # Skip future dates
                    if row_date > today:
                        continue
                    
                    # Calculate days difference from today
                    date_diff = abs((today - row_date).days)
                    
                    # If this is closer to today than our current best, update
                    if date_diff < best_date_diff:
                        best_date_diff = date_diff
                        
                        # Get previous from 6th column (index 5)
                        previous_cell = tds.nth(5)
                        previous_text = previous_cell.inner_text().strip()
                        
                        best_row = (date_text, actual_text, previous_text)
                        print(f"   📅 Found better row: {date_text} (diff: {date_diff} days) - Actual: {actual_text}, Previous: {previous_text}")
                        
                except ValueError as e:
                    print(f"   ⚠️ Could not parse date '{date_text}': {e}")
                    continue

            if not best_row:
                print("   ❌ No TE refinery rows with valid historical dates")
                return None

            date_text, actual_text, previous_text = best_row
            try:
                # Clean the actual value (remove 'M' and convert to float)
                actual_val = float(actual_text.replace("M", "").strip())
                
                out = {
                    "price": actual_val,
                    "timestamp": date_text,
                    "region": "United States",
                    "source": "tradingeconomics_refinery_runs",
                    "fuel_type": "refinery_crude_runs",
                }
                print(f"   ✅ TE Refinery Runs {actual_val}M ({date_text}) - Most recent historical data")
                return out
            except Exception as e:
                print(f"   ❌ TE parse error: {e}")
                print(f"      Raw values - Actual: '{actual_text}'")
                return None

    # ---------------------------
    # Orchestrators (mirror your API)
    # ---------------------------
    def run_gasbuddy_job(self):
        print(f"\n--- GasBuddy job @ {now_ts()} ---")
        data = self.scrape_gasbuddy()
        if data and self._is_plausible_change("gasbuddy_fuel_insights", data["price"]):
            self.save_to_database(data)
        else:
            print("❌ GasBuddy scrape failed or rejected by sanity filter")

    def run_aaa_job(self):
        print(f"\n--- AAA job @ {now_ts()} ---")
        data = self.scrape_aaa()
        if data and self._is_plausible_change("aaa_gas_prices", data["price"]):
            self.save_to_database(data)
        else:
            print("❌ AAA scrape failed or rejected by sanity filter")

    def run_rbob_job(self):
        print(f"\n--- RBOB job @ {now_ts()} ---")
        data = self.scrape_rbob()
        if data and self._is_plausible_change("marketwatch_rbob_futures", data["price"]):
            self.save_to_database(data)
        else:
            print("❌ RBOB scrape failed or rejected by sanity filter")

    def run_wti_job(self):
        print(f"\n--- WTI job @ {now_ts()} ---")
        data = self.scrape_wti()
        if data and self._is_plausible_change("marketwatch_wti_futures", data["price"]):
            self.save_to_database(data)
        else:
            print("❌ WTI scrape failed or rejected by sanity filter")

    def run_gasoline_stocks_job(self):
        print(f"\n--- TE Gasoline Stocks job @ {now_ts()} ---")
        data = self.scrape_gasoline_stocks()
        if data:
            self.save_to_database(data)
        else:
            print("❌ TE Gasoline Stocks scrape failed")

    def run_refinery_runs_job(self):
        print(f"\n--- TE Refinery Runs job @ {now_ts()} ---")
        data = self.scrape_refinery_runs()
        if data:
            self.save_to_database(data)
        else:
            print("❌ TE Refinery Runs scrape failed")

    def run_all_sources_once(self):
        print(f"\n=== Run all once @ {now_ts()} ===")
        try:
            self.run_gasbuddy_job()
            time.sleep(2)
            self.run_aaa_job()
            time.sleep(2)
            self.run_rbob_job()
            time.sleep(2)
            self.run_wti_job()
            time.sleep(2)
            self.run_gasoline_stocks_job()
            time.sleep(2)
            self.run_refinery_runs_job()
            print("✅ All sources completed")
        except Exception as e:
            print(f"❌ Error running all sources: {e}")

    # ---------------------------
    # Queries / Export (unchanged logic)
    # ---------------------------
    def get_latest_prices(self, limit=20):
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                ORDER BY scraped_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return rows
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return []

    def export_daily_excel(self):
        try:
            today = datetime.now().date()
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()

            def fetch(source):
                cur.execute(
                    """
                    SELECT price, timestamp, region, source, fuel_type, scraped_at
                    FROM gas_prices
                    WHERE source = %s AND DATE(scraped_at) = %s
                    ORDER BY scraped_at DESC
                    """,
                    (source, today),
                )
                return cur.fetchall()

            gasbuddy_data = fetch("gasbuddy_fuel_insights")
            aaa_data = fetch("aaa_gas_prices")
            rbob_data = fetch("marketwatch_rbob_futures")
            wti_data = fetch("marketwatch_wti_futures")

            cur.execute(
                """
                SELECT price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_gasoline_stocks' AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
                """,
                (today,),
            )
            gasoline_stocks_data = cur.fetchall()

            cur.execute(
                """
                SELECT price, timestamp, region, source, fuel_type, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_refinery_runs' AND DATE(scraped_at) = %s
                ORDER BY scraped_at DESC
                """,
                (today,),
            )
            refinery_runs_data = cur.fetchall()

            cur.close()
            conn.close()

            filename = f"gas_prices_daily_{today.strftime('%Y%m%d')}.xlsx"
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                def write_sheet(name, data, cols):
                    df = pd.DataFrame(data, columns=cols)
                    df.to_excel(writer, sheet_name=name, index=False)

                write_sheet("GasBuddy", gasbuddy_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("AAA", aaa_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("RBOB", rbob_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("WTI", wti_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("Gasoline_Stocks", gasoline_stocks_data,
                            ["price","timestamp","region","source","fuel_type","consensus","surprise","scraped_at"])
                write_sheet("Refinery_Runs", refinery_runs_data,
                            ["price","timestamp","region","source","fuel_type","scraped_at"])

            print(f"✅ Daily Excel export: {filename}")
            return filename
        except Exception as e:
            print(f"❌ Error exporting daily Excel: {e}")
            return None

    def export_monthly_excel(self):
        try:
            now = datetime.now()
            start = now.replace(day=1).date()
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor()

            def fetch(source, extra_cols=""):
                cols = "price, timestamp, region, source, fuel_type, scraped_at"
                if extra_cols:
                    cols = f"{cols}, {extra_cols}"
                cur.execute(
                    f"""
                    SELECT {cols}
                    FROM gas_prices
                    WHERE source = %s AND DATE(scraped_at) >= %s
                    ORDER BY scraped_at DESC
                    """,
                    (source, start),
                )
                return cur.fetchall()

            gasbuddy_data = fetch("gasbuddy_fuel_insights")
            aaa_data = fetch("aaa_gas_prices")
            rbob_data = fetch("marketwatch_rbob_futures")
            wti_data = fetch("marketwatch_wti_futures")

            cur.execute(
                """
                SELECT price, timestamp, region, source, fuel_type, consensus, surprise, scraped_at
                FROM gas_prices
                WHERE source = 'tradingeconomics_gasoline_stocks' AND DATE(scraped_at) >= %s
                ORDER BY scraped_at DESC
                """,
                (start,),
            )
            gasoline_stocks_data = cur.fetchall()

            refinery_runs_data = fetch("tradingeconomics_refinery_runs")

            cur.close()
            conn.close()

            filename = f"gas_prices_monthly_{now.strftime('%Y%m')}.xlsx"
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                def write_sheet(name, data, cols):
                    df = pd.DataFrame(data, columns=cols)
                    df.to_excel(writer, sheet_name=name, index=False)

                write_sheet("GasBuddy", gasbuddy_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("AAA", aaa_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("RBOB", rbob_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("WTI", wti_data, ["price","timestamp","region","source","fuel_type","scraped_at"])
                write_sheet("Gasoline_Stocks", gasoline_stocks_data,
                            ["price","timestamp","region","source","fuel_type","consensus","surprise","scraped_at"])
                write_sheet("Refinery_Runs", refinery_runs_data,
                            ["price","timestamp","region","source","fuel_type","scraped_at"])

            print(f"✅ Monthly Excel export: {filename}")
            return filename
        except Exception as e:
            print(f"❌ Error exporting monthly Excel: {e}")
            return None

    # ---------------------------
    # Scheduler (mirrors yours)
    # ---------------------------
    def _setup_scheduler(self):
        """Set up all scheduled jobs without starting the blocking loop"""
        print("🚗 Hexa Source Gas Scraper (Playwright) — Scheduled Mode")
        print("=" * 50)
        print("• GasBuddy: every 10 min")
        print("• AAA: daily 3:30 AM EST (7:30 UTC)")
        print("• RBOB & WTI: every hour Sun 6pm-Fri 8pm EST")
        print("• EIA (TE pages): daily 11:00, 12:00, 13:00, 17:00 EST (16:00, 17:00, 18:00, 22:00 UTC)")
        print("• Daily Excel: 22:00 UTC")
        print("• Monthly Excel check: 22:00 UTC (run if day==1)")
        print("=" * 50)
        
        # Clear any existing jobs
        schedule.clear()

        # NOTE: All times are in UTC (Railway runs in UTC)
        # EST is UTC-5 (UTC-4 during daylight saving time)
        
        # GasBuddy: every 10 minutes (changed from 15)
        try:
            schedule.every(10).minutes.do(self.run_gasbuddy_job)
        except Exception as e:
            print(f"⚠️ Error scheduling GasBuddy: {e}")
        
        # AAA: daily at 3:30 AM EST (7:30 UTC)
        try:
            schedule.every().day.at("07:30").do(self.run_aaa_job)
        except Exception as e:
            print(f"⚠️ Error scheduling AAA: {e}")
        
        # TE pages (was 10:35 EST). New times: 11:00, 12:00, 13:00, 17:00 EST
        # → 16:00, 17:00, 18:00, 22:00 UTC
        try:
            for t in ("16:00", "17:00", "18:00", "22:00"):
                schedule.every().day.at(t).do(self.run_gasoline_stocks_job)
                schedule.every().day.at(t).do(self.run_refinery_runs_job)
        except Exception as e:
            print(f"⚠️ Error scheduling TE pages: {e}")
        
        # Daily Excel: 22:00 UTC
        try:
            schedule.every().day.at("22:00").do(self.export_daily_excel)
            schedule.every().day.at("22:00").do(self._monthly_check)
        except Exception as e:
            print(f"⚠️ Error scheduling daily Excel: {e}")
        
        # Daily backup: 23:00 UTC
        try:
            schedule.every().day.at("23:00").do(self._daily_backup_safe)
        except Exception as e:
            print(f"⚠️ Error scheduling daily backup: {e}")

        # RBOB & WTI: every hour from Sunday 6pm EST to Friday 8pm EST
        # Sunday: 6pm-11pm EST (23:00-04:00 UTC next day)
        for hour in range(23, 24):
            try:
                schedule.every().sunday.at(f"{hour:02d}:00").do(self.run_rbob_job)
                schedule.every().sunday.at(f"{hour:02d}:00").do(self.run_wti_job)
            except Exception as e:
                print(f"⚠️ Error scheduling Sunday at {hour:02d}:00: {e}")
        # Monday 00:00-04:00 UTC (Sunday 7pm-11pm EST)
        for hour in range(0, 4):
            try:
                schedule.every().monday.at(f"{hour:02d}:00").do(self.run_rbob_job)
                schedule.every().monday.at(f"{hour:02d}:00").do(self.run_wti_job)
            except Exception as e:
                print(f"⚠️ Error scheduling Monday at {hour:02d}:00: {e}")
        
        # Monday-Thursday: every hour from 6am-11pm EST (11:00-04:00 UTC next day)
        for hour in range(11, 24):
            for day in ("monday", "tuesday", "wednesday", "thursday"):
                try:
                    getattr(schedule.every(), day).at(f"{hour:02d}:00").do(self.run_rbob_job)
                    getattr(schedule.every(), day).at(f"{hour:02d}:00").do(self.run_wti_job)
                except Exception as e:
                    print(f"⚠️ Error scheduling {day} at {hour:02d}:00: {e}")
        
        # Friday: every hour from 6am-8pm EST (11:00-00:00 UTC next day)
        for hour in range(11, 24):
            try:
                schedule.every().friday.at(f"{hour:02d}:00").do(self.run_rbob_job)
                schedule.every().friday.at(f"{hour:02d}:00").do(self.run_wti_job)
            except Exception as e:
                print(f"⚠️ Error scheduling Friday at {hour:02d}:00: {e}")
        # Add 00:00 UTC (8pm EST Friday)
        try:
            schedule.every().friday.at("00:00").do(self.run_rbob_job)
            schedule.every().friday.at("00:00").do(self.run_wti_job)
        except Exception as e:
            print(f"⚠️ Error scheduling Friday at 00:00: {e}")

        print("✅ Scheduler started")
        
        # Verify all jobs were scheduled correctly
        total_jobs = len(schedule.get_jobs())
        print(f"📊 Total scheduled jobs: {total_jobs}")

    def run_scheduled(self):
        # Set up the scheduler first
        self._setup_scheduler()
        
        print("🚀 Initial run of all sources once...")
        self.run_all_sources_once()
        print("✅ Initial run complete; continuing on schedule.")

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping scheduler...")
        finally:
            self._shutdown_playwright()

    # helpers for schedule
    def _monthly_check(self):
        if datetime.now().day == 1:
            self.export_monthly_excel()

    def _daily_backup_safe(self):
        try:
            from backup_database import main as backup_main
            backup_main()
            print("✅ Daily backup completed")
        except Exception as e:
            print(f"❌ Daily backup failed: {e}")

# ---------------------------
# CLI
# ---------------------------
def main():
    print("🚗 Hexa Source Gas Scraper (Playwright)")
    print("=" * 50)
    scraper = GasScraper(headless=True)

    if os.getenv("NONINTERACTIVE", "0") == "1":
        print("🚀 Non-interactive/Railway mode — starting scheduler...")
        scraper.run_scheduled()
        return

    try:
        print("\nOptions:")
        print("1. GasBuddy once")
        print("2. AAA once")
        print("3. RBOB once")
        print("4. WTI once")
        print("5. Gasoline Stocks (TE) once")
        print("6. Refinery Runs (TE) once")
        print("7. Run all once")
        print("8. Start scheduled mode")
        print("9. View latest data")
        print("10. Export daily Excel")
        print("11. Export monthly Excel")
        print("12. Exit")

        choice = input("\nEnter choice (1-12): ").strip()
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
            scraper.run_all_sources_once()
        elif choice == "8":
            scraper.run_scheduled()
        elif choice == "9":
            rows = scraper.get_latest_prices(20)
            if rows:
                print("\n📊 Latest:")
                for price, timestamp, region, source, fuel_type, scraped_at in rows:
                    print(f"${price} - {timestamp} ({region}) - {source} - {fuel_type} - {scraped_at}")
            else:
                print("No data found.")
        elif choice == "10":
            scraper.export_daily_excel()
        elif choice == "11":
            scraper.export_monthly_excel()
        elif choice == "12":
            print("Bye")
        else:
            print("Invalid choice.")
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        scraper._shutdown_playwright()

if __name__ == "__main__":
    main()
