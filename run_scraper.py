#!/usr/bin/env python3
"""
Scraper Launcher Script
This script starts the gas price scraper in scheduled mode for Railway deployment.
"""

import os
import sys
import time
import socket
import signal
import schedule
from http.server import HTTPServer, BaseHTTPRequestHandler
from gas_scraper import GasScraper

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health' or self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK - Gas Price Scraper Running')
        elif self.path == '/favicon.ico':
            self.send_response(204)  # No content for favicon
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress HTTP request logging
        pass

def start_health_server(port=8000):
    """Start a simple HTTP server for health checks"""
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"🏥 Health check server started on port {port}")
        return server
    except Exception as e:
        print(f"❌ Health server error: {e}")
        return None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def main():
    print("🚀 Starting Gas Price Scraper in scheduled mode...")
    print("=" * 50)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Test PostgreSQL connection first
        print("🔌 Testing database connection...")
        from test_postgresql import test_connection
        if not test_connection():
            print("❌ Database connection failed. Cannot start scraper.")
            sys.exit(1)
        
        # Create scraper instance
        print("🔧 Creating scraper instance...")
        scraper = GasScraper()
        
        # Start health check server
        port = int(os.getenv('PORT', 8080))  # Match Railway's default port
        print(f"🌐 Starting HTTP server on port {port}...")
        
        server = start_health_server(port)
        if not server:
            print("❌ Failed to start HTTP server")
            sys.exit(1)
        
        print("✅ HTTP server ready for health checks")
        
        # Start the scheduler in a non-blocking way
        print("📅 Starting scheduler...")
        
        # Set up all scheduled jobs (this was missing!)
        # We need to call the scheduler setup without running the blocking loop
        try:
            scraper._setup_scheduler()
            print("✅ Scheduler setup completed successfully")
        except Exception as e:
            print(f"❌ Critical error setting up scheduler: {e}")
            print("🔄 Continuing with basic functionality only...")
            # Set up minimal schedule as fallback
            schedule.every(10).minutes.do(scraper.run_gasbuddy_job)
            print("✅ Basic fallback scheduler set up")
        
        # Run initial job once
        print("🚀 Initial run of all sources once...")
        scraper.run_all_sources_once()
        print("✅ Initial run complete; continuing on schedule.")
        
        # Run scheduler and HTTP server together
        last_status_time = time.time()
        while True:
            try:
                # Handle HTTP requests (non-blocking with timeout)
                server.socket.settimeout(0.1)  # 100ms timeout
                try:
                    server.handle_request()
                except socket.timeout:
                    pass  # No request, continue to scheduler
                
                # Run pending scheduled tasks
                try:
                    jobs_run = schedule.run_pending()
                    if jobs_run:
                        print(f"✅ Executed {len(jobs_run)} scheduled job(s) @ {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    print(f"⚠️ Error running scheduled tasks: {e}")
                
                # Show status every 5 minutes to confirm scheduler is alive
                current_time = time.time()
                if current_time - last_status_time > 300:  # 5 minutes
                    try:
                        print(f"🔄 Scheduler heartbeat @ {time.strftime('%Y-%m-%d %H:%M:%S')} - {len(schedule.get_jobs())} jobs registered")
                        
                        # Show next few scheduled jobs
                        next_jobs = schedule.get_jobs()
                        if next_jobs:
                            print(f"   📅 Next jobs:")
                            for i, job in enumerate(next_jobs[:3]):  # Show next 3 jobs
                                try:
                                    print(f"      {i+1}. {job.job_func.__name__} @ {job.next_run}")
                                except Exception as e:
                                    print(f"      {i+1}. Unknown job (error: {e})")
                    except Exception as e:
                        print(f"⚠️ Error in scheduler heartbeat: {e}")
                    
                    last_status_time = current_time
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print("\n⏹️ Scraper stopped by user")
                break
            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                time.sleep(1)  # Wait before retrying
        
    except Exception as e:
        print(f"❌ Error starting scraper: {e}")
        sys.exit(1)
    finally:
        if 'server' in locals():
            server.server_close()
        if 'scraper' in locals():
            try:
                scraper._shutdown_playwright()
            except Exception as e:
                print(f"⚠️ Error during shutdown: {e}")
        print("🔄 Shutting down...")

if __name__ == "__main__":
    main()
