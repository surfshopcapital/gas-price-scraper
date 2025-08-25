#!/usr/bin/env python3
"""
Scraper Launcher Script
This script starts the gas price scraper in scheduled mode for Railway deployment.
"""

import os
import sys
import time
import select
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

def main():
    print("🚀 Starting Gas Price Scraper in scheduled mode...")
    print("=" * 50)
    
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
        port = int(os.getenv('PORT', 8000))
        print(f"🌐 Starting HTTP server on port {port}...")
        
        server = start_health_server(port)
        if not server:
            print("❌ Failed to start HTTP server")
            sys.exit(1)
        
        print("✅ HTTP server ready for health checks")
        
        # Start the scheduler in a non-blocking way
        print("📅 Starting scheduler...")
        
        # Run initial job once
        print("🚀 Initial run of all sources once...")
        scraper.run_all_sources_once()
        print("✅ Initial run complete; continuing on schedule.")
        
        # Run scheduler and HTTP server together
        while True:
            try:
                # Handle HTTP requests (non-blocking)
                server.handle_request()
                
                # Run pending scheduled tasks
                import schedule
                schedule.run_pending()
                
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
        print("🔄 Shutting down...")

if __name__ == "__main__":
    main()
