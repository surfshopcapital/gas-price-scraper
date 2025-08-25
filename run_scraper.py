#!/usr/bin/env python3
"""
Scraper Launcher Script
This script starts the gas price scraper in scheduled mode for Railway deployment.
"""

import os
import sys
import threading
import time
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
        
        # Start health check server FIRST and wait for it to be ready
        port = int(os.getenv('PORT', 8000))
        print(f"🌐 Starting HTTP server on port {port}...")
        
        # Start HTTP server in main thread first to ensure it's ready
        server = start_health_server(port)
        if not server:
            print("❌ Failed to start HTTP server")
            sys.exit(1)
        
        # Start HTTP server in background thread
        def run_server():
            try:
                server.serve_forever()
            except Exception as e:
                print(f"❌ HTTP server error: {e}")
        
        health_thread = threading.Thread(target=run_server, daemon=True)
        health_thread.start()
        
        # Give the server a moment to start
        time.sleep(2)
        print("✅ HTTP server ready for health checks")
        
        # Start the scheduler
        print("📅 Starting scheduler...")
        scraper.run_scheduled()
        
    except KeyboardInterrupt:
        print("\n⏹️ Scraper stopped by user")
    except Exception as e:
        print(f"❌ Error starting scraper: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
