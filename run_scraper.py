#!/usr/bin/env python3
"""
Scraper Launcher Script
This script starts the gas price scraper in scheduled mode for Railway deployment.
"""

import os
import sys
from gas_scraper import GasScraper

def main():
    print("🚀 Starting Gas Price Scraper in scheduled mode...")
    print("=" * 50)
    
    try:
        # Create scraper instance
        scraper = GasScraper()
        
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
