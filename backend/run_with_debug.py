#!/usr/bin/env python3
"""
Run API with full error logging
"""
import sys
import logging

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_errors.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Import and run
try:
    import uvicorn
    from api import app
    
    print("=" * 60)
    print("Starting API with DEBUG logging...")
    print("Logs will be written to: api_errors.log")
    print("=" * 60)
    
    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="debug")
    
except Exception as e:
    logging.exception("FATAL ERROR starting API:")
    print(f"\n\nERROR: {e}")
    import traceback
    traceback.print_exc()
