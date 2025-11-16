#!/usr/bin/env python
"""
Simple script to run the API server.
"""

import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from api.server import app

if __name__ == '__main__':
    print("Starting ETL API server on http://localhost:5000")
    print("API endpoints:")
    print("  POST /upload - Upload a file")
    print("  GET  /schema?source_id=<id> - Get schema")
    print("  GET  /schema/history?source_id=<id> - Get schema history")
    print("  GET  /records?source_id=<id> - Get records")
    print("  POST /query - Execute query")
    print("  GET  /health - Health check")
    print("\nPress Ctrl+C to stop the server\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)

