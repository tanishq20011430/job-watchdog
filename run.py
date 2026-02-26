#!/usr/bin/env python3
"""
Job Watchdog v2.0 - Entry Point
Run with: python run.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.orchestrator import run

if __name__ == "__main__":
    run()
