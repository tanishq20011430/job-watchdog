"""
Job Watchdog v2.0
Intelligent Multi-Source Job Alert System for India
"""

__version__ = "2.0.0"
__author__ = "Tanishq"

from .orchestrator import JobWatchdog, main, run

__all__ = ["JobWatchdog", "main", "run", "__version__"]
