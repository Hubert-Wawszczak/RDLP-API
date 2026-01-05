#!/usr/bin/env python
"""Test runner script that sets up Python path before running tests"""
import sys
import os
from pathlib import Path

# Add project root to Python path BEFORE any imports
project_root = Path(__file__).parent.absolute()
project_root_str = str(project_root)

# Set PYTHONPATH environment variable
os.environ['PYTHONPATH'] = project_root_str + os.pathsep + os.environ.get('PYTHONPATH', '')

# Also add to sys.path
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

# Now import and run unittest
import unittest

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(project_root / 'tests'), 
        pattern='test_*.py', 
        top_level_dir=str(project_root)
    )
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
