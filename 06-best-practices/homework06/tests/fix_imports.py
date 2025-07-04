#!/usr/bin/env python3
"""
Helper module to add the parent directory to the Python path
so that imports work correctly when running tests from the terminal
"""

import os
import sys

# Get the absolute path of the parent directory (homework06)
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the parent directory to the Python path if it's not already there
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
    print(f"Added {parent_dir} to Python path")

# Also add the grandparent directory (06-best-practices) to Python path
# This helps with importing homework06 as a module
grandparent_dir = os.path.dirname(parent_dir)
if grandparent_dir not in sys.path:
    sys.path.insert(0, grandparent_dir)
    print(f"Added {grandparent_dir} to Python path")
