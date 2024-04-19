import sys
import unittest
import os

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src"))

def run_tests():
    script_dir = os.path.dirname(__file__)
    suite = unittest.TestLoader().discover(script_dir, pattern="*_test.py")
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    run_tests()