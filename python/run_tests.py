import unittest
import os

def run_tests():
    script_dir = os.path.dirname(__file__)
    test_dir = os.path.join(script_dir, "test/mapreduce")
    suite = unittest.TestLoader().discover(test_dir, pattern="*_test.py")
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    run_tests()