# run.py
# load and run unittests
import unittest
import os.path


def load_tests():
    start_dir = os.path.split(__file__)[0]
    top_level = os.path.normpath(os.path.join(start_dir, "../.."))

    l = unittest.TestLoader()
    t = l.discover(start_dir, top_level_dir=top_level)
    return t
