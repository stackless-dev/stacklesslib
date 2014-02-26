#!/usr/bin/python
# -*- coding: utf-8 -*-

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages
setup(
    name = "stacklesslib",
    version = "0.2.0",
    packages = find_packages(),

    # metadata for upload to PyPI
    author = "Kristján Valur Jónsson",
    author_email = "sweskman@gmail.com",
    description = "A set of essential utilities for Stackless",
    license = "PSF",
    keywords = "stackless",
    url = "https://bitbucket.org/stackless-dev/stacklesslib",
    test_suite = "stacklesslib.test.run.load_tests",
    use_2to3 = True,
    use_2to3_exclude_fixers = ['lib2to3.fixes.fix_throw'],
)
