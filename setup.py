"""Setuptools entry point."""

import os
from setuptools import setup, find_packages

import codecs

from lambdautils import __version__, __author__

DESCRIPTION = "Utilities for AWS Lambda functions"""
dirname = os.path.dirname(__file__)

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    if os.path.isfile("README.md"):
        long_description = codecs.open(os.path.join(dirname, "README.md"),
                                       encoding="utf-8").read()
    else:
        long_description = DESCRIPTION

setup(
    name="lambdautils",
    version=__version__,
    author=__author__,
    author_email="german@findhotel.net",
    url="https://github.com/humilis/humilis-lambdautils",
    license="MIT",
    description=DESCRIPTION,
    long_description=long_description,
    packages=find_packages(include=["lambdautils"]),
    install_requires=[
        "raven",
        "six",
        "retrying"],
    classifiers=[
        "Programming Language :: Python :: 2.7"],
    zip_safe=False
)
