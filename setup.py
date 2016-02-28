#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import lambdautils.metadata as metadata
import os

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except(IOError, ImportError, RuntimeError):
    if os.path.isfile("README.md"):
        long_description = open("README.md").read()
    else:
        long_description = metadata.description

setup(
    name=metadata.project,
    version=metadata.version,
    author=metadata.authors[0],
    author_email=metadata.emails[0],
    url=metadata.url,
    license=metadata.license,
    description=metadata.description,
    long_description=long_description,
    packages=find_packages(),
    # We do not include boto3 as an install requirement because the version of
    # boto3 shipped with the Lambda environment is fine. We specify the boto3
    # is required in the test and dev requirements files.
    install_requires=["raven"],
    classifiers=[
        "Programming Language :: Python :: 2.7"],
    zip_safe=False
)
