#!/usr/bin/env python
"""Setup specs for packaging, distributing, and installing MR lib."""

import distribute_setup
# User may not have setuptools installed on their machines.
# This script will automatically install the right version from PyPI.
distribute_setup.use_setuptools()


# pylint: disable=g-import-not-at-top
import setuptools


# To debug, set DISTUTILS_DEBUG env var to anything.
setuptools.setup(
    name="mapreduce",
    version="1.1.0",
    packages=setuptools.find_packages(),
    author="Kevin Sookocheff",
    author_email="ksookocheff@vendasta.com",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    keywords="google app engine mapreduce data processing",
    license="Apache License 2.0",
    description="Enable MapReduce style data processing on App Engine",
    zip_safe=True,
    # Include package data except README.
    include_package_data=True,
    exclude_package_data={"": ["README"]},
    install_requires=[
        "GoogleAppEngineCloudStorageClient >= 1.9.15",
        "pipeline >= 1.1.0",
        "Graphy >= 1.0.0"
    ]
)
