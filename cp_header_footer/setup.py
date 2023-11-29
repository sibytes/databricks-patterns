"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the cp_header_footer project.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import cp_header_footer

setup(
    name="cp_header_footer",
    version=cp_header_footer.__version__,
    url="https://databricks.com",
    author="shaun.ryan@shaunchiburihotmail.onmicrosoft.com",
    description="wheel file based on cp_header_footer/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        # "packages": [
        #     "main=cp_header_footer.main:main"
        # ]
    },
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools"
    ],
)
