import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="dbxconfig",
    version="0.1.0",
    description="Databricks Configuration Framework",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://dbxconfig.readthedocs.io/en/latest/",
    project_urls={
        'GitHub': 'https://github.com/semanticinsight/dbxconfig',
        'Documentation': 'https://dbxconfig.readthedocs.io/en/latest/'
    },
    author="Shaun Ryan",
    author_email="shaun_chiburi@hotmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    packages=[
        "dbxconfig"],
    install_requires=[
          'pyspark',
          'PyYAML',
          'jinja2',
          'pydantic'
      ],
    zip_safe=False
)
