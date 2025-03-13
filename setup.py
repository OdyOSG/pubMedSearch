from setuptools import setup, find_packages


# Standard Library Imports
import os
import re
import time
import xml.etree.ElementTree as ET


setup(
    name="epamPubMedSearch",  # Replace with your package name
    version="0.0.1",          # Initial version
    author="EPAM",
    author_email="",
    description="PubMed Search",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/OdyOSG/pubMedSearch/tree/main",  # Replace with your repo URL
    packages=find_packages(),  # Automatically find sub-packages
    install_requires=[
      "pyspark",
      "numpy",
      "pandas",
      "requests",
      "langchain_openai",
      "PubMedFetcher"
    ]
    #install_requires=open("requirements.txt").read().splitlines()  # Dependencies
    # classifiers=[
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: MIT License",
    #     "Operating System :: OS Independent",
    # ],
    # python_requires=">=3.6",  # Specify Python version compatibility
)
