from setuptools import setup, find_packages

setup(
    name="pubmed-search",
    version="0.1.0",
    description="A Python wrapper around OHDSI/searchpubmed for RWD-focused PubMed searches",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/pubmed-search",
    author="Your Name",
    license="Apache-2.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "searchpubmed @ git+https://github.com/OHDSI/searchpubmed.git@develop#egg=searchpubmed",
        "pubMedSearch @ git+https://github.com/OdyOSG/pubMedSearch.git@develop#egg=pubMedSearch",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    zip_safe=False,
)
