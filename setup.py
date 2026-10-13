from setuptools import setup, find_packages

setup(
    name="formatflick",
    version="0.0.2",
    author="Abhinaba Chakraborty",
    author_email="abhinabacr4@gmail.com",
    description="formatflick: change your file formats as you wish",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/cohitherewer/formatflick-engine",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords=["python", "file", "extension", "json", "xml", "csv", "pandas", "polars", "pyarrow", "orjson"],
    packages=find_packages(),
    install_requires=[
        "pandas>=2.1.4",
        "openpyxl>=3.1.0",
        "lxml>=5.1.0",
        "xmltodict>=0.13.0",
        "psutil>=5.9.0",
    ],
    extras_require={
        "accel": [
            "polars>=1.0.0",
            "pyarrow>=14.0.0",
            "orjson>=3.9.0",
            "ijson>=3.2.0",
            "python-calamine>=0.1.7",
            "xlsxwriter>=3.1.0",
        ],
        "dev": [
            "pytest>=8.1.1",
            "pytest-benchmark>=4.0.0",
            "requests>=2.31.0",
            "setuptools>=69.0.0",
        ],
    },
    python_requires=">=3.9",
)
