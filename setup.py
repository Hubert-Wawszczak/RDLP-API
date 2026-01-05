"""Setup script for RDLP-API"""
from setuptools import setup, find_packages

setup(
    name="rdlp-api",
    version="0.1.0",
    description="API for loading RDLP data from ogcapi.bdl.lasy.gov.pl",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_dir={"": "."},
    install_requires=[
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "python-dotenv>=1.0.0",
        "asyncpg>=0.29.0",
        "aiohttp>=3.9.0",
        "aiofiles>=23.2.0",
        "shapely>=2.0.0",
        "tenacity>=8.2.0",
        "pyyaml>=6.0",
        "path>=16.0.0",
    ],
    python_requires=">=3.13",
)

