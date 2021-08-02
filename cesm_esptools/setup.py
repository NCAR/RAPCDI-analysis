import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

VERSION = '0.1.0'
PACKAGE_NAME = 'cesm_esptools'
AUTHOR = 'Elizabeth Maroon, Stephen Yeager'
AUTHOR_EMAIL = 'emaroon@wisc.edu'
URL = 'https://github.com/you/yoNCAR/RAPCDI-analysis/cesm_esptools'

LICENSE = 'MIT License'
DESCRIPTION = 'Utilities for CESM Earth System Prediction analysis'
LONG_DESCRIPTION = (HERE / "README.md").read_text()
LONG_DESC_TYPE = "text/markdown"

INSTALL_REQUIRES = [
      'numpy',
      'xarray',
      'dask',
      'cftime'
]

setup(name=PACKAGE_NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      long_description_content_type=LONG_DESC_TYPE,
      author=AUTHOR,
      license=LICENSE,
      author_email=AUTHOR_EMAIL,
      url=URL,
      install_requires=INSTALL_REQUIRES,
      packages=find_packages()
      )
