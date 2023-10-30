'''
This file is used to install the package.
'''

from setuptools import setup, find_packages

setup(
    name='svspyed',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # list the dependencies here
        'numpy',
        'pandas',

    ],
)
