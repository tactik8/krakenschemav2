from setuptools import setup

setup(
    name='krakenschema',
    version='0.1.2',
    description='kraken schema module',
    author='Tactik8',
    author_email='admin@tactik8.com',
    packages=['krakenschema'],  #same as name
    install_requires=['requests'], #external packages as dependencies
)
