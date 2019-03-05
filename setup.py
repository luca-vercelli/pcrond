#!/usr/bin/env python
# this is the standard way of installing a Python module, using distutils:
# sudo ./setup.py install
# or
# ./setup.py install --prefix=~/.local
# uninstall is not provided, see https://stackoverflow.com/questions/1550226

from distutils.core import setup

setup(name='pcrond',
      version='1.0',
      description='Userspace cron daemon',
      author='Luca Vercelli',
      author_email='luca.vercelli.to@gmail.com',
      url='https://github.com/luca-vercelli/pcrond',
      packages=['pcrond'],
      scripts=['scripts/pcrond.py'],
      )
