python crond
============
Userspace cron daemon

Some of the code was taken from https://github.com/dbader/schedule, release under MIT license.

This project is not interested in the "human stuff" of the original problem.
We want to launch processes in the same way crond does, and we want to do that in userspace.





.. image:: https://api.travis-ci.org/dbader/schedule.svg?branch=master
        :target: https://travis-ci.org/dbader/schedule

.. image:: https://coveralls.io/repos/dbader/schedule/badge.svg?branch=master
        :target: https://coveralls.io/r/dbader/schedule

.. image:: https://img.shields.io/pypi/v/schedule.svg
        :target: https://pypi.python.org/pypi/schedule


Features
--------
- A simple to use API for scheduling jobs.
- Very lightweight and no external dependencies.
- Excellent test coverage.
- Tested on Python 2.7, 3.5, and 3.6

Usage
-----

.. code-block:: bash

    $ python ./setup.py
    $ ./pcrond.py path/to/my/crontab/file
    

It is also possible to use this library within youir python program, howebver this is not the intended use.
If you want to do this, just mimic what pcrond.py does.

.. code-block:: python

    from pcrond import scheduler
    ...

    