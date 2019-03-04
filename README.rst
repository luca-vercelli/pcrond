python crond
============
.. image:: https://api.travis-ci.org/luca-vercelli/schedule.svg?branch=master
        :target: https://travis-ci.org/luca-vercelli/pcrond

.. image:: https://coveralls.io/repos/luca-vercelli/pcrond/badge.svg?branch=master
        :target: https://coveralls.io/r/luca-vercelli/pcrond


Userspace cron daemon

A daemon similar to the standard `crond`, however it is designed to run in userspace, not as root.
Jobs scheduling use exactly the same formalism of crond.
Written in Python.

Some of the code was taken from https://github.com/dbader/schedule, release under MIT license.

This project is not interested in the "human stuff" of the original project.



Features (mostly taken from https://github.com/dbader/schedule)
---------------------------------------------------------------
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
    scheduler.add_job("30 4 * * 0", my_python_func)     #every sunday at 4:30
    scheduler.main_loop()

    
