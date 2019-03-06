python crond
============
.. image:: https://api.travis-ci.org/luca-vercelli/pcrond.svg?branch=master
        :target: https://travis-ci.org/luca-vercelli/pcrond

.. image:: https://coveralls.io/repos/github/luca-vercelli/pcrond/badge.svg?branch=master
        :target: https://coveralls.io/github/luca-vercelli/pcrond?branch=master

Userspace cron daemon

A daemon similar to the standard `crond`, however it is designed to run in userspace, not as root.
Jobs scheduling use exactly the same formalism of crond.
Written in Python.

Some of the code was taken from ``schedule`` [1], release under MIT license.

This project is not interested in the "human stuff" of the original project.


Features 
--------
(well, mostly taken from [1])

- A simple to use API for scheduling jobs.
- Very lightweight and no external dependencies.
- Good test coverage.
- Tested on Python 2.7 and 3.6

Install
-----

.. code-block:: bash

    $ ./setup.py install --prefix=~/.local
    
(this assumes that ``~/.local/bin`` is in the PATH, that is quite common)

Usage
-----

.. code-block:: bash

    $ pcrond.py -r path/to/my/crontab/file
    
It is also possible to use this library within your Python program, however this is not the intended use.
For example:

.. code-block:: python

    from pcrond import scheduler
    scheduler.add_job("30 4 * * 0", my_python_func)     #every sunday at 4:30
    scheduler.main_loop()

    
[1] https://github.com/dbader/schedule
