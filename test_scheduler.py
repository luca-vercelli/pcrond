#!/usr/bin/env python
"""Unit tests for pcrond.py"""
import datetime
import unittest #use "assert" and "with self.assertRaises(ValueError)"

# Silence "missing docstring", "method could be a function",
# "class already defined", and "too many public methods" messages:
# pylint: disable-msg=R0201,C0111,E0102,R0904,R0901

from pcrond import scheduler, Job, Scheduler


def do_nothing():
    pass

class SchedulerTests(unittest.TestCase):
    def setUp(self):
        scheduler.clear()

    def test_job_constructor_basic(self):
        job = Job("* 4 * * *")
        ###
        assert job.allowed_every_min
        ###
        assert not job.allowed_every_hour
        assert job.allowed_hours == set([4])
        ###
        assert job.allowed_every_dom
        assert not job.must_calculate_last_dom
        ###
        assert job.allowed_every_month
        ###
        assert job.allowed_every_dow
        ###
        assert job.allowed_every_year

    def test_job_constructor_more_complicated(self):
        job = Job("30 */3 * mar-jun,dec mon")
        ###
        assert not job.allowed_every_min
        assert job.allowed_min == set([30])
        ###
        assert not job.allowed_every_hour
        assert job.allowed_hours == set([0,3,6,9,12,15,18,21])
        ###
        assert job.allowed_every_dom
        assert not job.must_calculate_last_dom
        ###
        assert not job.allowed_every_month
        assert job.allowed_months == set([3,4,5,6,12])
        ###
        assert not job.allowed_every_dow
        assert job.allowed_dow == set([1])
        ###
        assert job.allowed_every_year

    def test_job_constructor_L(self):
        job = Job("* * L * *")
        assert job.must_calculate_last_dom
        assert job.allowed_dom == set([-1])
        assert job._check_day_in_month(datetime.datetime(2019,3,31))
        assert not job._check_day_in_month(datetime.datetime(2019,3,28))
        assert job._check_day_in_month(datetime.datetime(2019,2,28))

    def test_job_constructor_reverse_order(self):
        job = Job("* 23-4 * * *")
        assert job.allowed_hours == set([23,0,1,2,3,4])

    def test_misconfigured_job_wont_break_scheduler(self):
        """
        Ensure an interrupted job definition chain won't break
        the scheduler instance permanently.
        """
        scheduler.add_job("* * * * *", do_nothing)
        with self.assertRaises(ValueError):
            scheduler.add_job("some very bad string pattern", do_nothing)
        scheduler.run_pending()
        
        
if __name__ == '__main__':
    unittest.main()
