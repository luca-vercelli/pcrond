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

    def _test_job_constructor_basic(self):
        job = Job("* * * * *")
        assert len(job.allowed_min) == 60
        assert len(job.allowed_hours) == 24
        assert len(job.allowed_months) == 12
        assert len(job.allowed_days_of_week) == 7
        assert len(job.allowed_days_of_month) == 31
        assert datetime.datetime.now().year in job.allowed_years
        assert not job.allowed_last_day_of_month

    def _test_job_constructor_more_complicated(self):
        job = Job("30 4 * mar-jun,dec mon")
        assert job.allowed_min == set([30])
        assert job.allowed_hours == set([4])
        assert job.allowed_months == set([3,4,5,6,12])
        assert job.allowed_days_of_week == set([1])
        assert len(job.allowed_days_of_month) == 31
        assert datetime.datetime.now().year in job.allowed_years
        assert not job.allowed_last_day_of_month

    def _test_job_constructor_L(self):
        job = Job("* * L * *")
        assert job.allowed_last_day_of_month
        assert job.allowed_days_of_month == set([-1])
        assert job._check_day_in_month(datetime.datetime(2019,3,31))
        assert not job._check_day_in_month(datetime.datetime(2019,3,28))
        assert job._check_day_in_month(datetime.datetime(2019,2,28))

    def test_job_constructor_reverse_order(self):
        job = Job("* 23-4 * * *")
        print(job.allowed_hours)
        assert job.allowed_hours == set([23,0,1,2,3,4])

    def _test_misconfigured_job_wont_break_scheduler(self):
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
