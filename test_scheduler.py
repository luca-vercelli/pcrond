#!/usr/bin/env python
"""Unit tests for pcrond.py"""
import datetime
import unittest

from pcrond import scheduler, Job


def do_nothing():
    pass


def modify_obj(obj):
    def f():
        obj['modified'] = True
    return f


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
        assert job.allowed_hours == set([0, 3, 6, 9, 12, 15, 18, 21])
        ###
        assert job.allowed_every_dom
        assert not job.must_calculate_last_dom
        ###
        assert not job.allowed_every_month
        assert job.allowed_months == set([3, 4, 5, 6, 12])
        ###
        assert not job.allowed_every_dow
        assert job.allowed_dow == set([1])
        ###
        assert job.allowed_every_year

    def test_job_constructor_L(self):
        job = Job("* * L * *")
        assert job.must_calculate_last_dom
        assert job.allowed_dom == set([-1])
        assert job._check_day_in_month(datetime.datetime(2019, 3, 31))
        assert not job._check_day_in_month(datetime.datetime(2019, 3, 28))
        assert job._check_day_in_month(datetime.datetime(2019, 2, 28))

    def test_job_constructor_reverse_order(self):
        job = Job("* 23-4 * * *")
        assert job.allowed_hours == set([23, 0, 1, 2, 3, 4])

    def test_job_constructor_wrong(self):
        with self.assertRaises(ValueError):
            Job("some silly text")
        with self.assertRaises(ValueError):
            Job("* * goofy * *")
        with self.assertRaises(ValueError):
            Job("* * * * * L")
        with self.assertRaises(ValueError):
            Job("* 1-2-3 * *")
        with self.assertRaises(ValueError):
            Job("* 1;2;3 * *")
        # currently, hour=25 does not raise errors.

    def test_misconfigured_job_wont_break_scheduler(self):
        """
        Ensure an interrupted job definition chain won't break
        the scheduler instance permanently.
        """
        scheduler.cron("* * * * *", do_nothing)
        with self.assertRaises(ValueError):
            scheduler.cron("some very bad string pattern", do_nothing)
        scheduler.run_pending()

    def test_add_job_run_all(self):
        test_obj = {'modified': False}
        scheduler.cron("* * * * *", modify_obj(test_obj))
        assert len(scheduler.jobs) == 1
        scheduler.run_all()
        assert test_obj['modified']

    def test_add_job_run_pending(self):
        test_obj = {'modified': False}
        now = datetime.datetime.now()
        scheduler.cron("%d %d * * *" % (now.minute, now.hour), modify_obj(test_obj))
        assert len(scheduler.jobs) == 1
        scheduler.run_pending()
        assert test_obj['modified']

    def test_load_crontab(self):
        import os
        scheduler.load_crontab_file(os.path.join("tests","crontab.txt"))
        assert len(scheduler.jobs) == 3

if __name__ == '__main__':
    unittest.main()
