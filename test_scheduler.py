#!/usr/bin/env python
"""Unit tests for pcrond.py"""
import unittest
import logging
import sys
from datetime import datetime as d
from pcrond import scheduler, Job, Parser

# when tests with a logger fail, you can set this to True
SHOW_LOGGING = False

logger = logging.getLogger()
if SHOW_LOGGING:
    logging.basicConfig()
else:
    logger.addHandler(logging.NullHandler())  # do not show logs.


def do_nothing():
    pass


def modify_obj(obj):
    def f():
        obj['modified'] = True
    return f


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        scheduler.clear()
        self.parser = Parser()

    def test_split_tokens(self):
        with self.assertRaises(ValueError):
            self.parser.split_tokens("1/2/3")
        with self.assertRaises(ValueError):
            self.parser.split_tokens("1-2-3")
        with self.assertRaises(ValueError):
            self.parser.split_tokens("1/3")
        with self.assertRaises(ValueError):
            self.parser.split_tokens("1-2/goofy")
        (s, r) = self.parser.split_tokens("1,2-5/3,jul,10-goofy/6")
        assert s == ['1', 'jul']
        assert r == [['2', '5', 3], ['10', 'goofy', 6]]

    def test_decode_token(self):
        assert self.parser.decode_token("1234", {}) == "1234"
        assert self.parser.decode_token("goofy", {'goofy': "1234"}) == "1234"
        assert self.parser.decode_token("goofy", {'goofy': 1234}) == 1234

    def test_get_num_wom(self):
        job = Job()
        assert job.get_num_wom(d(2019, 3, 7)) == 1      # first thursday of month
        assert job.get_num_wom(d(2019, 3, 8)) == 2      # second friday of month
        assert job.get_num_wom(d(2019, 3, 31)) == 5     # fifth sunday of month

    def test_is_last_wom(self):
        job = Job()
        assert not job.is_last_wom(d(2019, 3, 1))
        assert not job.is_last_wom(d(2019, 3, 24))
        assert job.is_last_wom(d(2019, 3, 25))
        assert job.is_last_wom(d(2019, 3, 31))

    def test_job_constructor_basic(self):
        job = Job("* 4 * * *")
        ###
        assert job.allowed_every_min
        ###
        assert not job.allowed_every_hour
        assert job.allowed_hours == set([4])
        ###
        assert job.allowed_every_dom
        assert not job.allowed_last_dom
        ###
        assert job.allowed_every_month
        ###
        assert job.allowed_every_dow
        ###
        assert job.allowed_every_year

    def test_job_constructor_more_complicated(self):
        job = Job("30 */3 * mar-jun,dec MON")
        ###
        assert not job.allowed_every_min
        assert job.allowed_min == set([30])
        ###
        assert not job.allowed_every_hour
        assert job.allowed_hours == set([0, 3, 6, 9, 12, 15, 18, 21])
        ###
        assert job.allowed_every_dom
        assert not job.allowed_last_dom
        ###
        assert not job.allowed_every_month
        assert job.allowed_months == set([3, 4, 5, 6, 12])
        ###
        assert not job.allowed_every_dow
        assert job.allowed_dow == set([0])             # 1 in cron, 0 in python
        ###
        assert job.allowed_every_year

    def test_job_constructor_L_dom(self):
        job = Job("* * L * *")
        assert not job.allowed_every_dom
        assert job.allowed_last_dom
        assert job.allowed_dom == set([-1])
        assert job._should_run_at(d(2019, 3, 31))
        assert not job._should_run_at(d(2019, 3, 28))
        assert job._should_run_at(d(2019, 2, 28))

    def test_job_constructor_W_dom(self):
        job = Job("* * 15w * *")
        assert not job.allowed_every_dom
        assert not job.allowed_last_dom
        assert job.allowed_dom == set()
        assert job.allowed_wdom == set([15])
        assert job._should_run_at(d(2019, 3, 15))       # was fri
        assert not job._should_run_at(d(2018, 12, 15))  # was sat
        assert job._should_run_at(d(2018, 12, 14))      # was fri   *
        assert not job._should_run_at(d(2018, 7, 15))   # was sun
        assert job._should_run_at(d(2018, 7, 16))       # was mon
        ###
        job = Job("* * 1w * *")
        assert not job.allowed_every_dom
        assert not job.allowed_last_dom
        assert job.allowed_dom == set()
        assert job.allowed_wdom == set([1])
        assert job._should_run_at(d(2019, 3, 1))        # was fri
        assert not job._should_run_at(d(2019, 6, 1))    # was sat
        assert job._should_run_at(d(2019, 6, 3))        # was mon

    def test_job_constructor_L_dow(self):
        job = Job("* * * * 5l")      # 5=friday, l=only the last one of the month
        assert job.allowed_every_dom
        assert not job.allowed_last_dom
        assert not job.allowed_every_dow
        assert not job.allowed_dow
        assert job.allowed_dowl == set([4])             # 5 in cron, 4 in python
        assert job._should_run_at(d(2019, 3, 29))       # was fri
        assert not job._should_run_at(d(2019, 3, 28))   # was thu
        assert not job._should_run_at(d(2019, 3, 8))    # was fri

    def test_job_constructor_dow_sharp(self):
        job = Job("* * * * 5#2")      # second friday in the month
        assert job.allowed_every_dom
        assert not job.allowed_last_dom
        assert not job.allowed_every_dow
        assert not job.allowed_dow
        assert not job.allowed_dowl
        assert 2 in job.allowed_dow_sharp.keys()
        assert job.allowed_dow_sharp[2] == set([4])      # 5 in cron, 4 in python
        assert not job._should_run_at(d(2019, 3, 1))     # was fri
        assert job._should_run_at(d(2019, 3, 8))         # was fri
        assert not job._should_run_at(d(2019, 3, 15))    # was fri
        assert not job._should_run_at(d(2019, 3, 22))    # was fri
        assert not job._should_run_at(d(2019, 3, 29))    # was fri
        assert not job._should_run_at(d(2019, 3, 28))    # was thu

    def test_job_constructor_alias(self):
        job = Job("@hourly")
        assert not job.allowed_every_min
        assert job.allowed_every_hour
        assert job.allowed_every_month
        assert job.allowed_every_year

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
        with self.assertRaises(ValueError):
            Job("* @hourly")
        with self.assertRaises(ValueError):
            Job("* L * * *")
        with self.assertRaises(ValueError):
            Job("* * 1w2 * *")
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
        """ schedule a task, then invoke run_all()"""
        test_obj = {'modified': False}
        scheduler.cron("* * * * *", modify_obj(test_obj))
        assert len(scheduler.jobs) == 1
        scheduler.run_all()
        assert test_obj['modified']

    def test_add_job_run_pending(self):
        """ schedule a task for this exact minute, then invoke run_pending()"""
        test_obj = {'modified': False}
        now = d.now()
        scheduler.cron("%d %d * * *" % (now.minute, now.hour), modify_obj(test_obj))
        assert len(scheduler.jobs) == 1
        scheduler.run_pending()
        assert test_obj['modified']

    def test_add_job_run_pending_not(self):
        """ schedule a task for this exact minute+5, then invoke run_pending()"""
        test_obj = {'modified': False}
        now = d.now()
        scheduler.cron("%d %d * * *" % (now.minute+5, now.hour), modify_obj(test_obj))
        assert len(scheduler.jobs) == 1
        scheduler.run_pending()
        assert test_obj['modified'] is False

    def test_split_input_line(self):
        assert scheduler._split_input_line('aaaa%%bbbbbb%cccc%dd%%ee') == ['aaaa%bbbbbb', 'cccc\ndd%ee']
        assert scheduler._split_input_line('aaaa%%bbbbbb') == ['aaaa%bbbbbb']

    def test_load_crontab(self):
        """ load test crontab file """
        import os
        scheduler.load_crontab_file(os.path.join("tests", "crontab.txt"))
        assert len(scheduler.jobs) == 4

    @unittest.skipIf(sys.platform.startswith("win"), "requires *NIX")
    def test_load_crontab_and_main_loop(self):
        # FIXME not working
        # and even if it worked, this is a long test, and will run on *nix only
        import os
        import time
        from threading import Thread

        def run_in_another_thread():
            scheduler.load_crontab_file(os.path.join("tests", "crontab2.txt"))
            scheduler.main_loop()

        start_time = d.now()
        thread = Thread(target=run_in_another_thread)
        thread.start()
        print("Waiting for 15 seconds...")
        time.sleep(15)
        scheduler.ask_for_stop = True
        thread.join()
        assert os.path.isfile(os.path.join("tests", "somefile"))
        assert os.path.getmtime(os.path.join("tests", "somefile")) >= d.utcfromtimestamp(start_time)

    @unittest.skipUnless(sys.platform.startswith("win"), "requires Windows")
    def test_load_crontab_and_main_loop(self):
        # TODO
        pass


if __name__ == '__main__':
    unittest.main()
