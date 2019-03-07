
import logging
logger = logging.getLogger('schedule')

MONTH_OFFSET = {'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 'may': '5', 'jun': '6',
                'jul': '7', 'aug': '8', 'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12'}
WEEK_OFFSET = {'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3', 'thu': '4', 'fri': '5', 'sat': '6'}
ALIASES = {'@yearly':    '0 0 1 1 *',
           '@annually':  '0 0 1 1 *',
           '@monthly':   '0 0 1 * *',
           '@weekly':    '0 0 * * 0',
           '@daily':     '0 0 * * *',
           '@midnight':  '0 0 * * *',
           '@hourly':    '0 * * * *',
           }


class Job(object):
    """
    A periodic job as used by :class:`Scheduler`.
    """
    def __init__(self, crontab=None, job_func=None, scheduler=None):
        """
        Constructor
        :param crontab:
            string containing crontab pattern
            Its tokens may be either: 1 (if alias), 5 (without year token),
            6 (with year token)
            if None, you should set it later
        :param job_func:
            the job 0-ary function to run
            if None, you should set it later
        :param scheduler:
            scheduler to register with
            if None, you should set it later
        """
        self.job_func = job_func
        self.scheduler = scheduler
        self.running = False
        if crontab is not None:
            self.set_crontab(crontab)

    def set_crontab(self, crontab):
        if crontab is None:
            raise ValueError("given None crontab")

        crontab = crontab.lower().strip()

        if crontab in ALIASES.keys():
            crontab = ALIASES[crontab]
        elif crontab == "@reboot":
            import datetime
            now = datetime.datetime.now()
            crontab = "%d %d * %d * %d" % (now.minute + 1, now.hour, now.month, now.year)

        crontab_lst = crontab.split()

        if len(crontab_lst) == 5:
            crontab_lst.append("*")
        if len(crontab_lst) != 6:
            raise ValueError(
                "Each crontab pattern *must* contain either 1, 5 or 6 items")

        from .cronparser import Parser
        parser = Parser()

        # Easy ones:
        [self.allowed_every_min, self.allowed_min] = parser.parse_minute(crontab_lst[0])
        [self.allowed_every_hour, self.allowed_hours] = parser.parse_hour(crontab_lst[1])
        [self.allowed_every_month, self.allowed_months] = parser.parse_month(crontab_lst[3])
        [self.allowed_every_year, self.allowed_years] = parser.parse_year(crontab_lst[5])

        # Day of month.
        # L = last day
        # 15W= nearest working day around the 15th, in the same month
        [self.allowed_every_dom, self.allowed_dom, self.allowed_last_dom, self.allowed_wdom] = \
            parser.parse_day_in_month(crontab_lst[2])

        # Day of week.
        # 5L = last friday of the month
        # 5#2 = second friday of the month
        [self.allowed_every_dow, self.allowed_dow, self.allowed_dowl, self.allowed_dow_sharp] = \
            parser.parse_day_in_week(crontab_lst[4])

        self.crontab_pattern = crontab_lst

    def get_last_dom(self, now):
        """ get last day in month determined by given datetime """
        import calendar
        last_day_of_month = calendar.monthrange(now.year, now.month)[1]
        return last_day_of_month

    def get_num_wom(self, now):
        """
        for a given date, return the number of the week in the month (1..5)
        intended for #
        """
        return ((now.day - 1) // 7) + 1

    def is_last_wom(self, now):
        """ true if given date is in the last week of the month """
        return now.day > self.get_last_dom(now) - 7

    def _check_w(self, now):
        """ used for checking 15w """
        w = now.weekday()
        if w >= 5:
            return False
        d = now.day
        if d in self.allowed_wdom:
            return True
        if w == 0:
            if (d - 1) in self.allowed_wdom:
                return True
            # 1w matches monday, 3rd
            if d == 3 and 1 in self.allowed_wdom:
                return True
        elif w == 4:
            if (d + 1) in self.allowed_wdom:
                return True
            # 31w matches friday, 29th
            lday = self.get_last_dom(now)
            if d == lday - 2 and lday in self.allowed_wdom:
                return True
        return False

    def should_run(self):
        """
        :return: ``True`` if the job should be run now.
        """
        import datetime
        now = datetime.datetime.now()
        return self._should_run_at(now)

    def _should_run_at(self, now):
        """
        :return: ``True`` if the job should be run at given datetime.
        """
        # warning: in Python, Monday is 0 and Sunday is 6
        #          in cron, Sunday=0
        w = now.weekday()
        num_wom = self.get_num_wom(now)
        return (not self.running
                and (self.allowed_every_year or now.year in self.allowed_years)
                and (self.allowed_every_month or now.month in self.allowed_months)
                and (self.allowed_every_hour or now.hour in self.allowed_hours)
                and (self.allowed_every_min or now.minute in self.allowed_min)
                and (self.allowed_every_dow
                     or (w in self.allowed_dow)
                     or (self.allowed_dowl
                         and w in self.allowed_dowl
                         and self.is_last_wom(now))
                     or (self.allowed_dow_sharp[num_wom]
                         and w in self.allowed_dow_sharp[num_wom]))
                and (self.allowed_every_dom
                     or now.day in self.allowed_dom
                     or (self.allowed_last_dom and now.day == self.get_last_dom(now))
                     or (self.allowed_wdom and self._check_w(now)))
                )

    def run(self):
        """
        Run the job.
        :return: The return value returned by the `job_func`
        """
        logger.info('Running job %s', self)
        self.running = True
        ret = self.job_func()
        self.running = False
        return ret

    def run_if_should(self):
        """
        Run the job if needed.
        :return: The return value returned by the `job_func`
        """
        if self.should_run():
            return self.run()
