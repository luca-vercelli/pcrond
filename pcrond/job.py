
import logging
logger = logging.getLogger('schedule')

MONTH_OFFSET = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
WEEK_OFFSET = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6,
               '0l': -7, '1l': -6, '2l': -5, '3l': -4, '4l': -3, '5l': -2, '6l': -1}
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
    def __init__(self, crontab, job_func=None, scheduler=None):
        """
        Constructor
        :param crontab:
            string containing crontab pattern
            Its tokens may be either: 1 (if alias), 5 (without year token),
            6 (with year token)
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

        if crontab is None:
            raise ValueError("given None crontab")

        crontab = crontab.lower().strip()

        if crontab in ALIASES.keys():
            crontab = ALIASES[crontab]

        if crontab == "@reboot":
            import datetime
            now = datetime.datetime.now()
            crontab = "%d %d * %d * %d" % (now.minute+1, now.hour, now.month, now.year)

        crontab_lst = crontab.split()

        if len(crontab_lst) == 5:
            crontab_lst.append("*")
        if len(crontab_lst) != 6:
            raise ValueError(
                "Each crontab pattern *must* contain either 5 or 6 items")
        [self.allowed_every_min, self.allowed_min] = self._parse_min(crontab_lst[0])
        [self.allowed_every_hour, self.allowed_hours] = self._parse_hour(crontab_lst[1])
        [self.allowed_every_dom, self.allowed_dom] = self._parse_day_in_month(crontab_lst[2])
        [self.allowed_every_month, self.allowed_months] = self._parse_month(crontab_lst[3])
        [self.allowed_every_dow, self.allowed_dow] = self._parse_day_in_week(crontab_lst[4])
        [self.allowed_every_year, self.allowed_years] = self._parse_year(crontab_lst[5])

        self.allowed_last_dom = (-1 in self.allowed_dom)

        self.must_consider_wom = (self.allowed_dow is not None
                                  and len(self.allowed_dow) > 0
                                  and min(self.allowed_dow) < 0)

        self.crontab_pattern = crontab_lst

    def _parse_token(self, token, offsets):
        """
        return int(token), possibly replacing token with offsets[token]
        offset keys are **lowercase**
        """
        try:
            newtoken = offsets[token]
            try:
                return int(newtoken)
            except ValueError:
                # this should not happen
                raise ValueError("token %s maps to %s, however the latter is not an integer" % (token, newtoken))
        except KeyError:
            pass
        try:
            return int(token)
        except ValueError:
            raise ValueError(("token %s is not an integer, nor it is a known constant") % token)

    def _split_tokens(self, s):
        """
        given "1,2-5,jul,10-L" return [["1"],["2","5"],["jul"], ["10","L"]]
        * and @ not supported
        """
        # here "1,2-5,jul,10-L"
        ranges = s.split(",")
        # here ["1","2-5","jul","10-L"]
        ranges = [x.split("-") for x in ranges]
        # here [["1"],["2","5"],["jul"], ["10","L"]]
        return ranges

    def _explode_ranges(self, ranges, minval, maxval):
        """
        given [[1],[2,5],[7], [10,11]] return  [[1], [2,3,4,5], [7], [10, 11]] 
        """
        if max([len(x) for x in ranges]) > 2:
            raise ValueError(
                "Wrong format '%s' - a string x-y-z is meaningless" % s)
        ranges_xp = [x for x in ranges if len(x) == 1]
        ranges_xp.extend([range(x[0], x[1]+1) for x in ranges if len(x) == 2 and x[0] <= x[1]])
        ranges_xp.extend([range(x[0], maxval) for x in ranges if len(x) == 2 and x[0] > x[1]])
        ranges_xp.extend([range(minval, x[1]+1) for x in ranges if len(x) == 2 and x[0] > x[1]])
        return ranges_xp

    def _parse_common(self, s, maxval, offsets={}, minval=0):
        """
        Generate a set of integers, corresponding to "allowed values".
        Work for minute, hours, weeks, month, ad days of week, because they
        are all "similar".
        Does not work very well for years and days of month
        Supported formats: "*", "*/3", "1,2,3", "L", "1,2-5,jul,10-L", "50-10"
        :param maxval:
            es. 60 for minutes, 12 for month, ...
        :param offsets:
            a dict mapping names (es. "mar") to their offsets (es. 2).
        """
        if s == "*":
            return [True, []]           # every minute
        elif s.startswith("*/"):        # every tot minutes
            try:
                step = int(s[2:])
            except ValueError:
                raise ValueError(
                    "Wrong format '%s' - expecting an integer after '*/'" % s)
            return [False, set(range(minval, maxval, step))]
        else:                           # at given minutes
            # DEBUG
            # import pdb
            # pdb.set_trace()
            # here "1,2-5,jul,10-L"
            ranges = self._split_tokens(s)
            # here [["1"],["2","5"],["jul"], ["10","L"]]
            ranges = [[self._parse_token(w, offsets) for w in x] for x in ranges]
            ranges_xp = self._explode_ranges(ranges, minval, maxval)
            flatlist = [z for rng in ranges_xp for z in rng]
            return [False, set(flatlist)]

    def _parse_min(self, s):
        return self._parse_common(s, 60)

    def _parse_hour(self, s):
        return self._parse_common(s, 24)

    def _parse_day_in_month(self, s):
        return self._parse_common(s, 31, {"l": -1})

    def _parse_month(self, s):
        return self._parse_common(s, 12, MONTH_OFFSET)

    def _parse_day_in_week(self, s):
        return self._parse_common(s, 7, WEEK_OFFSET)

    def _parse_year(self, s):
        return self._parse_common(s, 2099, minval=1970)

    def get_last_dom(self, now):
        """ get last day in month determined by given datetime """
        import calendar
        last_day_of_month = calendar.monthrange(now.year, now.month)[1]
        return last_day_of_month

    def is_last_wom(self, now):
        """ true if given date is in the last week of the month """
        return now.day >= self.get_last_dom(now) - 7

    def __lt__(self, other):
        """
        Periodic Jobs are sortable based on the scheduled time they
        run next.
        """
        return self.next_run < other.next_run

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
        return (not self.running
                and (self.allowed_every_year or now.year in self.allowed_years)
                and (self.allowed_every_month or now.month in self.allowed_months)
                and (self.allowed_every_hour or now.hour in self.allowed_hours)
                and (self.allowed_every_min or now.minute in self.allowed_min)
                and (self.allowed_every_dow
                     or ((now.weekday() + 1) % 7) in self.allowed_dow
                     or (self.must_consider_wom
                         and (now.weekday() + 8) % 7) in self.allowed_dow
                         and self.is_last_wom(now))
                and (self.allowed_every_dom
                     or now.day in self.allowed_dom
                     or (self.allowed_last_dom and now.day == self.get_last_dom(now)))
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
