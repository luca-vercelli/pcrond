
import logging
logger = logging.getLogger('schedule')

MONTH_OFFSET = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
WEEK_OFFSET = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5,
               'sat': 6}
ALIASES = {'@yearly':  '0 0 1 1 *',
           '@annually':  '0 0 1 1 *',
           '@monthly': '0 0 1 * *',
           '@weekly':  '0 0 * * 0',
           '@daily':   '0 0 * * *',
           '@hourly':  '0 * * * *',
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

        if crontab in ALIASES.keys():
            crontab = ALIASES[crontab]

        crontab_lst = crontab.split()

        if crontab_lst[0] in ALIASES.keys():
            raise ValueError("Cannot mix @Aliases and other tokens")

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

        self.must_calculate_last_dom = (-1 in self.allowed_dom)

        if -1 in self.allowed_years:
            raise ValueError(("Wrong format '%s' : 'L' is meaningless talking about Years") % crontab_lst[5])

        self.crontab_pattern = crontab_lst

    def _parse_token(self, token, offsets):
        """
        return int(token), possibly replacing token with offsets[token]
        """
        if token in offsets.keys():
            newtoken = offsets[token]
            try:
                return int(newtoken)
            except ValueError:
                raise ValueError("token %s maps to %s, however the latter is not an integer" % (token, newtoken))
        try:
            return int(token)
        except ValueError:
            raise ValueError(("token %s is not an integer, nor it is a known constant") % token)

    def _parse_common(self, s, maxval, offsets={}):
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
        if "L" not in offsets:
            offsets["L"] = maxval-1
        if s == "*":
            return [True, []]           # every minute
        elif s.startswith("*/"):        # every tot minutes
            try:
                step = int(s[2:])
            except ValueError:
                raise ValueError(
                    "Wrong format '%s' - expecting an integer after '*/'" % s)
            return [False, set(range(0, maxval, step))]
        else:                           # at given minutes
            # DEBUG
            # import pdb
            # pdb.set_trace()
            # here "1,2-5,jul,10-L"
            ranges = s.split(",")
            # here ["1","2-5","jul","10-L"]
            ranges = [x.split("-") for x in ranges]
            # here [["1"],["2","5"],["aug"], ["10","L"]]
            ranges = [[self._parse_token(w, offsets) for w in x] for x in ranges]
            # here [[1],[2,5],[7], [10,11]]
            if max([len(x) for x in ranges]) > 2:
                raise ValueError(
                    "Wrong format '%s' - a string x-y-z is meaningless" % s)
            ranges_xp = [x for x in ranges if len(x) == 1]
            ranges_xp.extend([range(x[0], x[1]+1) for x in ranges if len(x) == 2 and x[0] <= x[1]])
            ranges_xp.extend([range(x[0], maxval) for x in ranges if len(x) == 2 and x[0] > x[1]])
            ranges_xp.extend([range(0, x[1]+1) for x in ranges if len(x) == 2 and x[0] > x[1]])
            # here [[2,3,4,5], [10, 11]]
            flatlist = [z for rng in ranges_xp for z in rng]
            return [False, set(flatlist)]

    def _parse_min(self, s):
        return self._parse_common(s, 60)

    def _parse_hour(self, s):
        return self._parse_common(s, 24)

    def _parse_month(self, s):
        return self._parse_common(s, 12, MONTH_OFFSET)

    def _parse_day_in_week(self, s):
        return self._parse_common(s, 7, WEEK_OFFSET)

    def _parse_year(self, s):
        """ to put things simple, I assume a range of years between 0 and 3000
        This is mostly useless. """
        return self._parse_common(s, 3000, {"L": -1})

    def _parse_day_in_month(self, s):
        return self._parse_common(s, 31, {"L": -1})    # this works by chance

    def _check_day_in_month(self, now):
        if self.must_calculate_last_dom:
            # this is a hack for avoiding to calculate "L" when not needed
            import calendar
            last_day_of_month = calendar.monthrange(now.year, now.month)[1]
            if now.day == last_day_of_month:
                return True
        return now.day in self.allowed_dom

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
        return not self.running \
            and (self.allowed_every_year or now.year in self.allowed_years) \
            and (self.allowed_every_month or now.month in self.allowed_months) \
            and (self.allowed_every_dow or now.weekday() in self.allowed_days_in_week) \
            and (self.allowed_every_hour or now.hour in self.allowed_hours) \
            and (self.allowed_every_minute or now.minute in self.allowed_minutes) \
            and (self.allowed_every_dom or self._check_day_in_month(now))

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
