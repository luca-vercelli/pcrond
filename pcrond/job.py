
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

        # Easy ones:
        [self.allowed_every_min, self.allowed_min] = self._parse_min(crontab_lst[0])
        [self.allowed_every_hour, self.allowed_hours] = self._parse_hour(crontab_lst[1])
        [self.allowed_every_month, self.allowed_months] = self._parse_month(crontab_lst[3])
        [self.allowed_every_year, self.allowed_years] = self._parse_year(crontab_lst[5])

        # Day of month.
        # L = last day
        # 15W= nearest working day around the 15th, in the same month
        [self.allowed_every_dom, self.allowed_dom, self.allowed_wdom] = self._parse_day_in_month(crontab_lst[2])

        # Day of week.
        # 5L = last friday of the month
        # 5#2 = second friday of the month
        [self.allowed_every_dow, self.allowed_dow, self.allowed_dowl, self.allowed_dow_sharp] = \
            self._parse_day_in_week(crontab_lst[4])

        self.allowed_last_dom = (-1 in self.allowed_dom)

        self.crontab_pattern = crontab_lst

    def _decode_token(self, token, offsets):
        """
        return offsets[token], or token if not found
        offset keys are **lowercase**
        """
        try:
            return offsets[token]
        except KeyError:
            return token

    def _split_tokens(self, s):
        """
        identify ranges in pattern
        given "1,2-5/3,jul,10-goofy/6" return two lists, the singletons ['1', 'jul']
        and the ranges [['10', 'goofy', 1], ['2', '5', 3]]
        * and @ not supported
        :return: two lists, single items and ranges
        """
        # here '1,2-5/3,jul,10-goofy'
        ranges = s.split(",")
        # here ranges == ['1', '2-5/3', 'jul', '10-goofy']
        ranges = [x.split("/") for x in ranges]
        # here ranges == [['1'], ['2-5', '3'], ['jul'], ['10-goofy']]
        ranges = [[x[0].split("-")] + x[1:] for x in ranges]
        # here ranges == [[['1']], [['2', '5'], '3'], [['jul']], [['10', 'goofy']]]
        if max([len(x) for x in ranges]) > 2:
            raise ValueError("Wrong format '%s' - a string x/y/z is meaningless" % s)
        if max([len(x) for z in ranges for x in z]) > 2:
            raise ValueError("Wrong format '%s' - a string x-y-z is meaningless" % s)
        if [x for x in ranges if len(x) == 2 and len(x[0]) == 1]:
            raise ValueError("Wrong format '%s' - a string y/z is meaningless, should be x-y/z" % s)
        singletons = [w for x in ranges for z in x for w in z if len(z) == 1 and len(x) == 1]
        # here singletons == ['1', 'jul']
        ranges_no_step = [x[0] + [1] for x in ranges if len(x) == 1 and len(x[0]) == 2]
        # here ranges_no_step == [['10', 'goofy', 1]]
        try:
            ranges_with_step = [x[0] + [int(x[1])] for x in ranges if len(x) == 2 and len(x[0]) == 2]
        except ValueError:
            raise ValueError("Wrong format '%s' - expecting an integer after '/'" % s)
        # here ranges_with_step == [['2', '5', 3]]
        return (singletons, ranges_no_step + ranges_with_step)

    def _parse_common(self, s, minval, maxval, offsets={}, callback=None):
        """
        Generate a set of integers, corresponding to "allowed values".
        Work for minute, hours, weeks, month, ad days of week, because they
        are all "similar".
        Does not work for '*'
        :param minval, maxval:
            es. 0-59 for minutes, 1-12 for month, ...
        :param offsets:
            a dict mapping names (es. "mar") to their offsets (es. 2).
        :param minval:
            es. 0 for hours and minutes, 1 for days and months
        :param callback:
            a 2-ary function that pre-elaborates singletons and ranges
        """
        if s.startswith("*/"):        # every tot minutes
            try:
                step = int(s[2:])
            except ValueError:
                raise ValueError("Wrong format '%s' - expecting an integer after '*/'" % s)
            return set(range(minval, maxval + 1, step))
        else:                           # at given minutes
            # here s == '1,2-5/3,jul,10-nov'
            (singletons, ranges) = self._split_tokens(s)
            # here singletons == ['1', 'jul'], ranges == [['2', '5', 3], ['10', 'nov', 1]]
            singletons = [self._decode_token(x, offsets) for x in singletons]
            ranges = [[self._decode_token(rng[0], offsets), self._decode_token(rng[1], offsets), rng[2]]
                      for rng in ranges]
            if callback is not None:
                (singletons, ranges) = callback(singletons, ranges)
            singletons = map(int, singletons)           # may raise ValueError
            ranges = [map(int, rng) for rng in ranges]   # may raise ValueError
            # here singletons == [1, 7], ranges == [[2, 5, 3], [10, 11, 1]]
            ranges = [range(rng[0], rng[1] + 1, rng[2]) for rng in ranges if (rng[0] <= rng[1])] + \
                     [range(rng[0], maxval + 1, rng[2]) for rng in ranges if rng[0] > rng[1]] + \
                     [range(minval, rng[1] + 1, rng[2]) for rng in ranges if rng[0] > rng[1]]
            # here ranges == [range(2, 5, 3), range(10, 11, 1]]
            flatlist = singletons + [z for rng in ranges for z in rng]
            # here flatlist == [1, 7, 2, 3, 4, 5, 10, 11]
            return set(flatlist)

    def _parse_min(self, s):
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 0, 59)]

    def _parse_hour(self, s):
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 0, 23)]

    def _parse_day_in_month(self, s):
        if s == '*':
            return [True, None, None]

        def ignore_w(singletons, ranges):
            if [x for x in ranges for z in x if 'w' in x]:
                raise ValueError("Cannot use W pattern inside ranges")
            return ([x for x in singletons if x[-1] != 'w'], ranges)

        def only_w(singletons, ranges):
            return ([x[:-1] for x in singletons if x[-1] == 'w'], [])

        dom = self._parse_common(s, 1, 31, {'l': '-1'}, ignore_w)
        wdom = self._parse_common(s, 1, 31, {}, only_w)

        return [False, dom, wdom]

    def _parse_month(self, s):
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 1, 13, MONTH_OFFSET)]

    def _parse_day_in_week(self, s):
        if s == '*':
            return [True, None, None, None]
        def only_plain(singletons, ranges):
            if [x for x in ranges for z in x if ('l' in x or '#' in x)]:
                raise ValueError("Cannot use L or # pattern inside ranges")
            return ([x for x in singletons if not ('l' in x or '#' in x)], ranges)

        def only_l(singletons, ranges):
            return ([x[:-1] for x in singletons if x[-1] == 'l'], [])

        def only_sharp(n):
            suffix = '#' + str(n)
            lens = len(suffix)
            return lambda singletons, ranges: ([x[:-lens] for x in singletons if x.endswith(suffix)], [])

        # warning: in Python, Monday is 0 and Sunday is 6
        #          in cron, Sunday=0 and Saturday is 6
        def cron2py(x):
            return (x + 6) % 7

        dow = self._parse_common(s, 0, 6, WEEK_OFFSET, only_plain)
        dow = set(map(cron2py, dow))
        dow_l = self._parse_common(s, 0, 6, WEEK_OFFSET, only_l)
        dow_l = set(map(cron2py, dow_l))
        dow_s = {}
        for n in range(1, 6):
            t = self._parse_common(s, 0, 6, WEEK_OFFSET, only_sharp(n))
            dow_s[n] = set(map(cron2py, t))
        return [False, dow, dow_l, dow_s]

    def _parse_year(self, s):
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 1970, 2099)]

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
