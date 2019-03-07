MONTH_OFFSET = {'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 'may': '5', 'jun': '6',
                'jul': '7', 'aug': '8', 'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12'}
WEEK_OFFSET = {'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3', 'thu': '4', 'fri': '5', 'sat': '6'}


class Parser:
    """
    This class is just a library of "class" methods, used to parse crontab strings
    """

    def decode_token(self, token, offsets):
        """
        return offsets[token], or token if not found
        offset keys are **lowercase**
        """
        try:
            return offsets[token]
        except KeyError:
            return token

    def split_tokens(self, s):
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
            (singletons, ranges) = self.split_tokens(s)
            # here singletons == ['1', 'jul'], ranges == [['2', '5', 3], ['10', 'nov', 1]]
            singletons = [self.decode_token(x, offsets) for x in singletons]
            ranges = [[self.decode_token(rng[0], offsets), self.decode_token(rng[1], offsets), rng[2]]
                      for rng in ranges]
            if callback is not None:
                (singletons, ranges) = callback(singletons, ranges)
            singletons = list(map(int, singletons))           # may raise ValueError
            ranges = [list(map(int, rng)) for rng in ranges]   # may raise ValueError
            # here singletons == [1, 7], ranges == [[2, 5, 3], [10, 11, 1]]
            ranges = [range(rng[0], rng[1] + 1, rng[2]) for rng in ranges if (rng[0] <= rng[1])] + \
                     [range(rng[0], maxval + 1, rng[2]) for rng in ranges if rng[0] > rng[1]] + \
                     [range(minval, rng[1] + 1, rng[2]) for rng in ranges if rng[0] > rng[1]]
            # here ranges == [range(2, 5, 3), range(10, 11, 1]]
            flatlist = singletons + [z for rng in ranges for z in rng]
            # here flatlist == [1, 7, 2, 3, 4, 5, 10, 11]
            return set(flatlist)

    def parse_minute(self, s):
        """
        :return: [run_on_every_minute: boolean,
                  allowed_minutes: list or None]
        """
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 0, 59)]

    def parse_hour(self, s):
        """
        :return: [run_on_every_hour: boolean,
                  allowed_hours: list or None]
        """
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 0, 23)]

    def parse_day_in_month(self, s):
        """
        :return: [run_on_every_day: boolean,
                  allowed_days: list or None,
                  run_on_last_day_of_month: boolean,
                  allowed_weekdays: list or None]
        """
        if s == '*':
            return [True, None, False, None]

        def ignore_w(singletons, ranges):
            if [x for x in ranges for z in x if 'w' in x]:
                raise ValueError("Cannot use W pattern inside ranges")
            return ([x for x in singletons if x[-1] != 'w'], ranges)

        def only_w(singletons, ranges):
            return ([x[:-1] for x in singletons if x[-1] == 'w'], [])

        dom = self._parse_common(s, 1, 31, {'l': '-1'}, ignore_w)
        wdom = self._parse_common(s, 1, 31, {}, only_w)

        return [False, dom, -1 in dom, wdom]

    def parse_month(self, s):
        """
        :return: [run_on_every_month: boolean,
                  allowed_months: list or None]
        """
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 1, 13, MONTH_OFFSET)]

    def parse_day_in_week(self, s):
        """
        :return: [run_on_every_weekday: boolean,
                  allowed_weekdays: list or None,
                  allowed_days_in_last_weeks_of_month: list or None,
                  allowed_days_in_specific_weeks_of_month: dict ]
        """
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

    def parse_year(self, s):
        """
        :return: [run_on_every_year: boolean,
                  allowed_years: list or None]
        """
        if s == '*':
            return [True, None]
        return [False, self._parse_common(s, 1970, 2099)]
