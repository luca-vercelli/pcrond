# most of the code here comes from https://github.com/dbader/schedule

from .job import Job, ALIASES
import logging
import time

logger = logging.getLogger('pcrond')


def std_launch_func(cmd_splitted, stdin=None):
    """
    Default way of executing commands is to invoke subprocess.run()
    """
    if stdin is None:
        def f():
            from subprocess import run
            run(cmd_splitted)
            # not returning anything here
    else:
        def f():
            from subprocess import Popen, PIPE
            p = Popen(cmd_splitted, stdin=PIPE)
            p.communicate(input=stdin)
            # not returning anything here
    return f


class Scheduler(object):
    """
    Objects instantiated by the :class:`Scheduler <Scheduler>` are
    factories to create jobs, keep record of scheduled jobs and
    handle their execution.
    """
    def __init__(self):
        self.delay = 60         # in seconds
        self.jobs = []
        self.ask_for_stop = False

    def run_pending(self):
        """
        Run all jobs that are scheduled to run.
        Please note that it is *intended behavior that run_pending()
        does not run missed jobs*. For example, if you've registered a job
        that should run every minute and you only call run_pending()
        in one hour increments then your job won't be run 60 times in
        between but only once.
        """
        runnable_jobs = (job for job in self.jobs if job.should_run())
        for job in runnable_jobs:
            job.run()

    def run_all(self, delay_seconds=0):
        """
        Run all jobs regardless if they are scheduled to run or not.
        A delay of `delay` seconds is added between each job. This helps
        distribute system load generated by the jobs more evenly
        over time.
        :param delay_seconds: A delay added between every executed job
        """
        logger.info('Running *all* %i jobs with %is delay inbetween',
                    len(self.jobs), delay_seconds)
        for job in self.jobs[:]:
            job.run()
            time.sleep(delay_seconds)

    def clear(self):
        """
        Deletes scheduled jobs
        """
        del self.jobs[:]
        logger.info("jobs cleared")

    def cancel_job(self, job):
        """
        Delete a scheduled job.
        If the job is running it won't be stopped.
        :param job: The job to be unscheduled
        """
        try:
            self.jobs.remove(job)
        except ValueError:
            pass

    def cron(self, crontab, job_func):
        """
        Create a job and add it to this Scheduler
        :param crontab:
            string containing crontab pattern
            Its tokens may be either: 1 (if alias), 5 (without year token),
            6 (with year token)
        :param job_func:
            the job 0-ary function to run
        :return: a Job
        """
        job = Job(crontab, job_func, self)
        self.jobs.append(job)
        return job

    def _load_crontab_line(self, rownum, crontab_line, job_func_func=std_launch_func, stdin=None):
        """
        create a Job from a single crontab entry, and add it to this Scheduler
        :param crontab_line:
            a line from crontab
            PRE: not empty and it not a comment
        :param job_func_func:
            function to be executed, @see load_crontab_file
        :return: a Job
        """
        pieces = crontab_line.split()

        if pieces[0] in ALIASES.keys():
            try:
                # CASE 1 - pattern using alias
                job = self.cron(pieces[0], job_func_func(pieces[1:]))
                return job
            except ValueError as e:
                # shouldn't happen
                logger.error(("Error at line %d, cannot parse pattern, the line will be ignored.\r\n" +
                              "Inner Exception: %s") % (rownum, str(e)))
                return None
        if len(pieces) < 6:
            logger.error("Error at line %d, expected at least 6 tokens" % rownum)
            return None
        if len(pieces) >= 7:
            try:
                # CASE 2 - pattern including year
                job = self.cron(" ".join(pieces[0:6]), job_func_func(pieces[6:]))
                return job
            except ValueError:
                pass
        try:
            # CASE 3 - pattern not including  year
            job = self.cron(" ".join(pieces[0:5]), job_func_func(pieces[5:]))
            return job
        except ValueError as e:
            logger.error(("Error at line %d, cannot parse pattern, the line will be ignored.\r\n" +
                          "Inner Exception: %s") % (rownum, str(e)))
            return None

    def _split_input_line(self, s):
        """
        Command is split in command and stdin using %, not %%
        :return: two strings, command and stdin
        """
        # s == aaaa%%bbbbbb%cccc%dd%%ee
        pieces = [x.split('%') for x in s.split('%%')]
        # pieces == [[aaaa],[bbbbbb,cccc,dd],[ee]]
        rejoin = "%".join(["\n".join(x) for x in pieces])
        # rejoin == aaaa%bbbbbb\ncccc\ndd%ee
        return rejoin.split('\n', 1)
        # lines == [aaaa%bbbbbb,ccc\ndd%ee]

    def load_crontab_file(self, crontab_file, clear=True, job_func_func=std_launch_func):
        """
        Read crontab file, create corresponding jobs in this scheduler
        :param crontab_file:
            crontab file path
        :param job_func_func:
            a function that takes a list of tokens (from crontab file) and
            returns a 0-args function
        :param clear:
            should the new schedule override the previous ones?
        """
        if clear:
            self.clear()
        with open(crontab_file) as fp:
            for rownum, line in enumerate(fp):
                if line is not None:                  # not sure if this can happen
                    line = line.strip()
                    if line != "" and line[0] != "#":
                        # skip empty lines and comments
                        pieces = self._split_input_line(line)
                        stdin = pieces[1] if len(pieces) > 1 else None
                        self._load_crontab_line(rownum, pieces[0], job_func_func, stdin)
                        # TODO support % sign inside command, should consider pieces[1] if any
        logger.info(str(len(self.jobs)) + " jobs loaded from configuration file")

    def main_loop(self):
        """
        Perform main run-and-wait loop.
        """
        import time
        while not self.ask_for_stop:
            self.run_pending()
            time.sleep(self.delay)
