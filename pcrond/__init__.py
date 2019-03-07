
# Here, flake8 gives error F401 '.job.Job' imported but unused
# However I have to import that, don't I ?
# pylint: disable-msg=F401
from .job import Job
from .sched import Scheduler
from .cronparser import Parser

# default instance
scheduler = Scheduler()
