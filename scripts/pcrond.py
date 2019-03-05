#!/usr/bin/env python

import logging

VERSION = "1.0"
logger = logging.getLogger()


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Launch a crond-like daemon in userspace.')
    parser.add_argument('-r', '--crontabfile',
                        help='the crontab file (default ~/.local/crontab)',
                        default='~/.local/crontab')
    parser.add_argument('-l', '--logfile',
                        help='the log file (default ~/.local/pcrond.log)',
                        default='~/.local/pcrond.log')
    parser.add_argument('-v', '--version', action='store_true', help='print version then exit')
    parser.add_argument('-x', '--debug', action='store_true', help='enable debug logging')
    args = parser.parse_args()
    return args


def setup_logger(args):             # HOPE this affects modules too
    logginglevel = logging.DEBUG if args.debug else logging.INFO
    handler = logging.handlers.RotatingFileHandler(filename=args.logfile,
                                                   level=logginglevel,
                                                   maxBytes=2000,
                                                   backupCount=10)
    logger.addHandler(handler)


if __name__ == "__main__":
    args = parse_args()
    if args.version:
        print(VERSION)
        exit(0)
    setup_logger(args)

    from pcrond import scheduler
    scheduler.load_crontab_file(args.crontabfile)
    scheduler.main_loop()
