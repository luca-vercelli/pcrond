#!/usr/bin/env python

from pcrond import scheduler

crontab_filename = None


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Launch a crond-like daemon in userspace.')
    parser.add_argument('filename', help='the crontab file')
    args = parser.parse_args()

    global crontab_filename
    crontab_filename = args.filename


if __name__ == "__main__":
    parse_args()
    scheduler.load_crontab_file(crontab_filename)
    scheduler.main_loop()
