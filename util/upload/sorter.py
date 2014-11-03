#!/usr/bin/env python3

# Produces output in the following form:
#
# WARCPATH [tab] TEXTPATH [tab] GROUP
#
# where GROUP is a month and year string.
#
# Use with chunker.py.

import sys
import glob
import re
import fileinput
import subprocess
import shlex
from datetime import datetime

warc_pattern = re.compile('.*/twitpic2-image_([^_]+)_([^-]+)-.*warc\.gz')
txt_pattern = re.compile('.*/twitpic2-scrape-image_([^_]+)_([^\.]+)\.txt\.gz')
ts_pattern = re.compile('timestamp:[^:]+:(.+)')

matches = dict()

class Group(object):
    warc = None
    datefile = None

    def is_complete(self):
        return self.warc != None and self.datefile != None

for entry in sys.stdin:
    fn = entry.rstrip()
    key = None
    wm = re.match(warc_pattern, fn)

    if wm:
        key = (wm.group(1), wm.group(2))
        matches.setdefault(key, Group())
        matches[key].warc = fn
    else:
        tm = re.match(txt_pattern, fn)

        if tm:
            key = (tm.group(1), tm.group(2))
            matches.setdefault(key, Group())
            matches[key].datefile = fn

    if key and matches[key].is_complete():
        group = matches[key]

        output = subprocess.check_output('zcat %s | grep timestamp | head -n1' % shlex.quote(group.datefile),
                shell=True, universal_newlines=True)
        tsm = re.match(ts_pattern, output)

        if tsm:
            dt = datetime.strptime(tsm.group(1), '%Y-%m-%d %H:%M:%S')
            print('%s\t%s\t%s' % (group.warc, group.datefile, dt.strftime('%b_%Y')))
        else:
            print('unable to read date from %s' % group.datefile, file=sys.stderr)
        
        del matches[key]
