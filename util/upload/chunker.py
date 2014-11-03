#!/usr/bin/env python3

# Accepts input of the form
#
# WARCPATH [tab] TEXTPATH [tab] GROUP
#
# and looks for the lexicographically highest directory starting with GROUP in
# its working directory.  If it cannot find such a directory, creates one.
# For each line in the input, moves WARCPATH and TEXTPATH to the directory
# corresponding to GROUP.
# 
# WARCPATH and TEXTPATH, if relative, are assumed relative to the current
# directory.
#
# If the directory corresponding to GROUP exceeds some threshold, moves GROUP
# to the given staging directory.

import os
import sys
import fileinput
import glob
import time

# The working directory, i.e. where groups are built.
#
# Must exist.
working_dir = sys.argv[1]

if not os.path.isdir(working_dir):
	sys.exit('%s is not a directory' % working_dir)

# The staging directory, i.e. where filled-out groups go.
#
# Must exist.
staging_dir = sys.argv[2]

if not os.path.isdir(staging_dir):
	sys.exit('%s is not a directory' % staging_dir)

# The directory size threshold.  Expected to be in gigabytes.
thrarg = sys.argv[3]
threshold = int(thrarg) * (10 ** 9)

if threshold < (10 ** 9):
	sys.exit('threshold must be at least 1 GB')

current_group_dir = None
current_group_size = 0

def dirsize(path):
	total = 0

	for path, dirnames, filenames in os.walk(path):
		for fn in filenames:
			total += os.path.getsize(os.path.join(path, fn))

	return total

current_group_dirs = dict()

for spec in sys.stdin:
	warc, text, group = spec.rstrip().split('\t')

	current_group_dir = current_group_dirs.get(group)

	if current_group_dir == None:
		candidates = [d for d in os.listdir(working_dir) if group in d]
		candidates.sort(reverse=True)

		if len(candidates) == 0:
			current_group_dir = '%s/%s_%s' % (working_dir, group, int(time.time()))
			os.makedirs(current_group_dir)
			print('made new group %s' % current_group_dir)
		else:
			current_group_dir = '%s/%s' % (working_dir, candidates[0])

		current_group_size = dirsize(current_group_dir)
		current_group_dirs[group] = current_group_dir

	warcsize = os.stat(warc).st_size
	textsize = os.stat(text).st_size
	
	os.rename(warc, '%s/%s' % (current_group_dir, os.path.basename(warc)))
	os.rename(text, '%s/%s' % (current_group_dir, os.path.basename(text)))

	current_group_size += (warcsize + textsize)

	if current_group_size >= threshold:
		print('group size is %s (>= %s), creating new group' % (current_group_size, threshold), file=sys.stderr)
		os.rename(current_group_dir, '%s/%s' % (staging_dir, os.path.basename(current_group_dir)))
		del current_group_dirs[group]
