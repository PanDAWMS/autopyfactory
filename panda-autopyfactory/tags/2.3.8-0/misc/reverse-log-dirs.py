#!/usr/bin/env python 


import commands
import re

reverse = {}

basedir = '/home/caballer/factory/logs/'
dirRe = re.compile(r"(\d{4})-(\d{2})-(\d{2})?$")

dircandidates = commands.getoutput('ls %s' %basedir)
for dir in dircandidates.split():
    if dirRe.match(dir):
        subdirs = commands.getoutput('ls %s/%s' %(basedir, dir))
        subdirs = subdirs.split('\n')
        for subdir in subdirs:
            if subdir not in reverse.keys():
                reverse[subdir] = [dir]
            else:
                reverse[subdir].append(dir)


for queue, dates in reverse.iteritems():
    commands.getoutput('mkdir %s' %queue)
    for date in dates:
        commands.getoutput('ln -s %s/%s/%s %s/%s' %(basedir, date, queue, queue,date))


