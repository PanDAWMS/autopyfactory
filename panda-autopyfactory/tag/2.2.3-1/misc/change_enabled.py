#!/usr/bin/env python   

"""
small script to change the value of enabled for a series of sections in a config file
USAGE:  ./change_enabled <config_filename> <new_value> <section1> <section2> .. <sectionN>
"""

import sys

args = sys.argv[1:]
if len(args) < 3:
    print "ERROR: usage  ./change_enabled filename newavlue sectionname1 sectionname2 ... sectionnameN"
    sys.exit()

FILENAME = args[0]
NEWVALUE = args[1]
SECTIONS = args[2:]

FILENAMES = [] 
FILENAMES.append( (FILENAME, '/tmp/%s.new' %FILENAME) )

for i in range(len(SECTIONS)-1):
    FILENAMES.append( (FILENAMES[i][1], '/tmp/%s.new.%s' %(FILENAME, i+1)) )


for i in range(len(SECTIONS)):

    section = SECTIONS[i]
    oldfilename = FILENAMES[i][0]
    newfilename = FILENAMES[i][1] 
    print 'processing section ', section

    f = open(oldfilename)
    lines = f.readlines()
    f.close()
    
    newfile = open(newfilename, 'w')
    
    issection=False
    
    for line in lines:
        line = line[:-1]
        if line == "[%s]" %section:
            issection=True
        if line.startswith('enabled') and issection == True:
            section=False
            line = 'enabled = %s' %NEWVALUE
        print >> newfile, line
    
    newfile.close()

print 'final result is in file ', FILENAMES[-1][1]
