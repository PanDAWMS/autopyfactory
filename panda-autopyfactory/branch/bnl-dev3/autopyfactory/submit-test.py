#!/usr/bin/env python 

import commands
from submit import *

print '--------------------------------'
print 'TEST 1'
l = JSDDirective('var = value')
print l.isvalid()
print l
print l()
print l.gettemplate()
print '--------------------------------'
print 'TEST 2'
l = JSDDirective('var = @@value@@')
print l.isvalid()
try:
        print l
except JSDDirectiveException, ex:
        print ex
print l()
print l.gettemplate()
l.replace('real value')
print l.isvalid()
print l
print '--------------------------------'
print 'TEST 3'
l2 = JSDDirective(l)
print l.isvalid()
print l
print '--------------------------------'
print 'TEST 4'
f = JSDFile()
l1 = JSDDirective('# this is a comment')
l2 = JSDDirective('var1 = @@some_value_here1@@')
l3 = JSDDirective('var2 = @@some_value_here2@@')
l4 = JSDDirective('queue')
f.add(l1)
f.add(l2)
f.add(l3)
f.add(l4)
print f
try:
        f.write('./out')
        # this is a nasty trick, but good enough for this test
        print commands.getoutput('cat ./out')
        commands.getoutput('rm ./out')
except JSDFileException, ex:
        print ex
print '--------------------------------'
print 'TEST 5'
# this is a nasty tric, but good enough for this test
fjld = open('template.jdl', 'w')
print >> fjld ,'# this is a test'
print >> fjld ,'var1 = value1'
print >> fjld ,'var2 = value2'
print >> fjld ,'queue'
fjld.close()
f2 = JSDFile(templatefile='template.jdl')
print f2
commands.getoutput('rm template.jdl')
print '--------------------------------'
print 'TEST 6'
f3 = JSDFile(templatejsd=f)
print f3.isvalid()
print f3
print '--------------------------------'
print 'TEST 7'
toreplace = {}
toreplace['some_value_here1'] = 'value1'
toreplace['some_value_here2'] = 'value2'
f3.replace(toreplace)
print f3
print f3.isvalid()
f.write('./out2')
# this is a nasty trick, but good enough for this test
print commands.getoutput('cat ./out2')
commands.getoutput('rm ./out2')
print '--------------------------------'
print 'TEST 8'
f4 = JSDFile(templateurl='http://www.usatlas.bnl.gov/~caballer/template.jdl')
print f4
print '--------------------------------'
