#!/usr/bin/env python 

from submit import *

print '--------------------------------'
l = JSDDirective('var = value')
print l.isvalid()
print l
print l()
print l.gettemplate()
print '--------------------------------'
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
l2 = JSDDirective(l)
print l.isvalid()
print l
print '--------------------------------'
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
except JSDFileException, ex:
        print ex
print '--------------------------------'
f2 = JSDFile(templatefile='template.jdl')
print f2
print '--------------------------------'
f3 = JSDFile(templatejsd=f)
print f3.isvalid()
print f3
print '--------------------------------'
toreplace = {}
toreplace['some_value_here1'] = 'value1'
toreplace['some_value_here2'] = 'value2'
f3.replace(toreplace)
print f3
print f3.isvalid()
f.write('./out2')
print '--------------------------------'
