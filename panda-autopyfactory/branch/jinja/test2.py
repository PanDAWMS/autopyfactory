#!/usr/bin/env python  

from jinja2 import Environment, PackageLoader

env = Environment(loader=PackageLoader('app','templates'))
template = env.get_template('template.html')

d = []

d.append({'txt':'Some text here 1', 'url':'http:blah blah 1', 'filled':True} )
d.append({'txt':'Some text here 2', 'url':'http:blah blah 2', 'filled':False} )

template.stream(d=d).dump('table.html')
