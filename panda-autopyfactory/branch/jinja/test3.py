#!/usr/bin/env python  

from jinja2 import Environment, PackageLoader, FileSystemLoader

#env = Environment(loader=PackageLoader('app','templates'))
#template = env.get_template('template.html')

loader = FileSystemLoader(searchpath=".")
env = Environment( loader=loader )
template = env.get_template('app/templates/template.html')

d = []

#d.append({'txt':'Some text here 1', 'url':'http:blah blah 1', 'filled':True} )
#d.append({'txt':'Some text here 2', 'url':'http:blah blah 2', 'filled':False} )

class I:
    def __init__(self, txt, url, filled):
        self.txt = txt
        self.url = url
        self.filled = filled

d.append(I("some text here 1", "http: bla blah 1", True))
d.append(I("some text here 1", "http: bla blah 1", True))
d.append(I("some text here 2", "http: bla blah 2", False))

template.stream(d=d).dump('table.html')
