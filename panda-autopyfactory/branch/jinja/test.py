from jinja2 import Environment, PackageLoader

env = Environment(loader=PackageLoader('yourapplication', 'templates'))

template = env.get_template('mytemplate.html')

#print template.render(url='variables', username='here', foo='bar')
d = {}
d['url'] = 'http://my.host.gov'
d['username'] = 'jcaballero'
d['foo'] = 'bar'
#print template.render(d)

template.stream(d).dump('hello.html')




