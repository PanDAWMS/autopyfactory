#! /usr/bin/env python
#
#  Job submission-related code.
#
#
#  Copyright (C) 2011 Jose Caballero
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import os


class JSDDirective(object):

    def __init__(self, value):

        self.log = logging.getLogger("main.jsddirective")
        self.content= value
        self.log.info('JSDDirective: Object initialized.')

    def __str__(self):
            return self.content
    def __repr__(self):
            return self.content


class JSDDirectiveKeyValue(object):

    def __init__(self, key, value):

        self.log = logging.getLogger("main.jsddirective")
        self.key = key
        self.value = value
        self.log.info('JSDDirectiveKeyValue: Object initialized.')

    def __str__(self):
            return '%s = %s' %(self.key, self.value)
    def __repr__(self):
            return '%s = %s' %(self.key, self.value)


class JSDDirectiveHandler(object):

    def __init__(self):
        self.directives = []

    def add(self, value):
        directive = JSDDirective(value)
        self.directives.append(directive)

    def get(self):
        # return a list of strings
        return [dir for dir in self.directives]


class JSDDirectiveKeyValueHandler(object):

    def __init__(self):
        # directives = {}
        #    -- key is the directive key
        #    -- value is the entire JSDDirectiveKeyValue object
        self.directives = {}

    def add(self, key, value):
        directive = JSDDirectiveKeyValue(key, value)
        self.directives[key] = directive

    def get(self):
        # return a list of strings
        return [dir for dir in self.directives.values()]


class JSDFile(object):

    def __init__(self):

        self.log = logging.getLogger("main.jsdfile")
        self.handlers = []
        self.log.info('JSDFile: Object initialized.')

    def add(self, handler):
        self.log.debug('add: Starting.')
        self.handlers.append(handler)
        self.log.debug('add: Leaving.')

    def write(self, path, filename):
        '''
        Dumps the whole content of the JSDFile object into a disk file
        '''

        self.log.debug('writeJSD: Starting.')

        if not os.access(path, os.F_OK):
            try:
                os.makedirs(path)
                self.log.debug('writeJSD: Created directory %s', path)
            except OSError, (errno, errMsg):
                self.log.error('writeJSD: Failed to create directory %s (error %d): %s', path, errno, errMsg)
                return
        jsdfilename = os.path.join(path, filename)
        self._dump(jsdfilename)
        self.log.debug('writeJSD: the submit file content is\n %s ' %self)
        self.log.debug('writeJSD: Leaving.')
        return jsdfilename

    def _dump(self, jsdfilename):

        self.log.debug('_dump: Starting.')

        jsdfile = open(jsdfilename, 'w')

        for handler in self.handlers:
            for line in handler.get():
                print >> jsdfile, line

        jsdfile.close()

        self.log.debug('_dump: Leaving.')





# ==============================================================================

if __name__ == '__main__':

    jsd = JSDFile()

    lh1 = JSDDirectiveHandler()
    lh2 = JSDDirectiveHandler()
    dh1 = JSDDirectiveKeyValueHandler()
    dh2 = JSDDirectiveKeyValueHandler()

    jsd.add(lh1)
    jsd.add(dh1)
    jsd.add(lh2)
    jsd.add(dh2)

    lh1.add('linea 1')
    lh1.add('linea 2')

    dh1.add('key1', 'value1')
    dh1.add('key2', 'value2')

    lh2.add('linea 3')
    lh2.add('linea 4')

    dh2.add('key3', 'value3')
    dh2.add('key4', 'value4')
    dh2.add('key4', 'VALUE4')

    jsd.write('/tmp/jsd', 'out')

