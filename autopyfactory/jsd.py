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


class JSDFile(object):

    def __init__(self):

        self.log = logging.getLogger()
        self.directive_lines = []
        self.directive_dict = {}
        self.log.debug('JSDFile: Object initialized.')

    def add(self, *k):
        """
        """

        self.log.debug('Starting.')

        if len(k) == 1:
            line = k[0]
            self.directive_lines.append(line)
        if len(k) == 2:
            key = k[0]
            value = k[1]
            self.directive_dict[key] = value

        self.log.debug('Leaving.')


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

        self.log.debug('Starting.')

        jsdfile = open(jsdfilename, 'w')

        for key,value in self.directive_dict.iteritems(): 
            print >> jsdfile, '%s = %s' %(key,value)
        for line in self.directive_lines: 
            print >> jsdfile, line

        jsdfile.close()

        self.log.debug('Leaving.')


# ==============================================================================

if __name__ == '__main__':

    jsd = JSDFile()

    jsd.add('linea 1')
    jsd.add('linea 2')

    jsd.add('key1', 'value1')
    jsd.add('key2', 'value2')

    jsd.add('linea 3')
    jsd.add('linea 4')

    jsd.add('key3', 'value3')
    jsd.add('key4', 'value4')
    jsd.add('key4', 'VALUE4')

    jsd.write('/tmp/kk/testjsd', 'out')

