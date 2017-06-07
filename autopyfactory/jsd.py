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

        self.log = logging.getLogger('jsdfile')
        if len(self.log.parent.handlers) < 1:
            logStream = logging.StreamHandler()
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
            formatter = logging.Formatter(FORMAT)
            formatter.converter = time.gmtime  # to convert timestamps to UTC
            logStream.setFormatter(formatter)
            log.addHandler(logStream)
            log.setLevel(logging.DEBUG)
        self.lines = []
        self.log.debug('JSDFile: Object initialized.')

    def add(self, *k):
        """
        """
        self.log.debug('Starting.')
        if len(k) == 1:
            self.lines.append(k[0])
        if len(k) == 2:
            self.lines.append('%s = %s' %(k[0], k[1]))
        self.log.debug('Leaving.')

    def write(self, path, filename):
        """
        Dumps the whole content of the JSDFile object into a disk file
        """
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
        for line in self.lines: 
            print >> jsdfile, line
        jsdfile.close()
        self.log.debug('Leaving.')

