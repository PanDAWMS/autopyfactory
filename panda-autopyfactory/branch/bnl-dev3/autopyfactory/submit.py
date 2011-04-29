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


class JSD(object):
        """class to create and handle the job submission file creation.
        This class intents to facilitate the creation and handling of the
        job submission file for the different submission mechanisms that 
        AutoPyFactory will be using (or back-ends).

        The constructor __init__ could eventually accept a template with 
        some pre-prepared submission file. 
        Possible format for this template are:
                - a file 
                - an url

        The constructor __init__ could also accept as input another object
        of the same class, used as initial content. That will allow having
        layers of objects, each one with different sets of attributes. 

        The generated submission content can be retrieved in two ways:
                - writing it into a file
                - providing for the equivalent string

        The class implements a sanity check that validates all to-be-replaced
        attributes from the input template have a valid value. An exception 
        will be raised otherwise.
        """

        def __init__(self, templatefile=None, templateurl=None, templatejsd=None):
                if templatefile:
                        # a txt template file is passed as input
                        # It is parsed, and all variables are recorded as attributes
                        self.__clonetemplatefromfile(templatefile)
                elif templateurl:
                        # An url containing a template file is passed as input
                        # It is parsed, and all variables are recorded as attributes
                        self.__clonetemplatefromurl(templateurl)
                elif templatejsd:
                        # Another object is passed as input.
                        # All its attributes are copied. 
                        self.__clonejsd(templatejsd)

        def __clonetemplatefromfile(self, templatefile):
                templatefd = open(templatefile)
                for line in templatefd.readlines():
                        line = line[:-1]  # removing the \n
                        key, value = line.split('=')
                        self.__setattr__(key.strip(), value.strip())
                templatefd.close()

        def __clonetemplatefromurl(self, url):
                # to be implemented
                pass

        def __clonejsd(self, jsd):
                for k,v in jsd.__dict__.iteritems():
                        self.__setattr__(k,v)

        def write(self, path):
                """calls __str__ and prints out the result in a file
                """
                jsdcontent = self.__str__()
                if jsdcontent:
                        jsdfile = open(path, 'w')
                        print >> jdlfile, jsdcontent
                        jsdfile.close()

        def __str__(self):
                """loop over all attributes, creates an string 
                with the content of the object, and returns it. 
                """
                # FIXME: just a temporary solution.
                # It should be better, it should raise an exception
                if not self.__sanitycheck():
                        return False

                out = ''
                for attr in self.__dict__.iteritems():
                        out += '%s = %s\n' %attr
                out += 'queue'  # FIXME: this is a temporary solution 
                return out

        def __sanitycheck(self):
                """checks that all variables have a valid value and no
                one from the templates still need to be replaced.
                A regexp will look for the pattern:
                        <something>@<something>@<something>
                """

                import re
                pattern = '^.*@.+@.*$'

                for value in self.__dict__.values():
                        if re.match(pattern, value):
                                return False
                # if everything went fine...
                return True
