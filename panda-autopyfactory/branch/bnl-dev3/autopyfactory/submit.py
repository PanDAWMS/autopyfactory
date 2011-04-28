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

        def __init__(self, templatefile=None, templateurl=None, templatejdl=None):
                # a txt template file is passed as input
                # It is parsed, and all variables are recorded as attributes
                if templatefile:
                        templatef = open(templatefile)
                        for line in templatef.readlines():
                                line = line[:-1]  # removing the \n
                                key, value = line.split('=')
                                self.__setattr__(key.strip(), value.strip())

                # An url containing a template file is passed as input
                # It is parsed, and all variables are recorded as attributes
                if templateurl:
                        pass 

                # Another object is passed as input.
                # All its attributes are copied. 
                if templatejdl:
                        for k,v in templatejdl.__dict__.iteritems():
                                self.__setattr__(k,v)


        def write(self, path):
                """loop over all attributes that have been added
                dinamically to an object JSD, and print out
                all of them in a file.
                """

                # FIXME: just a temporary solution. It should be better.
                if not self.__sanitycheck():
                        return False

                jdlfile = open(path, 'w')
                for attr in self.__dict__.iteritems():
                        print >> jdlfile, '%s = %s' %attr
                print >> jdlfile, 'queue'
                jdlfile.close()

        def __str__(self):
                """creates an string with the content of the object
                and returns it. 
                """

                # FIXME: just a temporary solution. It should be better.
                if not self.__sanitycheck():
                        return False

                out = ''
                for attr in self.__dict__.iteritems():
                        out += '%s = %s\n' %attr
                out += 'queue' 
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
