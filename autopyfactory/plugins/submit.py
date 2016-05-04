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


import re
import urllib

from autopyfactory.utils import CommandLine

"""
Module with classes to handle the creation of the job description files
to submit jobs/pilots, i.e. the condorG file. 
The module contains 4 classes:
    - JSDDirective: a class to handle each line in the job description files.
    - JSDDirectiveException: an exception class to be used when the former 
      one is still a template and its content is requested.
    - JSDFile: a class to handle the entire job description file.
    - JSDFileException: an exception class to be used when the former 
      contains lines still a template, but the entire file content is requested. 

TO-DO: 
        * I do not like the name of classes and variables. I want to change them.
"""


class JSDDirectiveException(Exception):
        """
        class to raise exceptions when we try to print the content of
        an JSDDirective object which is a template and still not being 
        replaced.
        """
        def __init__(self, directive):
                self.directive = directive 
        def __str__(self):
                return "JSDDirective %s is still a template" %self.directive 

class JSDFileException(Exception):
        """
        class to raise exceptions when we try to print the content of
        an JSDDirective object which is a template and still not being 
        replaced.
        """
        def __init__(self):
                pass
        def __str__(self):
                return "JSDFile is still a template" 

class JSDDirective(object):
        """
        ------------------------------------------------------------------
        class to handle each line in a Job Submission Description File. 

        The constructor __init__ can accept a string or another object
        of the same class JSDDirective as input. 
        However, there is no difference,
        because the class has implemented __str__, so the object shows
        the same behaviour that a regular string.

        The class implements a sanity check that validates the content
        of the line is not a template to be replaced with a final value.
        If the content of the line is requested but still a template, 
        then an exception will be raised.
        ------------------------------------------------------------------
        Public Interface:
                * attributes:
                        * directive
                * methods:
                        * __init__(line, jsddirective)
                        * replace(value)
                        * isvalid()
                        * __str__()
                        * __call__()
                        * gettemplate()
                        * iscomment()
                        * ispair()
        ------------------------------------------------------------------
        COMMENTS:
                * I do not remember why I wrote __call__()
                * I do not like the name of the class and the attributes
        ------------------------------------------------------------------
        """

        #def __init__(self, line=None, jsddirective=None):
        #        if line:
        #                self.directive = line
        #        elif jsddirective:
        #                # this if-else is not really needed with 
        #                # the current implementation
        #                # given the method __str__ does the same
        #                # so the argument is always interpreted as 'line'
        #                # even thought when it is an JSDDirective object
        #                self.directive = jsddirective.directive
        #        else:
        #                self.directive = None

        def __init__(self, value=None):
                if not value:
                        self.directive = None
                else:
                        if type(value) == JSDDirective:
                                self.directive = value.directive
                        else:
                                self.directive = value


        def replace(self, value):
                """
                method to replace the template string by its real value, 
                when provided.
                First we check the directive is really a template, 
                and then we modify its value.
                """
                if not self.isvalid():
                        tokens = self.directive.split('@@')
                        tokens[1] = value
                        self.directive = ''.join(tokens)

        def __str__(self):
                if self.isvalid():
                        return self.directive
                else:
                        raise JSDDirectiveException(self.directive)

        # why did I write this...?
        def __call__(self):
                return self.directive

        def isvalid(self):
                """
                checks that the content has a valid value and no
                one that requires to be replaced. 
                A regexp will look for the pattern:
                        <something>@@<something>@@<something>
                """

                pattern = '^.*@@.+@@.*$'

                if re.match(pattern, self.directive):
                        return False
                return True

        def gettemplate(self):
                """
                if the directive is still a template, it returns
                the content of the string to be replaced.
                It returns False otherwise.
                """
                
                if self.isvalid():
                        return False
                else:
                        tokens = self.directive.split('@@')
                        return tokens[1]
        
        def iscomment(self):
                """
                return True if the first non-blank character is #
                """
                return self.directive.strip()[0] == '#'

        def ispair(self):
                """
                return True if the content is like 'var = value'
                """
                pattern = '^.*=.*$'
                if re.match(pattern, self.directive):
                        return True
                return False
                

class JSDFile(object):
        """
        ------------------------------------------------------------------
        class to create and handle the job submission file creation.
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

        There is a method to pass a dictionary with the list of values 
        to fill the template, when needed.
        ------------------------------------------------------------------
        Public Interface:
                * attributes:
                        * listofdirectives 
                * methods:
                        * __init__(templatefile, templateurl, templatejsd)
                        * add(directive)
                        * replace(template)
                        * isvalid()
                        * __str__()
                        * write(path)
        ------------------------------------------------------------------
        COMMENTS:
                * I do not like the name of the class and the attributes
        ------------------------------------------------------------------
        """

        def __init__(self, templatefile=None, templateurl=None, templatejsd=None):

                self.listofdirectives = []

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
                """
                get the initial input from a file
                """
                templatefd = open(templatefile)
                for line in templatefd.readlines():
                        line = line[:-1]  # removing the \n
                        #self.listofdirectives.append(JSDDirective(line=line))
                        self.listofdirectives.append(JSDDirective(line))
                templatefd.close()

        def __clonetemplatefromurl(self, url):
                """
                get the initial input from an url
                """
                socket = urllib.urlopen(url)
                data = socket.read()
                socket.close()

                for line in data.split('\n'):
                        if line.strip() != '':
                                #self.listofdirectives.append(JSDDirective(line=line))
                                self.listofdirectives.append(JSDDirective(line))


        def __clonejsd(self, jsd):
                """
                get the initial input from another object of class JSDFile
                """
                for directive in jsd.listofdirectives:
                        self.listofdirectives.append(directive)


        def add(self, directive):
                self.listofdirectives.append(JSDDirective(directive))

        def replace(self, template):
                """
                template is a dictionary with a list of pairs
                (key, value) used to complete each directive
                """

                for directive in self.listofdirectives:
                        key = directive.gettemplate()
                        if key:  # the directive is replace-able 
                                value = template.get(key)
                                if value: # the key is in the template 
                                        directive.replace(value) 

        def write(self, path):
                """
                calls __str__ and prints out the result in a file
                """

                if self.isvalid():
                        jsdcontent = self.__str__()
                        if jsdcontent:
                                jsdfile = open(path, 'w')
                                print >> jsdfile, jsdcontent
                                jsdfile.close()
                else:
                        raise JSDFileException()

        def __str__(self):
                """
                loop over all attributes, creates an string 
                with the content of the object, and returns it. 
                """
                out = '\n'.join([dir() for dir in self.listofdirectives])
                return out

        def isvalid(self):
                """
                loops over all directives in self.listofdirectives
                and check if they are valid or not, one by one.
                As soon as one non-valid directive is found the entire
                list is non-valid. 
                """
                for directive in self.listofdirectives:
                        if not directive.isvalid():
                                return False
                return True


if __name__ == '__main__':

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
        f = JSDFile()
        f.add('hi')
        f.add('world')
        print f
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
