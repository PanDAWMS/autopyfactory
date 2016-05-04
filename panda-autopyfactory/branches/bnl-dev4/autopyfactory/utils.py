#!/usr/bin/env python
'''
   Convenience utilities for AutoPyFactory.
'''

import popen2

__author__ = "Jose Caballero"
__copyright__ = "2011, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class CommandLine(object):
        '''
        ------------------------------------------------------------------
        class to execute a program in the command line
        and get the output, error, and return code

        Making use of this class would allow to centralize
           - checking the executable exists, 
           - checking versions, 
           - retrials in case of failure,
           - raise an exception is case of failure,
           - change the way to perform the shell command
             (i.e. from popen2 to subprocess when all hosts migrate to python3)
           - etc. 

        Also can be used for executed commands bookkeeping.
        ------------------------------------------------------------------
        Public Interface:
                * Methods:
                        - __init__(program, exception=None)
                        - __str__()
                        - __call__()
                        - execute(tmp_options='', failure_message='', exception=None)
        ------------------------------------------------------------------
        '''
        def __init__(self, cmd, check=True, exception=None):

                self.cmd = cmd       # program to be executed
                self.output = None   # the std output after execution
                self.error = None    # the std error after execution
                self.status = None   # the return code after execution

        def execute(self, failure_message='', exception=None):
                '''
                Executes the program, if possible

                - failure message is a message to display is execution fails
                - exception is to be raised in case the execution fails
                '''

                ###status, output = commands.getstatusoutput(self.cmd)
                popen = popen2.Popen3(self.cmd, capturestderr=True) 
                output = popen.fromchild.read()
                error = popen.childerr.read() 
                status =  popen.wait() >> 8

                #removing the last '\n' char
                if output:
                        output = output[:-1]  
                if error:
                        error = error[:-1]
                        
                self.output = output
                self.error = error
                self.status = status

                if self.status != 0:
                        if failure_message:
                                print failure_message
                        if exception:
                                raise exception

                def __str__(self):
                        return self.command
                
                def __call__(self):
                        return self.command
                

if __name__ == '__main__':

        print '-----------------------------------------'
        cmd1 = '/bin/ls -ltr /tmp/'
        exe1 = CommandLine(cmd1)
        exe1.execute()
        print exe1.output
        print exe1.error
        print exe1.status
        print '-----------------------------------------'
        cmd2 = '/bin/ls -ltr /tmpx/'
        exe2 = CommandLine(cmd2)
        exe2.execute()
        print exe2.output
        print exe2.error
        print exe2.status
        print '-----------------------------------------'







