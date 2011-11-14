#!/usr/bin/env python
'''
   Convenience utilities for AutoPyFactory.
'''


import os
import signal
import subprocess
import threading
import time

__author__ = "Jose Caballero"
__copyright__ = "2011, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class TimeOutException(Exception):
       pass

class ExecutionFailedException(Exception):
       pass


class CommandLine(object):
        """
        -----------------------------------------------------------------------
        class to run shell commands.
        It encapsulates calls to subprocess.Popen()
        Can implement a timeout and abort execution if needed.
        Can print a custom failure message and/or raise custom exceptions.
        -----------------------------------------------------------------------
        Public Interface:
                __init__(): inherited from threading.Thread
                self.output
                self.error 
                self.status
                self.pid   
                self.time  
        -----------------------------------------------------------------------
        """
        
        def __init__(self, cmd, timeout=None, failure_msg=None, exception=None):
                
                class SubProcess(threading.Thread):
                        def __init__(self, program):
                                threading.Thread.__init__(self)
                                self.program   = program
                                self.output    = None
                                self.error     = None
                                self.status    = None
                                self.pid       = None

                        def run(self):
                                self.p = subprocess.Popen(self.program, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
                                self.pid = self.p.pid
                                self.output = self.p.stdout.read()
                                self.error = self.p.stderr.read()
                                self.status  = self.p.wait()


                self.timeout = timeout
                self.failure_msg = failure_msg
                self.exception = exception

                self.cmd = SubProcess(cmd)

                now = time.time()
                self.run()
                self.time = time.time() - now

                self.checkoutput()

        def run(self):
                
                self.cmd.start()

                if self.timeout:
                        while self.cmd.isAlive() and self.timeout > 0:
                                time.sleep(1)
                                self.timeout -= 1
                        if not self.timeout > 0:
                                os.kill(self.cmd.pid, signal.SIGKILL)
                                raise TimeOutException

                self.cmd.join()

                self.output = self.cmd.output
                self.error  = self.cmd.error
                self.status = self.cmd.status
                self.pid    = self.cmd.pid

        def checkoutput(self):

                if self.status != 0:
                        if self.failure_msg:
                                print self.failure_message
                        if self.exception:
                                raise self.exception


if __name__ == "__main__":
                
        try:
                #cmd = CommandLine('ls -ltr /tmpp/', exception=ExecutionFailedException)
                #cmd = CommandLine('ls -ltr /tmp/', exception=ExecutionFailedException)
                cmd = CommandLine('for i in a b c d e f g h; do echo $i; sleep 1; done', 2)
                #cmd = CommandLine('for i in a b c d e f g h; do echo $i; sleep 1; done')
                print '=================='
                print cmd.output
                print '------------------'
                print cmd.error
                print '------------------'
                print cmd.status
                print '------------------'
                print cmd.pid
                print '------------------'
                print cmd.time
                print '=================='
        except TimeOutException:
                print 'timeout'
        except ExecutionFailedException:
                print 'failed'

