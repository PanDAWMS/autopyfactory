#!/usr/bin/env python
'''
   Convenience utilities for AutoPyFactory.
'''


import os
import signal
import subprocess
import threading
import time


class TimeOutException(Exception):
       pass

class ExecutionFailedException(Exception):
       pass


class TimedCommand(object):
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

def fill(object, dictionary, mapping=None):
    '''
    function to fill an object with info 
    comming from a dictionary.

    Each key of the dictionary is supposed 
    to be one attribute in the object.

    For example, if object is instance of class
        class C():
            def __init__(self):
                self.x = ...
                self.y = ...
    then, the dictionary should look like
        d = {'x': ..., 'y':...}

    In case the dictionary keys and object attributes
    do not match, a dictionary mapping can be passed. 
    For exmaple, the object is instance of class
        class C():
            def __init__(self):
                self.x = ...
                self.y = ...
    and the dictionary look like
        d = {'a': ..., 'b':...}
    then, the mapping must be like
        mapping = {'a':'x', 'b':'y'}
    '''
    for k,v in dictionary.iteritems():
        if mapping:
            k = mapping[k]
        setattr(object, k, v)

def add(object, dictionary, mapping=None):
    '''
    function to add values from a dictionary
    to values already stored in an object.

    Each key of the dictionary is supposed 
    to be one attribute in the object.

    For example, if object is instance of class
        class C():
            def __init__(self):
                self.x = ...
                self.y = ...
    then, the dictionary should look like
        d = {'x': ..., 'y':...}

    In case the dictionary keys and object attributes
    do not match, a dictionary mapping can be passed. 
    For exmaple, the object is instance of class
        class C():
            def __init__(self):
                self.x = ...
                self.y = ...
    and the dictionary look like
        d = {'a': ..., 'b':...}
    then, the mapping must be like
        mapping = {'a':'x', 'b':'y'}
    '''

    for k,v in dictionary.iteritems():
        if mapping:
            k = mapping[k]
        current = object.__getattribute__(k)
        new = current + v
        setattr(object,k,new)

def checkDaemon(daemon, pattern='running'):
    '''
    checks if a given daemon service is active
    '''
    import commands 
    status = commands.getoutput('service %s status' %daemon)
    return status.lower().find(pattern) > 0


def which(file):
    for path in os.environ["PATH"].split(":"):
        if os.path.exists(path + "/" + file):
                return path + "/" + file


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

