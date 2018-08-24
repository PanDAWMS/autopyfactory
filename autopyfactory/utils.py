#!/usr/bin/env python
"""
   Convenience utilities for AutoPyFactory.
"""


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
                print( self.failure_message)
            if self.exception:
                raise self.exception


def checkDaemon(daemon, pattern='running'):
    """
    checks if a given daemon service is active
    """
    cmd = 'service %s status' %daemon
    subproc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = subproc.communicate()
    st = subproc.returncode
    return out.lower().find(pattern) > 0
    


def which(file):
    for path in os.environ["PATH"].split(":"):
        if os.path.exists(path + "/" + file):
                return path + "/" + file


class Container(object):
    """
    generic class that is built from a dictionary content
    """

    def __init__(self, input_d):

        self.input_d = input_d
        for k, v in self.input_d.items():
            self.__dict__[k] = v

    def __getattr__(self, name):
        """
        Return None for non-existent attributes, otherwise behave normally.         
        """
        try:
            return int(self.__getattribute__(name))
        except AttributeError:
            return None

    def __str__(self):
        s = 'Info Container ='
        for k, v in self.input_d.items():
            s += ' %: %s' %(k, v)
        return s


def remap(d, mapping, add_f=lambda x,y: x+y):
    """
    converts a dictionary into another dictionary
    changing keys (and aggregating values) 
    based on a mappings dictionary
    """
    out = {}
    for k, v in d.items():
        k = mapping[k]
        if k not in out.keys():
            out[k] = v
        else:
            out[k] = add_f(out[k], v)
    return out
    
# ================================================== 

if __name__ == "__main__":
        
    try:
        #cmd = CommandLine('ls -ltr /tmpp/', exception=ExecutionFailedException)
        #cmd = CommandLine('ls -ltr /tmp/', exception=ExecutionFailedException)
        cmd = CommandLine('for i in a b c d e f g h; do echo $i; sleep 1; done', 2)
        #cmd = CommandLine('for i in a b c d e f g h; do echo $i; sleep 1; done')
        print( '==================')
        print( cmd.output )
        print( '------------------')
        print( cmd.error )
        print( '------------------')
        print( cmd.status )
        print( '------------------')
        print( cmd.pid )
        print( '------------------')
        print( cmd.time )
        print( '==================')
    except TimeOutException:
        print( 'timeout' )
    except ExecutionFailedException:
        print( 'failed' )

