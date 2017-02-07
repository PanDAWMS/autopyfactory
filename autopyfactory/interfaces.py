import logging

class SchedInterface(object):
    '''
    -----------------------------------------------------------------------
    Calculates the number of jobs to be submitted for a given queue. 
    -----------------------------------------------------------------------
    Public Interface:
            calcSubmitNum()
    -----------------------------------------------------------------------
    '''
    def calcSubmitNum(self, nsub=0):
        '''
        Calculates number of jobs to submit for the associated APF queue. 
        '''
        raise NotImplementedError


class BatchStatusInterface(object):
    '''
    -----------------------------------------------------------------------
    Interacts with the underlying batch system to get job status. 
    Should return information about number of jobs currently on the desired queue. 
    -----------------------------------------------------------------------
    Public Interface:
            getInfo()
            getJobInfo()
    
    Returns BatchStatusInfo object
     
    -----------------------------------------------------------------------
    '''
    def getInfo(self, queue=None, maxtime=0):
        '''
        Returns aggregate statistics about jobs in batch system. Indexed by queue.
        If queue is provided, returns just the secondary object containing aggregate info
        about that queue.  
        '''
        raise NotImplementedError

    def getJobInfo(self, queue=None, maxtime=0):
        '''
        Returns per-job info about jobs in batch system. Indexed by queue. 
        If queue is provided, returns just the secondary object containing aggregate info
        about that queue.  
        '''
        raise NotImplementedError
    

class BatchHistoryInterface(object):
    def getInfo(self, queue=None, maxtime=0):
        raise NotImplementedError
    def getXYZ(self, queue=None, maxtime=0):
        raise NotImplementedError





class WMSStatusInterface(object):
    '''
    -----------------------------------------------------------------------
    Interface for all WMSStatus plugins. 
    Should return information about cloud status, site status and jobs status. 
    -----------------------------------------------------------------------
    Public Interface:
            getCloudInfo()
            getSiteInfo()
            getJobsInfo()
    -----------------------------------------------------------------------
    '''
    def getCloudInfo(self, cloud=None, maxtime=0):
        '''
        Method to get and updated picture of the cloud status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getSiteInfo(self, site=None, maxtime=0):
        '''
        Method to get and updated picture of the site status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getInfo(self, queue=None, maxtime=0):
        '''
        Method to get and updated picture of the jobs status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError


class ConfigInterface(object):
    '''
    -----------------------------------------------------------------------
    Reads configuration from different sources to create a ConfigLoader
    object for the APFQueues configuration
    -----------------------------------------------------------------------
    Public Interface:
            getConfig()
    -----------------------------------------------------------------------
    '''
    def getConfig(self):
        '''
        returns a ConfigLoader object 
        '''
        raise NotImplementedError


class BatchSubmitInterface(object):
    '''
    -----------------------------------------------------------------------
    Interacts with underlying batch system to submit jobs. 
    It should be instantiated one per queue. 
    -----------------------------------------------------------------------
    Public Interface:
            submit(number)
            addJSD()
            writeJSD()
    -----------------------------------------------------------------------
    '''
    def submit(self, n):
        '''
        Method to submit pilots.
        Returns list of JobInfo objects 
        Returns list of JobInfo objects representing successfully submitted jobs. 
        The JobInfo must have a jobid attribute
        '''
        raise NotImplementedError

    def addJSD(self):
        '''
        Adds content to the JSD file
        '''
        raise NotImplementedError
        
    def writeJSD(self):
        '''
        Writes on file the content of the JSD file
        '''
        raise NotImplementedError

    def retire(self, n):
        '''
        Primarily relevant for EC2 VM jobs. Tells APF to connect to this many
        *startds* and retire all jobs running.  
        
        '''
        raise NotImplementedError

    def cleanup(self):
        '''
        Provides a method that gets called unconditionally every cycle. 
        
        '''
        raise NotImplementedError


class MonitorInterface(object):
    '''
    Interface for publishing job info to external monitors/dashboards/loggers.
    ------------------------------
    Public Interface:
    
    
    ''' 
    def registerFactory(self, apfqueue):
        '''
        Initial startup hello message from new factory...
        
        '''
        raise NotImplementedError
    
    
    def sendMessage(self, text):
        '''
        Send message to monitor, if it supports this function. 
        '''
        raise NotImplementedError
    
    
    def updateJobs(self, jobinfolist ):
        '''
        Update information about job/jobs. 
        Should support either single job object or list of job objects.  
         
        '''
        raise NotImplementedError
   

# ================================================================================
#       THREADS INTERFACE
# ================================================================================

import threading
import time


class _thread(threading.Thread):
    
    def __init__(self):
        
        self.log = logging.getLogger('_thread')

        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()
        # to avoid the thread to be started more than once
        self._thread_started = False 
        # recording last time the actions were done
        self._thread_last_action = 0
        # time to wait before checking again if the threads has been killed
        self._thread_abort_interval = 1
        # time to wait before next loop
        self._thread_loop_interval = 1
        self.log.debug('object _thread initialized')

         
    def start(self):
        # this methods is overriden
        # to prevent the thread from being started more than once.
        # That could happen if the final threading class
        # implements the design pattern Singleton.
        # In that cases, multiple copies of the same object
        # may be instantiated, and eventually "started"
        
        if not self._thread_started:
            self.log.debug('starting thread')
            self._thread_started = True
            threading.Thread.start(self)


    def run(self):
        self.log.debug('starting run()')
        self._prerun()
        self._mainloop()
        self._postrun()
        self.log.debug('leaving run()')
    

    def _prerun(self):
        '''
        actions to be done before starting the main loop
        '''
        # default implementation is to do nothing
        pass

    
    def _postrun(self):
        '''
        actions to be done after the main loop is finished
        '''
        # default implementation is to do nothing
        pass

    
    def _mainloop(self):
        while not self.stopevent.isSet():
            try:                       
                if self._check_for_actions():
                    self._run()
                    self._thread_last_action = int( time.time() )
            except Exception, e:
                if self._propagate_exception():
                    raise e
                if self._abort_on_exception():
                    self.join()
                self._thread_last_action = int( time.time() )
            self._wait_for_abort()


    def _check_for_actions(self):
        '''
        checks if a new loop of action should take place
        '''
        # default implementation
        now = int(time.time())
        check = (now - self._thread_last_action) > self._thread_loop_interval
        return check


    def _wait_for_abort(self):
        '''
        waits for the loop to be aborted because the thread has been killed
        '''
        time.sleep( self._thread_abort_interval )


    def _propagate_exception(self):
        '''
        boolean to decide if the Exception needs to be propagated. 
        Defaults to False.
        '''
        # reimplement this method if response is not unconditionally False
        return False 


    def _abort_on_exception(self):
        '''
        boolean to decide if the Exception triggers the thread to be killed. 
        Defaults to False.
        '''
        # reimplement this method if response is not unconditionally False
        return False 


    def _run(self):
        raise NotImplementedError


    def join(self,timeout=None):
        if not self.stopevent.isSet():
            self.log.debug('joining thread')
            self.stopevent.set()
            self._join()
            threading.Thread.join(self, timeout)


    def _join(self):
        pass
