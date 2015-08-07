

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
    
