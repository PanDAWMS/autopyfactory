__author__ = "Jose Caballero, John Hover"
__copyright__ = "2011, Jose Caballero, John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero, John Hover"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"


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
    
    Returns BatchStatusInfo object
     
    -----------------------------------------------------------------------
    '''
    def getInfo(self, maxtime=0):
        '''
        Returns aggregate statistics about jobs in batch system. Indexed by apfqeueue. 
        '''
        raise NotImplementedError

    def getJobInfo(self, maxtime=0):
        '''
        Returns per-job info about jobs in batch system. 
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
    def getCloudInfo(self, cloud, maxtime=0):
        '''
        Method to get and updated picture of the cloud status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getSiteInfo(self, site, maxtime=0):
        '''
        Method to get and updated picture of the site status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError

    def getJobsInfo(self, site, maxtime=0):
        '''
        Method to get and updated picture of the jobs status. 
        It returns a dictionary to be inserted directly into an
        Status object.
        '''
        raise NotImplementedError


class ConfigInterface(object):
    '''
    -----------------------------------------------------------------------
    Returns info to complete the queues config objects
    -----------------------------------------------------------------------
    Public Interface:
            getInfo()
    -----------------------------------------------------------------------
    '''
    def getConfig(self):
        '''
        returns info 
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
    
