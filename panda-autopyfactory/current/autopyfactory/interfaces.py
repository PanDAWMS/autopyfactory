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
            valid()
    -----------------------------------------------------------------------
    '''
    def calcSubmitNum(self, nsub=0):
        '''
        Calculates number of jobs to submit for the associated APF queue. 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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
            valid()
    
    Returns BatchStatusInfo object
     
    -----------------------------------------------------------------------
    '''
    def getInfo(self, maxtime=0):
        '''
        Returns aggregate info about jobs in batch system. 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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
            valid()
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

    def valid(self):
        '''
        Says if the object has been initialized properly
        '''
        raise NotImplementedError


class ConfigInterface(object):
    '''
    -----------------------------------------------------------------------
    Returns info to complete the queues config objects
    -----------------------------------------------------------------------
    Public Interface:
            getInfo()
            valid()
    -----------------------------------------------------------------------
    '''
    def getConfig(self):
        '''
        returns info 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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
            valid()
            addJSD()
            writeJSD()
    -----------------------------------------------------------------------
    '''
    def submit(self, n):
        '''
        Method to submit pilots 
        '''
        raise NotImplementedError

    def valid(self):
        '''
        Says if the object has been initialized properly
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


class MonitorInterface(object):
    '''
    Interface for publishing job info to external monitors/dashboards/loggers.
    ------------------------------
    Public Interface:
    
    
    '''
    def __init__(self):
        self.msg = '' 

    def add2msg(self, txt):
        self.msg += txt

    def send2monitor(self):
        '''
        Sends the messages to the monitor
        '''
        self.send()
        self.msg = ''

    def send(self):
        '''
        Actually, sends the messages to the monitor, this time for real
        '''
        raise NotImplementedError
   
    def updateJobStatus(self, jobid, status ):
        '''
        Update information about a single job.  
        '''
        raise NotImplementedError
    