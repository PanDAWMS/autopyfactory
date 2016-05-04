

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
   

# ==============================================================================                                
#                      SINGLETON CLASSES 
# ==============================================================================  

class Singleton(type):
    '''
    -----------------------------------------------------------------------
    Ancillary class to be used as metaclass to make other classes Singleton.
    -----------------------------------------------------------------------
    '''
    
    def __init__(cls, name, bases, dct):
        cls.__instance = None 
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw): 
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance


class CondorSingleton(type):
    '''
    -----------------------------------------------------------------------
    Ancillary class to be used as metaclass to make other classes Singleton.
    This particular implementation is for CondorBatchStatusPlugin.
    It allow to create different instances, one per schedd.
    Each instance is a singleton. 
    -----------------------------------------------------------------------
    '''
    
    def __init__(cls, name, bases, dct):
        cls.__instance = {} 
        type.__init__(cls, name, bases, dct)

    def __call__(cls, *args, **kw): 
        condor_q_id = kw.get('condor_q_id', 'local')
        if condor_q_id not in cls.__instance.keys():
            cls.__instance[condor_q_id] = type.__call__(cls, *args,**kw)
        return cls.__instance[condor_q_id]


def singletonfactory(id_var=None, id_default=None):
    '''
    This is an abstraction of the two previous classes. 
    We have here a metaclass factory, which will decide 
    which type of Singleton metaclass returns based on the inputs

    If id_var is not passed, then we asume a regular singleton __metaclass__ is expected.
    If id_var has a value, then it is a multi-singleton.
    We understand by multi-singleton a class that can instantiate the same object or not,
    depending on the value of id_var. Same value of id_var will generate the same object.
  
    id_var is the name of a key variable to be passed via __init__() when asking for a new object.
    The value of that variable will be the ID to determine if a real new object is needed or not.

    Note: when calling __init__(), the id_var has to be passed as a key=value variable,
    not just as a positional variable. 

    Examples:

        class A(object):
            __metaclass__ = singletonfactory()

        ---------------------------------------------------------------------

        class B(object):
            __metaclass__ = singletonfactory(id_var='condorpool', id_default='local')
        
        obj1 = B(..., condorpool='pool1', ...)
        obj2 = B(..., condorpool='pool1', ...)
        obj3 = B(..., condorpool='pool2', ...)

        obj1 and obj2 will be the same. obj3 will not. 
    '''

    class Singleton(type):

        # regular singleton __metaclass__
        if not id_var:

            def __init__(cls, name, bases, dct):
                cls.__instance = None 
                type.__init__(cls, name, bases, dct)
            def __call__(cls, *args, **kw):
                if cls.__instance is None:
                    cls.__instance = type.__call__(cls, *args,**kw)
                return cls.__instance

        # multi-singleton __metaclass__
        else:

            def __init__(cls, name, bases, dct):
                cls.__instance = {}
                type.__init__(cls, name, bases, dct)
            def __call__(cls, *args, **kw):
                id = kw.get(id_var, id_default)
                # note: we read the value of id_var from **kw
                #       so it has to be passed as a key=value variable,
                #       not as a positional variable. 
                if id not in cls.__instance.keys():
                    cls.__instance[id] = type.__call__(cls, *args,**kw)
                return cls.__instance[id]

    return Singleton

