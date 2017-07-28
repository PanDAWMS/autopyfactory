
import logging

from autopyfactory.interfaces import MonitorInterface

class Dummy(MonitorInterface):
    
    def __init__(self, apfqueue, config, section):
        self.log = logging.getLogger("autopyfactory.monitor")
        self.log.debug("Dummy monitor initialized.")


    def registerFactory(self, apfqueue):
        """
        Initial startup hello message from new factory...
        
        """
        self.log.debug("registerFactory( apfqueue = %s) called." % apfqueue)
        return None
    
    
    def sendMessage(self, text):
        """
        Send message to monitor, if it supports this function. 
        """
        self.log.debug("sendMessage( text=%s) called." % text)
    
    
    def updateJobs(self, jobinfolist ):
        """
        Update information about job/jobs. 
        Should support either single job object or list of job objects.  
         
        """
        self.log.debug("updateJobs(jobinfolist=%s) called." % jobinfolist )
        return None
   
    def registerJobs(self, apfqueue, jobinfolist ):
        """
        Update information about job/jobs. 
        Should support either single job object or list of job objects.  
         
        """
        self.log.debug("registerJobs(apfqueue=%s, jobinfolist=%s) called." % ( apfqueue, jobinfolist))
        return None   
    
    def updateLabel(self, label, msg):
        """
        Update label. 
        Should support either single job object or list of job objects.  
         
        """
        self.log.debug("updateLabel(label=%s, msg=%s) called." % (label, msg))
        return None       
        
        
        
        
    
   