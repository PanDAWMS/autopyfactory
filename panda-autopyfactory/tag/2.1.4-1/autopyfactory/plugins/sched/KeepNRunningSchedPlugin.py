#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class KeepNRunningSchedPlugin(SchedInterface):
    '''
    This plugin strives to keep a certain number of jobs/pilots/VMs running, regardless 
    of ready/activated or input. 
    
    Understands Retiring VM job state. 
    
    May output a negative number, if keep_running is less than current running. 
      
           
    '''
    id = 'keepnrunning'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            self.keep_running = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.keepnrunning.keep_running', 'getint', logger=self.log)
            self.log.info("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, nsub=0):
        """ 
        It just returns nb of Activated Jobs - nb of Pending Pilots
        """
        input = nsub
        self.log.debug('calcSubmitNum: Starting.')

        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        self.batchinfo = self.apfqueue.batchstatus_plugin.getInfo(maxtime = self.apfqueue.batchstatusmaxtime)
        
        if self.batchinfo is None:
            self.log.warning("self.batchinfo is None!")
            out = 0
        elif not self.batchinfo.valid():
            out = 0 
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            self.key = self.apfqueue.apfqname
            self.log.info("Key is %s" % self.key)
            out = self._calc(input)
            self.log.debug("Returning %d" % out)
        return out

    def _calc(self, input):
        '''
        algorithm 
        '''
        
        # initial default values. 
        pending_pilots = 0
        running_pilots = 0
        retiring_pilots = 0

        try:
            pending_pilots = self.batchinfo[self.apfqueue.apfqname].pending  # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass
        except AttributeError:
            # This is OK--it just means no jobs. 
            pass

        try:        
            running_pilots = self.batchinfo[self.apfqueue.apfqname].running # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass
        except AttributeError:
            # This is OK--it just means no jobs. 
            pass

        try:        
            retiring_pilots = self.batchinfo[self.apfqueue.apfqname].retiring # using the new info objects
        except KeyError:
            # This is OK--it just means no jobs. 
            pass
        except AttributeError:
            # This is OK--it just means no jobs. 
            pass

        # 
        # Output is simply keep_running, minus potentially or currently running, while ignoring retiring jobs
        # 
        out = self.keep_running - ( (running_pilots - retiring_pilots) + pending_pilots)

        self.log.info('_calc() input=%s (ignored); keep_running=%s; pending=%s; running=%s; retiring=%s : Return=%s' %(input,
                                                                                         self.keep_running, 
                                                                                         pending_pilots, 
                                                                                         running_pilots,
                                                                                         retiring_pilots, 
                                                                                         out))
        return out




