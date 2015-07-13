#! /usr/bin/env python

import datetime
import logging
import htcondor

from autopyfactory.interfaces import SchedInterface


class ThrottleSchedPlugin(SchedInterface):
    id = 'throttle'
    
    def __init__(self, apfqueue):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger("main.schedplugin[%s]" % apfqueue.apfqname)
            try:
                self.interval = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.interval', 'getint', default_value=3600)
                self.maxtime = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.maxtime', 'getint', default_value=600)
                self.minevents = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.minevents', 'getint', default_value=0)
                self.ratioevents = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.minevents', 'getfloat', default_value=0.5)
                self.throttle = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.throttle', 'getfloat', default_value=0.5)

                self.log.debug("SchedPlugin: offset = %d" % self.offset)
            except:
                pass 
                # Not mandatory
                
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception, ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex


    def calcSubmitNum(self, n=0):
        """ 
        """

        out = n
        self.log.debug('Starting.')
        (out, msg) = self._calc(n)
        return (out, msg)


    def _calc(self, input):
        '''
        algorithm 
        '''

        # configuration variables we need:
        #   -- the interval window to look at: last 30 minutes?, last 60 minutes?, ...
        #   -- the max Wall Time to call a WN a black hole: jobs run in less than 5 minutes?, than 30 minutes?, ...
        #   -- the minimum number of black hole type jobs to start paying attention
        #           -- by number or by fraction?
        #   -- how much to throttle when a black hole is detected.



        # to convert the current date into seconds since Epoch
        now_sec_epoch = datetime.datetime.now().strftime('%s')

        # condor_history using the python bindings
        schedd = htcondor.Schedd()
        #out = schedd.history("RemoteWallClockTime < 600 && MATCH_APF_QUEUE == \"ANALY_BNL_LONG-gridgk03-htcondor\"", ['ClusterId, ProcID'], 0)

        timeinterval = int(now_sec_epoch) - self.interval
        condor_constraint_expr = "JobStartDate > %s" timeinterval
        out = schedd.history(condor_constraint_expr, ['RemoteWallClockTime', 'MATCH_APF_QUEUE'], 0)

        n_pilots = sum(1 for o in out)

        # in the constraint expression we need to add the pilot was queued after a given time
        # that given time is  
        #           now_sec_epoch - interval
        # and the Condor ClassAd to look at is QDate, or JobStartDate




         

        out = max(0, ( activated_jobs - self.offset)  - pending_pilots )
        self.log.info('input=%s; activated=%s; offset=%s pending=%s; running=%s; Return=%s' %(input,
                                                                                         activated_jobs,
                                                                                         self.offset, 
                                                                                         pending_pilots, 
                                                                                         running_pilots, 
                                                                                         out))
        msg = "Ready:in=%s;activated=%d,offset=%d,pending=%d;ret=%d" % (input, activated_jobs, self.offset, pending_pilots, out)
        return (out,msg)
