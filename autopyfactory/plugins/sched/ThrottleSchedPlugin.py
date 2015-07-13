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
                # interval is the time windows we observe. Default, last hour
                # maxtime is the maximum WallTime for a pilot to be declared "too short" 
                # ratio is the minimum ratio too short pilots over total pilots to decide there is a blackhole
                # submit is the number of pilots to submit when a blackhole is detected
                self.interval = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.interval', 'getint', default_value=3600)
                self.maxtime = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.maxtime', 'getint', default_value=1800)
                self.ratio = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.ratio', 'getfloat', default_value=0.5)
                self.submit = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.throttle.submit', 'getint', default_value=1)

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

        # to convert the current date into seconds since Epoch
        now_sec_epoch = datetime.datetime.now().strftime('%s')

        # condor_history using the python bindings
        schedd = htcondor.Schedd()
        #out = schedd.history("RemoteWallClockTime < 600 && MATCH_APF_QUEUE == \"ANALY_BNL_LONG-gridgk03-htcondor\"", ['ClusterId, ProcID'], 0)

        timeinterval = int(now_sec_epoch) - self.interval
        condor_constraint_expr = "JobStartDate > %s" timeinterval
        pilots = schedd.history(condor_constraint_expr, ['RemoteWallClockTime', 'MATCH_APF_QUEUE'], 0)

        # process the output of condor_history 
        # we need, for each queue (MATCH_APF_QUEUE), the total number of pilots, 
        # and the number of pilots that finished too fast (RemoteWallClockTime < maxtime)

        #n_pilots = sum(1 for p in pilots)
        pilots_dict = {}
        for pilot in pilots:
            q = pilot['MATCH_APF_QUEUE']
            if q not in pilots_dict.keys():
                pilots_dict[q] = {'total':0, 'short':0}
            pilots_dict[q]['total'] += 1
            w = pilot['RemoteWallClockTime']
            if w < self.maxtime:
                pilots_dict[q]['short'] += 1

        # now, we have a look to what happened with this queue:
        total = d[self.apfqueue.apfqname]['total']
        short = d[self.apfqueue.apfqname]['short']
        
        #self.ratioevents 
        #self.throttle 
        
        ratio = float(short)/total
        if ratio > self.ratio:
            self.log.warning('the ratio short pilots over total pilots %s is higher than limit %s. Submitting just %s' %(ratio, self.ratio, self.submit))
            out = self.submit
        else:
            out = input
        

        self.log.info('input=%s; totalpilots=%s; shortpilots=%s; ratio=%s; submit=%s; Return=%s' %(input,
                                                                                         total,
                                                                                         short, 
                                                                                         self.ratio, 
                                                                                         self.submit, 
                                                                                         out))

        msg = 'Throttle:in=%s;total=%s;short=%s;ratio=%s;submit=%s;ret=%s' %(input, total, short, self.ratio, self.submit, out)
        return (out,msg)
