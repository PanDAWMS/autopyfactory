from backend.base import base

class condorgBackend(base):
    def __init__(self):
        pass
    
    def status(self, factory):
        '''We query condor for jobs running as us (owner) and this factoryId so that multiple 
        factories can run on the same machine
        Ask for the output from condor to be in the form of "key=value" pairs so we can easily 
        convert to a dictionary.'''
        condorQuery = '''condor_q -constr '(owner=="''' + factory.config.get('Factory', 'condorUser') + \
            '''") && stringListMember("PANDA_JSID=''' + factory.config.get('Factory', 'factoryId') + \
            '''", Environment, " ")' -format 'jobStatus=%d ' JobStatus -format 'globusStatus=%d ' GlobusStatus -format 'gkUrl=%s' MATCH_gatekeeper_url -format '-%s ' MATCH_queue -format '%s\n' Environment'''
        self.factoryMessages.debug("condor query: %s" % (condorQuery))
        (condorStatus, condorOutput) = commands.getstatusoutput(condorQuery)
        if condorStatus != 0:
            raise CondorStatusFailure, 'Condor queue query returned %d: %s' % (condorStatus, condorOutput)
        # Count the number of queued pilots for each queue
        # For now simply divide into active and inactive pilots (JobStatus == or != 2)
        try:
            for queue in self.config.queues.keys():
                self.config.queues[queue]['pilotQueue'] = {'active' : 0, 'inactive' : 0, 'total' : 0,}
            for line in condorOutput.splitlines():
                statusItems = line.split()
                statusDict = {}
                for item in statusItems:
                    try:
                        (key, value) = item.split('=', 1)
                        statusDict[key] = value
                    except ValueError:
                        self.factoryMessages.warning('Unexpected output from condor_q query: %s' % line)
                        continue
                # We have encoded the factory queue name in the environment
                try:
                    self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['total'] += 1                
                    if statusDict['jobStatus'] == '2':
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['active'] += 1
                    else:
                        self.config.queues[statusDict['FACTORYQUEUE']]['pilotQueue']['inactive'] += 1
                except KeyError,e:
                    self.factoryMessages.debug('Key error from unusual condor status line: %s %s' % (e, line))
            for queue, queueParameters in self.config.queues.iteritems():
                self.factoryMessages.debug('Condor: %s, %s: pilot status: %s',  queueParameters['siteid'], 
                                           queue, queueParameters['pilotQueue'])
        except ValueError, errorMsg:
            raise CondorStatusFailure, 'Error in condor queue result: %s' % errorMsg
