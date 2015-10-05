
class FactoryConfigManager(object):

    def __init__(self, factory):

        self.log = logging.getLogger('main.configmanager')
        self.factory = factory
        self.configs = {}
        self._getconfigs()
        self.log.debug('ConfigManager: Object initialized.')


    def _getconfigs(self):
        '''
        create the list of Config objects, 
        based on information comming from 
            -- autopyfactory.conf 
            -- configs.conf
        '''

        # Queues Config plugins Config Loader (qccl)
        # a ConfigParser object with everything in file queueConfigConf, AND from autopyfactory.conf for backwards compatibility
        qccl = Config() # empty


        # 1st, we check if there are old-fashion variables in autopyfactory.conf
        #      (we keep them for backwards compatibility)
        queueConf = self.factory.fcl.generic_get('Factory', 'queueConf', default_value="None") 
        queueDirConf = self.factory.fcl.generic_get('Factory', 'queueDirConf', default_value="None") 
        queueDefaults = self.factory.fcl.generic_get('Factory', 'queueDefaults', default_value="None")
        # if queueConf and/or queuesDirConf and/or queuesDefaults are defined, 
        # they need to be converted into a File plugin  
        # we do this for backward compatibility
        if queueConf != "None" or queueDirConf != "None":
            qccl.addsection('NO_SECTION', {'configplugin':'File', 'queueConf': queueConf, 'queueDirConf': queueDirConf, 'queueDefaults':queueDefaults})
            # FIX !!! VERY IMPORTANT !!!  this section is fake, it is not in the config file, so cannot be traced back via  self.factory.<anything>...
            #                             in other words, the Config plugin __init__( ) needs to receive the configloader object itself


        # 2nd, we check if there is a queuesConfigConf file defined, 
        #      and we parse it
        queueConfigConf = self.factory.fcl.generic_get('Factory', 'queuesConfigConf') 
        if queueConfigConf:
            # Queues Config plugins Config Loader (qccl)
            # a ConfigParser object with everything in file queueConfigConf, AND from autopyfactory.conf for backwards compatibility
            newqccl = ConfigManager().getConfig(queueConfigConf)  

            for section in newqccl.sections():
                if newqccl.generic_get(section, 'enabled', 'getboolean', default_value=True):
                    qccl.merge(newqccl.getSection(section))

        return qccl

