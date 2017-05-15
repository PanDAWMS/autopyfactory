"""
 An APF auth plugin to provide SSH key info. 

"""
import logging
import os


# Does not need to be a thread because it doesn't need to perform asynch actions.
class SSH(object):
    """
        Container for SSH account info.
        Works with provided filepaths. 
        Eventually should work with config-only provided tokens, with files created.  
    """    
    def __init__(self, manager, config, section):
        self.log = logging.getLogger('autopyfactory.auth')
        self.name = section
        self.manager = manager
        self.factory = manager.factory
        self.basedir = os.path.expanduser(config.get(section, 'authbasedir'))
        self.sshtype = config.get(section, 'ssh.type' )
        self.privkey = config.get(section, 'ssh.privatekey' )
        self.pubkey = config.get(section, 'ssh.publickey' )
        self.privkeypass = config.get(section, 'ssh.privkeypass')
        self.privkeypath = os.path.expanduser(config.get(section, 'ssh.privatekeyfile' ))
        self.pubkeypath = os.path.expanduser(config.get(section, 'ssh.publickeyfile' ))
        self.privkeypasspath = config.get(section, 'ssh.privkeypassfile')
        #self.passwordfile = config.get(section, 'ssh.passwordfile')
        
        # Handle raw empty values
        if self.privkey.lower() == 'none':
            self.privkey = None
        if self.privkeypass.lower() == 'none':
            self.privkeypass = None
        if self.pubkey.lower() == 'none':
            self.pubkey = None
        
        # Handle path empty values    
        if self.privkeypath.lower() == 'none':
            self.privkeypath = None
        if self.privkeypasspath.lower() == 'none':
            self.privkeypasspath = None
        if self.pubkeypath.lower() == 'none':
            self.pubkeypath = None
        #if self.passwordfile.lower() == 'none':
        #    self.passwordfile = None
              
        # Create files if needed
        # TODO
        
    def _validate(self):
        """
        Confirm credentials exist and are valid. 
        """
        return True


    def getSSHPubKey(self):
        pass
    
    def getSSHPrivKey(self):
        pass
    
    def getSSHPubKeyFilePath(self):
        self.log.debug('[%s] Retrieving pubkeypath: %s' % (self.name, self.pubkeypath))
        return self.pubkeypath
    
    def getSSHPrivKeyFilePath(self):
        self.log.debug('[%s] Retrieving privkeypath: %s' % (self.name, self.privkeypath))
        return self.privkeypath
    
    def getSSHPassFilePath(self):
        self.log.debug('[%s] Retrieving passpath: %s' % (self.name, self.privkeypasspath))
        return self.privkeypasspath
    
        
