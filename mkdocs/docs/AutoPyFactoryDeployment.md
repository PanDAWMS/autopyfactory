</div>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<br>
<h1><a name="Deployment_of_AutoPyFactory"></a>  Deployment of AutoPyFactory </h1>

<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to install and configure <a href="../index.html" class="twikiLink">AutoPyFactory</a>
<p />
<p />
Conventions used in this document:
<p />
<p />
<font color="#808080">A <i>User Command Line</i> is illustrated by a green box that displays a prompt:</font>
<p />
<pre class="screen">
  [user@client ~]$
</pre>
<p />
<font color="#808080">A <i>Root Command Line</i> is illustrated by a red box that displays the <em>root</em> prompt:</font>
<p />
<pre class="rootscreen">
  [root@factory ~]$
</pre>
<p />
<font color="#808080"><i>Lines in a file</i> are illustrated by a yellow box that displays the desired lines in a file:</font>
<pre class="file">
priorities=1
</pre>
<p />
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Deployment_using_RPM"></a> 3  Deployment using RPM </h1>
<p />
Installation as root via RPMs has now been quite simplified. These instructions assume Red Hat /
Enterprise Linux 6.x (and derivates) and the system Python 2.6.x. Other distros and higher 
Python versions should work with some extra work. 
<p />
1) Install and enable a supported batch system. Condor is the current supported default. /
Software available from  <a href="http://www.cs.wisc.edu/condor/" target="_top">http://www.cs.wisc.edu/condor/</a>. Condor/Condor-G setup and 
configuration is beyond the scope of this documentation. Ensure that it is working
properly before proceeding. 
<p />
2) Install a grid client and set up the grid certificate+key under the user APF will run as. 
See  sysconfig section in <a href="../AutoPyFactoryConfiguration/">configuration</a> for details.
Please read the section proxy.conf in <a href="../AutoPyFactoryConfiguration/">configuration</a> regarding the proxy configuration file, so you see what 
will be needed. Make sure voms-proxy-* commands work properly. 
<p />
3) We distribute now AutoPyFactory RPMs using the Open Science Grid (OSG) yum infrastructure. 
Install the OSG yum repo files:
<p />
<pre class="rootscreen">
[root@factory ~]$ rpm -Uhv  http://repo.grid.iu.edu/osg/3.3/osg-3.3-el6-release-latest.rpm
Retrieving http://repo.grid.iu.edu/osg/3.3/osg-3.3-el6-release-latest.rpm
Preparing...                ########################################### [100%]
   1:osg-release            ########################################### [100%]
</pre>
<p />
For RedHat 7, in a similar way:
<p />
<p />
<pre class="rootscreen">
[root@factory ~]$ rpm -Uhv  http://repo.grid.iu.edu/osg/3.3/osg-3.3-el7-release-latest.rpm
Retrieving http://repo.grid.iu.edu/osg/3.3/osg-3.3-el7-release-latest.rpm
Preparing...                ########################################### [100%]
   1:osg-release            ########################################### [100%]
</pre>
<p />
<p />
More extensive documentation on how to install the OSG yum files can be found <a href="https://twiki.grid.iu.edu/bin/view/Documentation/Release3/YumRepositories" target="_top">here</a>.
<p />
<strong>IMPORTANT</strong>: it may happen that a new version of AutoPyFactory is not available in the release repo, but it is in the development repo. This will happen, for example, when a bug needs to be fixed, and the new RPM is not available yet "officially" until next OSG cycle. 
Therefore, for this type of cases, you may want to enable the development repo as well, as it is disabled by default, by setting <code><b>enabled=1</b></code> in the file <code>/etc/yum.repos.d/osg-el6-development.repo</code> :
<p />
<pre class="file">
[osg-development]
name=OSG Software for Enterprise Linux 6 - Development - $basearch
#baseurl=http://repo.grid.iu.edu/osg/3.3/el6/development/$basearch
mirrorlist=http://repo.grid.iu.edu/mirror/osg/3.3/el6/development/$basearch
failovermethod=priority
priority=98
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG
consider_as_osg=yes

[osg-development-source]
name=OSG Software for Enterprise Linux 6 - Development - $basearch - Source
baseurl=http://repo.grid.iu.edu/osg/3.3/el6/development/source/SRPMS
failovermethod=priority
priority=98
enabled=0
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG

[osg-development-debuginfo]
name=OSG Software for Enterprise Linux 6 - Development - $basearch - Debug
baseurl=http://repo.grid.iu.edu/osg/3.3/el6/development/$basearch/debug
failovermethod=priority
priority=98
enabled=0
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG

</pre>
<p />
For RedHat 7, the yum file is <code>/etc/yum.repos.d/osg-development.repo</code>. 
It looks similar, and the step is the same, enable the development repo setting <code><b>enabled=1</b></code>.
<p />
<pre class="file">
[osg-development]
name=OSG Software for Enterprise Linux 7 - Development - $basearch
#baseurl=http://repo.grid.iu.edu/osg/3.3/el7/development/$basearch
mirrorlist=http://repo.grid.iu.edu/mirror/osg/3.3/el7/development/$basearch
failovermethod=priority
priority=98
enabled=0
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG
consider_as_osg=yes

[osg-development-source]
name=OSG Software for Enterprise Linux 7 - Development - $basearch - Source
baseurl=http://repo.grid.iu.edu/osg/3.3/el7/development/source/SRPMS
failovermethod=priority
priority=98
enabled=0
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG

[osg-development-debuginfo]
name=OSG Software for Enterprise Linux 7 - Development - $basearch - Debug 
baseurl=http://repo.grid.iu.edu/osg/3.3/el7/development/$basearch/debug
failovermethod=priority
priority=98
enabled=0
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-OSG

</pre>
<p />
4) If you will be performing <strong>local</strong> batch system submission (as opposed to remote submission
via grid interfaces) you must confirm that whatever account you'll be submitting as exists on
the batch cluster. This is also the user you should set AutoPyFactory to run as. 
<p />
<strong>NOTE</strong>: You do not want local batch logs being written to NFS, so you will need to define a 
local directory for logs and be sure the APF user can write there. 
<p />
<p />
<p />
<p />
<p />
5) Install the AutoPyFactory RPMs:
<p />
AutoPyFactory is now distributed as a set of RPMs, rather than a single one. 
The complete list of available RPMs is described in the following table:
<p />
<table class="tg">
  <tr>
    <th class="tg-header">RPM</th>
    <th class="tg-header">content</th>
    <th class="tg-header">dependencies</th>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-common</td>
    <td class="tg-031e"> All core files and directories needed to make AutoPyFactory work </td>
    <td class="tg-031e"> condor <br> python-simplejson <br> python-pycurl </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-proxymanager</td>
    <td class="tg-031e"> The files to install the proxy management tool, standalone </td>
    <td class="tg-031e"> voms-clients <br> myproxy <br> autopyfactory-common </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-panda</td>
    <td class="tg-031e"> The files needed to allow AutoPyFactory to interact with PanDA </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-local</td>
    <td class="tg-031e"> The files needed to allow AutoPyFactory to submit locally or to query a local condor pool </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-remote</td>
    <td class="tg-031e"> All files needed by AutoPyFactory to submit pilots to the grid </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-cloud</td>
    <td class="tg-031e"> All files needed by AutoPyFactory to submit pilots to the clouds (for example, to EC2) </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-scheds</td>
    <td class="tg-031e"> The complete set of scheduler plugins </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-plugins-monitor</td>
    <td class="tg-031e"> The complete set of monitor plugins </td>
    <td class="tg-031e"> </td>
  </tr>
<p />
</table>
<p />
<p />
You can select the set of RPMs needed according to the particular purpose of your factory (submitting to a cloud, to the grid based on idle jobs in a local condor pool, etc.)
In all cases installing autopyfactory-common is required, as it contains the core functionalities of AutoPyFactory.
<pre class="rootscreen">
[root@factory ~]$ yum install autopyfactory-common</pre>
<p />
This performs several setup steps that otherwise would need to be done manually:
<p /> <ul>
<li> Creates 'autopyfactory' user that AutoPyFactory will run under by default.
</li> <li> Enables the factory init script via chkconfig.
</li> <li> Installs condor.
</li> <li> Installs python-simplejson and python-pycurl.
</li> <li> Installs RPM autopyfactory-proxymanager.
</li></ul> 
<p />
<p />
In order to facilitate the installation process, a set of META-RPMs are also available for the more common use cases:
<p />
<table class="tg">
  <tr>
    <th class="tg-header">META RPM</th>
    <th class="tg-header">Installs</th>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-remote</td>
    <td class="tg-031e"> autopyfactory-common <br> autopyfactory-plugins-remote <br> autopyfactory-proxymanager </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-panda</td>
    <td class="tg-031e"> autopyfactory-common <br> autopyfactory-plugins-panda  </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-wms</td>
    <td class="tg-031e"> autopyfactory-common <br> autopyfactory-plugins-local </td>
  </tr>
<tr>
    <td class="tg-raw1">autopyfactory-cloud</td>
    <td class="tg-031e"> autopyfactory-common <br> autopyfactory-plugins-cloud <br> autopyfactory-proxymanager </td>
  </tr>
</table>
<p />
<p />
6) Start AutoPyFactory:
<p />
<pre class="rootscreen">
[root@factory ~]$ /etc/init.d/autopyfactory start</pre>
<p />
7) Confirm that everything is OK:
<p /> <ul>
<li>  Check to see if APF is running:
</li></ul> 
<pre class="rootscreen">
[root@factory ~]$ /etc/init.d/factory status</pre>
<p /> <ul>
<li> Look at the output of ps to see that AutoPyFactory is running under the expected user.      This should show who it is running as, and the arguments in      <code>/etc/sysconfig/factory</code>: 
</li></ul> 
<p />
<pre class="rootscreen">
[root@factory ~]$ ps aux | grep autofactory | grep -v grep
502       6624  0.1  0.0 721440 12392 pts/0    Sl   Oct20   2:04 /usr/bin/python /usr/bin/autopyfactory --conf /etc/autopyfactory/autopyfactory.conf --info --sleep=60 --runas=autopyfactory --log=/var/log/autopyfactory/autopyfactory.log
</pre>
<p /> <ul>
<li>  Tail the log output and look for problems.
</li></ul> 
<p />
 <pre class="rootscreen">
[root@factory ~]$ tail -f /var/log/autopyfactory/autopyfactory.log</pre>
<p /> <ul>
<li> Check to be sure jobs are being submitted by whatever account AutoPyFactory is using by executing condor_q manually:  
</li></ul> 
<p />
 <pre class="rootscreen">
[root@factory ~]$ condor_q | grep autopyfactory</pre>
<p />
<h1><a name="4_Dependencies"></a> 4  Dependencies </h1>
<p />
In some cases, but not always, HTCondor will be needed to run operations. 
For those cases, HTCondor RPMs are expected to be deployed. 
AutoPyFactory does not set explicitly it as a dependency, so it will not be installed automatically. 
<p />
<h1><a name="5_Configuration"></a> 5  Configuration </h1>
<p />
The details about configuration variables in AutoPyFactory can be found <a href="AutoPyFactoryConfiguration/" target="_top">here</a>
<p />
<p />
<p />
<h1><a name="6_Relase_notes"></a> 6  Relase notes </h1>
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_1_Relase_semantics"></a> 6.1  Relase semantics </span></h2>
<p />
AutoPyFactory release versions are composed by 4 numbers:
<pre>
        major.minor.release-build
</pre>
For example: 1.2.3-4
<p /> <ul>
<li> A change in the major number means the entire architecture of AutoPyFactory has been redesign. It implies a change at the conceptual  level. In other words, changing the major number means a new project. 
</li></ul> 
<p /> <ul>
<li> A change in the configuration files that requires sys admins intervention  after updating implies a change in the minor number.
</li></ul> 
<p /> <ul>
<li> Implementing a new relevant feature implies changing the minor number.
</li></ul> 
<p /> <ul>
<li> A significative amount of code refactoring that may affect the performance  of AutoPyFactory -including speed, memory usage, disk usage, etc-  implies changing the minor number.
</li></ul> 
<p /> <ul>
<li> Small features and bug fixes imply changing the release number. 
</li></ul> 
<p /> <ul>
<li> A change in the RPM package but not in the code are reflected in the  build number.
</li></ul> 
<p /> <ul>
<li> Not all new releases are placed in the production RPM repository.   Many of them are available at the development and testing repositories,   but only those that have been verified to work are moved   to the production repository. 
</li></ul> 
<p /> <ul>
<li> Same RPM will have always the same numbers in all repositories. 
</li></ul> 
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_2_version_2_4_6"></a> 6.2  version 2.4.6 </span></h2>
<p />
Only difference between version 2.4.6 and previous 2.4.x versions is the location of the configuration files. 
Configuration files in 2.4.6 are deployed directly under directory <code>/etc/autopyfactory/</code>
<p />
2.4.6 respects the new nomenclature for files, directories and processes introduced in 2.4.0.
For factories being migrated from 2.3.x to 2.4.6 it is, therefore, highly recommended to read first section on upgrading a factory from 2.3</a>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_3_version_2_4_0"></a> 6.3  version 2.4.0 </span></h2>
<p />
Version 2.4.0 introduces major changes in name of files and directories, 
programs, users accounts, processes, etc. 
This recipe should help with a step by step migration:
<p />
 1. stop the factory
<p />
<pre class="rootscreen">
[root@factory ~]$ service factory stop
</pre>
<p />
 2. install RPM for autopyfactory-2.4.0
<p />
first, it is needed to removed the previous installation and the one for autopyfactory-tools, 
if it is installed, since there are incompatible requirements
This will delete some files and directories. If you customized the logrotation, you may want to make a copy first.
Also make a security copy of the configuration directory
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /etc/logrotate.d/autopyfactory.logrotate /tmp/
[root@factory ~]$ cp /etc/sysconfig/factory.sysconfig /tmp/
[root@factory ~]$ mkdir /tmp/etc/
[root@factory ~]$ cp /etc/apf/* /tmp/etc/
</pre>
<p />
Remove the old packages:
<p />
<pre class="rootscreen">
[root@factory ~]$ rpm -e panda-autopyfactory panda-autopyfactory-tools
</pre>        
<p />
Install the new autopyfactory package:
<p />
<pre class="rootscreen">
[root@factory ~]$ yum install autopyfactory
</pre>
<p />
<p />
3. Several directories have been created. 
<p />
Directory <code>/etc/autopyfactory/</code> has been created, but it is empty. 
<p />
The examples for the config files are placed under <code>/usr/share/doc/autopyfactory-2.4.0/</code>.
<p />
Also the examples for logrotate and sysconfig files are in  <code>/usr/share/doc/autopyfactory-2.4.0/logrotate/</code> and <code>/usr/share/doc/autopyfactory-2.4.0/sysconfig/</code>
<p />
The old config files are still under <code>/etc/apf/</code> and the old sysconfig is still as <code>/etc/sysconfig/factory.sysconfig</code>
<p />
4. sysconfig
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/sysconfig/autopyfactory-example /etc/sysconfig/autopyfactory
</pre>
<p />
Option 2: copy the old one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/factory.sysconfig  /etc/sysconfig/autopyfactory
</pre>
<p />
In the second case, some adjustments may be needed:
<p /> <ul>
<li>  replace      --runas=apf                           -->  --runas=autopyfactory
</li> <li>  replace      --log=/var/log/apf/apf.log            -->  --log=/var/log/autopyfactory/autopyfactory.log
</li> <li>  replace      CONSOLE_LOG=/var/log/apf/console.log  -->  CONSOLE_LOG=/var/log/autopyfactory/console.log       
</li></ul> 
<p />
<p />
5. log rotation
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/logrotate/autopyfactory-example /etc/logrotate.d/autopyfactory
</pre>
<p />
Option 2: copy the old one (saved in /tmp/)
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/autopyfactory.logrotate /etc/logrotate.d/autopyfactory
</pre>
<p />
In the second case, some adjustments may be needed:
<p /> <ul>
<li> replace       /var/log/apf/apf.log         -->     /var/log/autopyfactory/autopyfactory.log
</li> <li> replace       /var/log/apf/console.log     -->     /var/log/autopyfactory/console.log
</li> <li> replace       /etc/init.d/factory          -->     /etc/init.d/autopyfactory
</li></ul> 
<p />
6. autopyfactory.conf
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/autopyfactory.conf-example  /etc/autopyfactory/autopyfactory.conf
</pre>
<p />
Option 2: copy the old one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/etc/factory.conf /etc/autopyfactory/autopyfactory.conf
</pre>
<p />
In the second case, some adjustments may be needed:
<p /> <ul>
<li> replace         factoryUser = apf                           -->  factoryUser = autopyfactory
</li> <li> replace   queueConf = file:///etc/apf/queues.conf     -->  queueConf = file:///etc/autopyfactory/queues.conf
</li> <li> replace   queueDirConf = /etc/apf/queues.d/           -->  queueDirConf = /etc/autopyfactory/queues.d/
</li> <li> replace   proxyConf = /etc/apf/proxy.conf             -->  proxyConf = /etc/autopyfactory/proxy.conf
</li> <li> replace   monitorConf = file:///etc/apf/monitor.conf  -->  monitorConf = /etc/autopyfactory/monitor.conf
</li> <li> replace   baseLogDir = /home/apf/factory/logs         -->  baseLogDir = /home/autopyfactory/factory/logs
</li> <li> add line  mappingsConf = /etc/autopyfactory/mappings.conf
</li></ul> 
<p />
7. queues.conf
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/queues.conf-example  /etc/autopyfactory/queues.conf
</pre>        
<p />
Option 2: copy the old one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/etc/queues.conf /etc/autopyfactory/queues.conf
</pre>
<p />
In the first case, the file needs to be configured from scratch.
In the second case, no adjustments is needed.
<p />
8. proxy.conf
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/proxy.conf-example  /etc/autopyfactory/proxy.conf
</pre>
<p />
Option 2: copy the old one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/etc/proxy.conf /etc/autopyfactory/proxy.conf
</pre>
<p />
In the first case, the file needs to be configured from scratch.
In the second case, no adjustments is needed.
<p />
9. monitor.conf
<p />
Option 1: copy the new one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/monitor.conf-example  /etc/autopyfactory/monitor.conf
</pre>
<p />
Option 2: copy the old one
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /tmp/etc/monitor.conf /etc/autopyfactory/monitor.conf
</pre>
<p />
In the first case, the file needs to be configured from scratch, but most probably the default configuration is enough.
In the second case, no adjustments is needed.
<p />
10. mappings.conf
<p />
This config file is new, so it must be copied 
<p />
<pre class="rootscreen">
[root@factory ~]$ cp /usr/share/doc/autopyfactory-2.4.0/mappings.conf-example  /etc/autopyfactory/mappings.conf
</pre>        
<p />
Do not touch it.
<p />
11. since the factory will now run as user <code>autopyfactory</code> instead of <code>apf</code>, the new UNIX account needs to be created, if not already done during the RPM install process.
<p />
<pre class="rootscreen">
[root@factory ~]$ useradd autopyfactory
</pre>
<p />
assuming that account also hosts the keys for the X509 proxies in the regular directory .globus:
<p />
<pre class="rootscreen">
[root@factory ~]$ mkdir ~autopyfactory/.globus
[root@factory ~]$ cp -r ~apf/.globus/* ~autopyfactory/.globus/
[root@factory ~]$ chown -R autopyfactory:autopyfactory ~autopyfactory/.globus
</pre>
<p />
if that is not the case, then copy the .pem keys and/or change their ownership.
And to avoid problems, delete the current X509 that may still be in <code>/tmp/</code>
<p />
12. Install autopyfactory-tools
<p />
<pre class="rootscreen">
[root@factory ~]$ yum install autopyfactory-tools
</pre>        
<p />
13. start the factory
<p />
<pre class="rootscreen">
[root@factory ~]$ service autofactory start
</pre>
<p />
<strong>WARNING</strong>: after migrating to 2.4.0, factory runs under user <code>autopyfactory</code> instead of <code>apf</code>.
That means no one will now clean the old directories <code>/var/log/apf/</code> and <code>~apf/factory/logs/</code>
You may delete them at some point (not right away, since they will include condor logs for still running pilots)
<p />
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_4_upgrading_a_factory_from_2_3"></a> 6.4  upgrading a factory from 2.3.1 to 2.4.0 </span></h2>
<p />
In order to upgrade a factory from old stable version 2.3.1 to latest 2.4.x series, a few things need to be taken into account.
<p /> <ul>
<li> The name of files, directories, and processes has changed. A detailed explanation is in the above section version 2.4.0
</li> <li> a new optional variable has been included in the file <code>proxy.conf</code>: <code><b>voms.args</b></code>
</li> <li> a new mandatory option has been added to the new file <code>autopyfactory.conf</code>: <code><b>mappingsConf</b></code>
</li></ul> 
<p />
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_5_version_2_3_2"></a> 6.5  version 2.3.2 </span></h2>
<p /> <ul>
<li> RPM built incorrectly. Rebuilt against correct copy of code.  
</li></ul> 
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_6_version_2_3_0"></a> 6.6  version 2.3.0 </span></h2>
<p /> <ul>
<li> utils not distributed anymore within the RPM. They will be distributed with a dedicated one.
</li> <li> variable 'flavor' mandatory in DEFAULT section in <code>proxy.conf</code> Values are voms or myproxy
</li> <li> In case <code><b>flavor=myproxy</b></code> in <code>proxy.conf</code>, then these variables are needed: <ul>
<li> remote_host
</li> <li> remote_user
</li> <li> remote_owner
</li> <li> remote_group
</li></ul> 
</li> <li> New variable <code><b>factorySMTPServer</b></code> in <code>factory.conf</code>
</li> <li> New variable <code><b>proxymanager.sleep</b></code> in <code>factory.conf</code>
</li></ul> 
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="6_7_version_2_2_0_1"></a> 6.7  version 2.2.0-1 </span></h2>
<p /> <ul>
<li> examples of executables (a.k.a. wrappers) 
</li></ul> 
are placed underneath the <code>/etc/apf/</code> directory.
They are not copied directly into <code>/usr/libexec/</code> anymore. <ul>
<li> wrapper-0.9.9.sh has a different set of  input options than previous one.   Read carefully the inline documentation before using it.
</li> <li> Config plugins have been removed.   Any configuration variable in <code>queues.conf</code>  related PandaConfig or AGISConfig plugins   are not valid anymore.   Therefore the variable <code><b>configplugin</b></code> is gone too.
</li> <li> Variables <code><b>override</b></code> and <code><b>autofill</b></code> are also gone.
</li> <li> There is a new configuration file called <code>monitor.conf</code>  An example is provided underneath <code>/etc/apf/</code>
</li> <li> Old variable in <code>factory.conf</code> pointing to the monitor URL  is now in <code>monitor.conf</code>. The <code>monitor.conf</code> contains sections  for different monitor configurations.  The name of the section is setup in <code>queues.conf</code> via  the new variable <code><b>monitorsection</b></code>. Read carefully the inline documentation in <code>monitor.conf</code> before using it.
</li> <li> Utils, including script to generate <code>queues.conf</code>  with information from AGIS, have changed name and location.  New scripts are place underneath <code>/usr/share/apf</code>
</li></ul> 
<p />
<h1><a name="7_CHANGELOG"></a> 7  CHANGELOG </h1>
<pre>
2.4.9

* fixed bug in sched plugin StatusOffline not checking if siteinfo is None

2.4.8

* only reading configuration files that ends with string ".conf"
* simplified the name of plugins modules and classes, and pluginsmanagement code accordingly
* added a new layer to the plugins directory structure
* distinguish between self.keep_running is None than when is 0
* MaxToRun returns 0 when there is no BatchInfo
* added function debugrestart to init script
* added TRACE level to logging systems
* manually merged various changes to loglevels and sched plugins from ec2fixed adhoc branch.
* fixed minor bug with debugrestart when log file doesn't exist
* cleaned up sched plugin messages to be more consistent 
* introduced email notification throttling at factory level per message content

2.4.7 

* port number in case of submit plugin CondorOSGCE not hardcoded. But default is still 9619
* batchqueue was still mandatory in one plugin. Not anymore
* added panda status throttled to the mappings as notready
* some code in proxymanager.py cleaner, and made baseproxy not mandatory
* made the remote_* variables in proxymanager not mandatory, and removed from proxy.conf
* created bin/autopyfactory_version
* checking if methods _updateclouds( ) and _updatesites( ) in PANDA WMS Status plugin return something valid
* added MissingPluginException to apfexceptions.py
* split the code in method APFQueue.run( ) into 3 separated methods, so they could be called one by one from outside
* starting the APFQueue threads in a separate method _start( )
* using Queue() to serialize the condor_submit tasks
* using self.factory.qcl instead of self.qcl in cleanlogs, so it always keeps the latest configuration, even after reconfig
* Factory.__init__ refactorized, for clarity
* created directory sbin/ add added first draft version of autopyfactory-reconfigure
* using method Config.compare() to decide which queues to start and stop on every re-configuration
* created first draft code for a listener class
* created method Config.addsection() to add and entire section to a configloader object
* factory.py split into two files: factory.py and queues.py
* APFQueue objects are started all of them from method APFQueuesManager::start(), instead of being started when created
* added configplugin variable to autopyfactory.conf
* config/ plugins
* moved Singleton templates from factory.py to interfaces.py
* moved x509 proxy management from CondorBase submit plugin to CondorGrid submit plugin
* added methods isequal( ), sectionisequal( ) and compare( ) to configloader.py
* default value for queuesDirConf set to None to allow start just from install
* printing a WARNING log message when merging two sections with the same name
* set the default value of "override" to False (the new object has precedence over the current one)
* allowing both queues.conf and queues.d/ at the same time, each one with its own merging mechanism
* for clarity, code for plugins management in a separate module pluginsmgmt.py
* first draft for sched plugin Throttle 
* the config files, logrotate file and sysconfig files are not treated as documentation anymore
* commented out the line reading x509userproxy from ProxyManager in BatchSubmit Exec plugin
* new import path to find Clients.py  in WMSStatus Panda plugin
* bug fixed: usercert and userkey only needed in proxymanager.py when baseproxy is none, not always
* changes in the logrotate config file:
  no prerotate script, post rotate only restart daemon when there is a PID file
* normalized the format of the messages returned by the Sched plugins

2.4.6

* importing proxymanager only when proxymanager.enabled is True

2.4.5 

* added external/ directory, with panda client in it
* trying several paths to import panda Client.py

2.4.4

* each APFQueue carrying only one section in the config loader object qcl, instead of all sections, to avoid scalability issues.

2.4.3

* creating the directory baseLogDir if it does not exist yet (for example, first time APF is installed)
* removed manpages management from rpm-post.sh script

2.4.2

* variable batchqueue not mandatory
* typo fixed in proxymanager.py

2.4.1

* reorganizing the docs/* files, etc/* files,  and the man/* files
* added requirements from voms-clients and myproxy in setup.cfg
* removed the handling of sysconfig files in rpm-post.sh
* mappings.conf created.
  plugins now read the mappings from that config file, instead of having hardcoded dictionaries.
* explanation for "lifetime" and "minlife" fixed in proxy.conf-example, 
  and variable "vomshours" renamed as "hours" in proxymanager.
  doc updated.
  method _checkTimeleft() renamed  _checkVOMSTimeLeft(). Log messages fixed accordingly.
*  created method _checkProxyTimeLeft()
*  added a check on proxy timeleft shorter than voms timeleft
* Bugs and typos fixed
* changed nomenclature: everything now is called "autopyfactory" instead of "apf" or "factory"
  .sysconfig removed from  /etc/sysconfig/autopyfactory.sysconfig
  panda- removed from package name
  no wrappers copied to the /etc/ directory
* accept multiple XML docs as output of "condor_q -g". 
* checking if the output of condor_q in querycondor() is still XML even when RC != 0
* if -name and -pool are empty, the condor_q_id is again "local"
* checking output of condor_q() is not None in WMS Status plugin Condor
* removed WMS Status plugin CondorLocal (moved to attic)
* new grid_resource line in Condor-CE submit plugin: CondorOSGCE
* added voms.args to proxymanager.py
* condorlocal -> condor for WMS Status plugin in queues.conf-example and documentation
* adding Condor WMS Status plugin, which is a multi-singleton, to replace old  CondorLocal WMS Status plugin
* allow getwmsstatusplugin() to return a multi-singleton plugin object
* Using queryargs in WMS Status plugin CondorLocal
* Email sent when the generated X509 proxy is not valid: does not exist, too old, or not proper VOMS attributes
* Adding a log message in StatusTest and StatusOffline Sched Plugins 
  when the queue is not test or offline.
* reading self.cloud from queues.conf removed
  Variables not used anymore deleted from queues.conf-example
* bug in manpage autopyfactory.1 fixed
* created submit plugin CondorPBS
* removed reload() function from the init script

ChangeLog 2014-01-24

* Fixed Condor submit plugin initialization hierarchy. 
* Switched from os.getlogin() to pwd.

ChangeLog 2014-01-02
--------

* adjustments to init.d script to improve error codes/detection
* proxymanager.sleep and remote_* config vars added to -example files. 

ChangeLog 2013-12-03
---------

* sched plugin Activated deleted
* bug fixed in method fill() in info.py to prevent an Exception when the dictionary has an unknown key.
* CondorEC2 BatchSubmit and BatchStatus plugins usable, supporting standard Condor ec2 grid type. Assuming VMs join local pool as startds, features correlation of VM jobs and corresponding startd. VM retirement via 'condor_off -peaceful' and VM shutdown when stard is retired. 
* KeepNRunning sched plugin to convert absolute to relative numbers. 
* utils not distributed anymore within the RPM. They will be distributed with a dedicated one.
* Removed reference to Panda cloud status in StatusOfflineSchedPlugin
* created apf-simulated-scheds in misc/
* Proxymanager now able to be run standalone as standard system init daemon. This is so base certs need not be owned or readable by the APF user. 
* Added email notification of factory owner when no valid proxy can be retrieved from the proxymanager. 
* Eliminated all _readconfig() methods in submit plugins. Switched to full initialization during __init__.
* MyProxy support in proxymanager. Allows retrieval of base proxy from MyProxy using passphrase, or using another proxymanager profile proxy for auth.
* fixed bug in Scale sched plugin returning 0 when n*factor < 1. 
* minimum Condor version check. Allows particular plugins to specify minimum. Failure aborts. 
* multiproxy functionality. Allow multiple proxy profiles. proxymanager tries all until one is found. failure generates email.  
* scripts and config files added for logs monitor apf-search-failed
* strip() after split by comma in configloader, to allow things like 'x = foo, bar' with whitespace after comma
* bug in MaxPending sched plugin logic fixed. Now no limit imposed when there are no pending pilots.
* queues configuration files can be read from a directory (i.e. /etc/apf/queues.d/). configloader adapted to accept directories instead of list of URIs
* method name removed from log messages.
* fixed bug in some sched plugins, mixing None and 0 in the logic
* add documentation to install.html (in verbatim, no html format)
* removed all methods for FactoryCLI from bin/factory, and mainLoop() renamed to run()
* added new custom logging level -TRACE- and using it for some long messages, like the output of condor_q in XML format
* configloader converts None in conf to Python None object. Change in default_value logic--if no default_value is provided
  then generic_get returns None. 
* info major refactoring of Info class hierarchy. most objects now have overridden methods to avoid exception generation. 
* passing queryargs from queues.conf to query.querycondor() call at Condor batchstatus plugin.
* siteid removed from several places and replaced by wmsqueue.
* generic_get() simplified
* InfoContainer removed. 
* fake info classes imports removed, and importing from the right place (info.py instead of factory.py)
* log messages in sched plugins more homogeneous. Now all of them are "Return=123".
* created a script in misc/ to search for pilots running (in theory) for more than 8 days.
* Removed allowed variables for Test and Offline Sched plugins, and from the queues.conf. If the plugin is used, we assume it is allowed. 
* more robust code to deal with scenario where condor_q gives no output
* all JSD.add() method replaced to use the new format (2 inputs instead of 1)
* jsd.py and jsd3.py moved to attic/ and jsd4.py moved to jsd.py
* logserver2.py moved to logserver.py, and old one moved to attic/
* using method merge() instead of deepcopy() in configloader.py
* method section2dict() created in configloader. It will be usefull to generate the mappings from config files
* getConfig() method in configloader.py embedded in a try - except block
  and all config loader object creation in factory.py embedded in try - except blocks
* Exception class ConfigFailure renamed ConfigFailureMandatoryAttr
* Cleaning info.py code:
    -- method dict() removed
    -- method getconfig() removed
    -- method __add__() removed
    -- method set() removed
    -- method get() removed
    -- property total removed
    -- class attribute valid = [] removed from all classes:
        -- therefore method reset() removed
        -- therefore method __setattr__() removed
    -- method __getattribute__() is created
    -- method valid() in class InfoContainer deleted
    -- no longer importing Config from configloader 
    -- no longer self.default_value
    -- all __init__() methods now hardcode the entire list of attributes
* Removed logger from the input options in method generic_get()
* As temporary solution, NotImplementedAttr defined in configloader.py
* Documentation on old Sched Plugin SimpleNQueue deleted.
* First draft for manpages created, and rpm-post script adapted to install them.
* Printing env again after switching ID.
* Bug fixed in CREAM example in queues.conf-example
* created misc/apf-panda-jobs-info.py 
* INFO messages in configloader for missing non-mandatory variables moved to DEBUG.
* all __XYZ__ module names deleted, except in factory.py
* Fixed setup.cfg
* Adjusted sysconfig, factory init, and logrotate to use a console.log for python interpreter-level 
  debugging. 
* Added a log message with the APF version number.
* All Sched Plugins returns a tuple (number of pilots, message). 
* Sending to the monitor the messages returned by Sched Plugins.
* Changed setup.py, factory.py and plugins/monitor/APFMonitorPlugin.py to use release version info directly
  from factory.py rather than requiring that it be correctly placed in a config file. 
  versionTag removed from factory.conf
* Added logserver2.py, to create directory listing similar to Apache one.
* Port number is got from URL instead of from config variable baseLogHttpPort
* Method setuppandaenv() deleted from factory.py

2013-05-06

* copy_to_spool set to True
* dumping the content of queues.conf on start
* allowing raw = True or False in getContent() method in configloader
* utils distributed via /usr/share/apf instead of /usr/bin, and have different names.
* created RELEASE_NOTES
* everything related euca and persistence removed from config files. 
* created test.py, to start creating unittest-like code
* should_transfer_files = NO
* new configuration file for monitor plugins.
* wrapper examples are placed in /etc/apf/ instead of /usr/libexec/
* No more Config Plugins.
* module condor.py created. It includes htcondor python bindings. 
* Non plugin modules (jsd.py, persistence.py) moved to main directory.
* Split plugins into their own directories. 
* version number for panda userinterface package in setup.cfg
* bugs in CondorLocalWMSStatusPlugin fixed
* ReadySchedPlugin created -> Activated decommissioned
* Removing self._valid from all plugins (work in progress)
* ConfigFailure moved to apfexceptions.
* Removed hardcoded setup of periodic_remove directive from CondorLocal Batch Submit plugin.
* Not trying to create robot.txt is logserver is disabled.
* Changing directory to new HOME directory after switching identity.
* CondorNordugrid Batch Submit plugin created.
* Returning cached info object when there are problems to contact the info service in Panda Config Plugin and AGIS Config Plugin. 
* Checking condor daemon is running in CondorBase Batch Submit Plugin and Condor Batch Status Plugin.
* Created a Singleton metaclass factory. It creates metaclasses for regular Singletons and multi Singletons.
* URLs for AGIS Config Plugin and PanDA Config Plugin read from queues.conf instead of hardcoded.
* Printing the path to executable condor_q and condor_submit in debug mode.
* Using new URL for AGIS Config Plugin, and fixing code to parse new output format (a dict of dicts instead of a list of dicts).
* New URL for Panda Config Plugin.
* Added __add__() method to BatchQueueInfo and WMSQueueInfo classes.
* CondorLSF Submit plugin created.
* Euca plugins improved. Still under development. NOTE: most probably they will be removed and never used.
* New type of plugins for monitor added.
* Added factory config variable 'enablequeues'.
* ScaleSchedPlugin created.
* Sched plugins do not check for negative outputs. It has been left up to the Submit Plugin to decide what to do in that case. 
* Added some DEBUG logging messages.
* Some messages in Activated Sched Plugin moved from DEBUG logging level to INFO level.
* Bug fixed: getboolean() instead of get() when reading value of proxymanager.enabled and logmanager.enabled.
* Top logger configured as root, instead of "main".
* Log messages format includes method name for python > 2.4
* KeepNRunningSchedPlugin created.
* This release is 'Babcock' v. 2.2.0
  http://en.wikipedia.org/wiki/List_of_Characters_in_The_Passage_Trilogy#Babcock


2012-07-23

* Using new PanDA client API method getJobStatisticsWithLabel.
* GRAM CE is called OSG-CE in AGIS, instead of CE. Changed in AGISConfig Plugin.
* Pilots in STAGE-IN status are now considered to be PENDING instead or RUNNING.
* All get() calls in configloader.py have now raw=True, so interpolations wait until the very end.
* wmsqueue can be read from SchedConfig via PandaConfig Plugin, and from AGIS via AGISConfig Plugin.
* PandaConfig Plugin reads from SchedConfig variables related to CREAM CE.
* MaxToRun sched plugin created.
* CondorOSGCE submit plugin created.
* CondorDeltaCloud submit plugin created.
* Documentation in HTML format under doc/ directory.

2012-06-07

* Bumped minor release version to reflect scale of several new features, and cloud submit plugin. 
* Two new Sched Plugins to handle test queues and offline queues
* Bug in monitor.py fixed. 
* Creates robots.txt file in base of logserver docroot. 
* Added create_run_var() to init script factory. 
  It creates the subdirectory var/run/ if it does not exist, to place factory.pid
* Added $APFHEAD to init script factory. 
  This allows for user deployment on any directory, not necessarily $HOME
* Fixed the bug in Activated plugin, not returning anything under some circumstances. 
* Creating multiple Sched plugins (MinPerCycle, MaxPerCycle, MinPending, MaxPending, MaxPerFactory), 
  and code in factory.py to allow chaining more than one sched plugin.
* PandaConfig Plugin refactored to query SchedConfig in a singleton thread. 
  This is possible because it now uses an URL that delivers the entire SchedConfig content.
  Also more variables added: jdladd, environ, and special_pars.
* First draft for Euca Submit Plugin created.
* This release is the 'The Hellion' v. 2.1.1
  http://www.darylgregory.com/pandemonium/Review_NYRSF.aspx

2012-03-30

* Refactored scheduler log cleanup. Now handled in a separate thread, one for entire factory. 
  Defaults may be specified globally in factory.conf or per-queue in queues.conf 
* Added grid and vo queue attributes, and added executable.defaultarguments and executable.arguments 
  interpolation. These changes were to support wrapper.sh, runpilot3.sh, and arbitrary executables in 
  a general way. This feature uses standard Python ConfigParser interpolation. See
     http://docs.python.org/library/configparser.html
* Made Monitor object a singleton, to avoid repeated timeout delays (during queue initialization) 
  when APF monitor is unresponsive. Now there is a single attempt when single Monitor is initialized. 
* Move to wrapper 0.9.5, which checks that retrieved tarball is indeed a tarball (and not an HTML error message
  from a misbehaving HTTP proxy.  
* Fixed logic problem in Activated plugin. Now correctly assuming that Running jobs are no longer
  Activated. 
* Added specific Submit plugin for Cloud.
* Configuration objects handled as python native ConfigParser objects instead of custom
  APF objects.
* Refined running as user rather than from RPM. 'setup.py install --home=/path/to/home' does a
  user-based setup. All libs are in ~/lib/python, configs and init script in ~/etc/, and so on.
* Added an external queue configuration information plugin mechanism to enable plugin-based 
  auto-fill/overriding of config information. 
* Consolidated all plugins into hierarchical inheritance tree, to eliminate duplicated code 
  (especially the Condor plugins). 
* This release is the 'Sleeper Service' v. 2.1.0
  http://en.wikipedia.org/wiki/GSV_Sleeper_Service

2011-12-02

* Major refactorization to integrate BNL changes. Full object-oriented functional 
  plugin architecture, each running in a distinct thread. Allows for 
  end-user/third-party customization without touching core code. 
* Refined distutils usage to allow deployment as non-root in a home directory, 
  or as root in system paths. Added functionality to drop privs even when run as
  root. 
* Added typical init script and sysconfig functionality to handle shell-level/UNIX 
  concerns. 
* Added standard Linux logrotate configuration. 
* Split config system into main source for APF instance (factory.conf), proxy 
  management (proxy.conf), individual queue configuration (queue.conf). The latter 
  can be accessed as URI, paving the way to queue configuration via remote DB+URL.
* The previous change involved passing ConfigLoader objects to classes, rather than 
  passing through lists of config files. 
* Integrated batch system log export via HTTP; now uses embedded Python HTTP server. 
* Integrated proxy handling and renewal, integrated submit system log file rotation. 
* Adjusted module, file, and class names to follow Python recommended guidelines. See PEP 8:
  http://www.python.org/dev/peps/pep-0008/ 
* Moved all filehandling to factory.py script from Factory class. Factory is now purely 
  object-oriented suitable for embedding (e.g. in a web application). 
* Added copy of Jose's generic top-level wrapper to libexec/.  
* IMPORTANT: For various reasons, it was very difficult to maintain the ability to automatically
  reload configs by detecting file mtime changes. So for now simply restart if you change a file.
  This feature  can be re-enabled soon. 
* This release is "The Clockmaker". v2.0.0
  http://en.wikipedia.org/wiki/List_of_Revelation_Space_characters#The_Clockmaker

2011-03-28

* Update configuration example to Peter's new monitoring URl.
* If this is a ptest pilot use Paul's development pilot code (override with
  PILOT_HTTP_SOURCES still possible).
* This release is "The Captain". v1.2.0.
  Now the Captain called me to his bed 
  He fumbled for my hand 
  "Take these silver bars," he said 
  "I'm giving you command." 
  "Command of what, there's no one here 
  There's only you and me -
  All the rest are dead or in retreat 
  Or with the enemy." 

2011-03-17

* Only try to use python2.6 if the env var APF_PYTHON26 is defined.

2011-03-16

* Prepended "FACTORY DEBUG:" to the setup of the panda client environment
  variables which get set before logging is enabled (this confused
  some people).
* Use traceback.format_exc() to dump exceptions properly into the 
  configured factory logger ("raise" sends output to stderr).

2011-01-13

* If PANDA_URL_CONF or PANDA_URL are set then do not alter them.

2010-12-15

* Support 'allowothercountry" for beyond the pledge resources. (Default False).
* Set the PANDA_URL_CONF and PANDA_URL environment variables to use the 
  panda server squid port (25085). Suppress this by setting the APF_NOSQUID
  environment variable.
* Download of queue data also now happens via the squid cache.
* Monitoring improvement to help scale better. Removal of monitoring cron jobs.
* This is the "Silent Running" release. Look after the plants, Dewey.
  http://en.wikipedia.org/wiki/Silent_Running

2010-11-22

* Changed default sleep time to 120s.
* N.B. Monitoring requires pycurl (install standard python-curl RPM).

2010-11-05

* Wrapper now supports environment variables starting with APF_FORCE_ 
  which set the environment variable with APF_FORCE_ stripped off.
  This is needed when setting the normal environment variable via
  condor-g fails because it gets overwritten by the site shell setup.

2010-09-15

* Removed deprecated code from runpilot3-wrapper.sh (uudecode and broken
  site hacks).
* This is the "Time Out of Joint" release. This pilot factory does not 
  exist. It is a small text file on your computer containing the words
  "Pilot Factory":
  http://en.wikipedia.org/wiki/Time_Out_of_Joint

2010-07-24

* Corrected bug where "site" parameter was used instead of "siteid" to get
  panda job states (broke ANALY sites for which these are different). 
  Reported by Xavi, diagnosed by Graeme, fixed by Peter.
* Reintroduced support for CREAM CEs - factory now detects CREAM CEs and
  provides the correct JDL to condor (recommended only for use with 
  condor >= 7.5.3). See configuration example for how to support this.
* Changed JDL for gt2 GRAM sumbission from anachronistic "universe = globus"
  to "universe = grid; grid_resource = gt2 GATEKEEPER_URL".
* Changed from 'jdl' field in schedconfig to 'queue', which is a better name 
  and has the correct value for condor sites ('jdl' supported but 
  deprecated).
* Changed default proxy location to /tmp/prodRoleProxy and pilotRoleProxy.
  This can still be inherited from X509_USER_PROXY.
* Changed the default source proxy to /tmp/plainProxy.
* Changed handling of deprecated keys to rewrite the configuration objects
  themselves, which gives more robust and consistent handling. Unknown
  keys are deleted and a warning printed.
* Schedconfig download failures are now errors (not warnings).

2010-06-01

* Changed argument passed to pilot from 'site' to 'siteid', which is correct
  and necessary for ANALY_ sites.
* Added new internal status of 'error' where queue has siteid=None. This 
  suppresses pilot submission to that queue.
* Added new configuration option for the [QueueDefaults] section 
  'analysisGridProxy' which is the default proxy used for ANALY_ sites.
* Default setting for 'user' of ANALY queues is now 'user' (can still be
  overwritten). 
* ATLAS software release areas are now ordered correctly by a python code
  snippet in the wrapper which correctly compares 'rel_M-N'. 
* Updated release number to 1.0.1.
* This is the "Feersum Endjinn" release:
  http://en.wikipedia.org/wiki/Feersum_Endjinn

2010-04-09

* Added protection in submission logic against None value for 
  depthboost.
* Updated example configuration file.
* Changed configuration logic so that override = True prevents loading
  of schedconfig values only for those values set in the factory
  configuration (previously this completely supressed schedconfig
  loading).
* Renamed various parameters to their new schedconfig names (Oracle 
  column names are case insensitive, so become lower case). 
    pilotDepth -> nqueue
    pilotDepthBoost -> depthboost
    idlePilotSuppression -> idlepilotsuppression
    pilotLimit -> pilotlimit
    transferringLimit -> transferringlimit
    env -> environ
  Old names are still supported for now, but generate a warning message.
* Changed submission logic for sites in test mode to keep one pilot
  queued, but up to nqueue running (better throughput for site
  revalidation).
* Moved import exception handling to main factory.py script.

2010-03-22

* Restructured logging to inherit from main logging class (deprecated
  global logging to console). Logging now definable as an option
  (syslog, rotating file, stdout). N.B. Default logging is now to
  syslog - if you want the old behaviour set --output=stdout.
* Rewrote JDL to have separate condor log file for each individual job.
  Will help integrate with panda monitoring.
* Added limit setting code. Wrapper will always set a file size of 20GB to 
  prevent mad jobs from killing a worker node. If the queue has a memory
  limit set, then the pilot also sets this as the VMEM limit in the
  shell.
* Removed panda server Client.getSiteSpecs() call. Better to get individual
  queue statuses from schedconfig (fixes https://savannah.cern.ch/bugs/?62751) 
* Deprecated status=ok,disabled (should be online, offline). Prints warning
  for now, but remains supported.

2010-03-19

* Internal merge with latest pyfactory trunk (the old release).

2010-03-18

* Setting filesize limit (20GB) and optionally memory limit in pilot wrapper.

2010-02-11 "Tyger! Tyger!" Release (r130 factory.py; r128 runpilot3-wrapper.sh)

* Incorporated Rod's patch to support CREAM CEs.
* Corrected handling of X509_USER_PROXY environment variable and added
  explicit addition of x509userproxy to JDL.
* Proxy is now defined per queue, enabling support for multi-role pilot
  factories with recent condor versions.
* Pilot tarball now downloaded from pandaserver.cern.ch, removing one
  dependency of job workflow on the panda monitor.
* Revised runpilot3-wrapper.sh which removed explict addition of
  python library installation from the ATLAS s/w area (reversed part 2
  of change 2 from "gunpowder").
* Corrected an error in detecting OSG sites in runpilot3-wrapper.sh
  (thanks to Rod).
* Reintroduced the idlePilotNumber parameter, which controls how many
  pilots are send during an idling cycle (it's useful for stress
  testing sites).
* Fixed a race condition in the vomsrenew.sh script.
* This release is for William Blake smoking opium at Chinese New Year.

2009-11-24 "900" Release (r35 runpilot3-wrapper.sh)

* Define ATLAS_AREA internal variable
  =$VO_ATLAS_SW_DIR for EGEE sites
  =$OSG_APP/atlas_app/atlas_rel for OSG sites
* Source $ATLAS_AREA/local/setup.sh if it exists.
* This release inspired by 900GeV collisions:
  http://atlas.web.cern.ch/Atlas/public/EVTDISPLAY/atlas2009-collision-atlantis-141749-406601-hits-web.png


2009-11-05 "Gunpowder" Release (r25)

* Changed SVN repository to svn.cern.ch/reps/scotgrid/atlas/pyfactory.
  (Verison number plunged down!)
* Made wrapper more intelligent in changing system PYTHONPATH to get
  32 bit lfc module, which helps on sites with 64 bit middleware. Also
  added ATLAS python library explicitly ahead of system python to
  prevent old libraries from being loaded by accident.
* Updated wrapper script to prefer TMPDIR over EDG_WL_SCRATCH (later
  is an anachronism from the lcg-RB).
* Source DDM setup.sh to ensure pilot has access to dq2 modules.
* http://en.wikipedia.org/wiki/Gunpowder_Plot


2009-07-08 "Totoro" Release (r635)

* Introduced "country" and "group" parameters for queues. These map to
  the pilot's -o and -v options allowing for pilots which only pick up
  particular types of jobs.
* Polling for queues now intelligently uses the country and group
  options above to only send pilots when activated jobs of that type
  are present at a site.
* Introduced "cloud" option to tag a queue to a panda cloud.
* Site and cloud status are polled and if 'offline' then pilot
  submission is supressed; if a site/cloud is in 'test' status then
  the pyfactory status flips to 'test' as well (limits pilot flow).
* Introuduced 'pilotDepthBoost' parameter (default 2), which allows
  the factory to submit up to queueDepth * pilotDepthBoost into a
  non-started state if there are sufficient jobs activated. This helps
  a lot if the site has short jobs where the lags in job status
  updates mean that sites can run short of pilots. You may want to set
  this higher at T1s (especially when doing reco).
  This is a more controlled version of the short job patch which was
  released in June 2009.
* Corrected a small bug in the default queue specification so that it
  works properly.
* Renamed the 'suppression' option to 'idlePilotSuppression'.
* Distributed wrapper script is now called "runpilot3-wrapper.sh",
  which is a much more sensible name.
* Default server is now pandaserver.cern.ch.
* Updated INSTALLATION file for new panda client location in SVN.
* Updates to README, INSTALLATION, Makefile corresponding to the above
  changes.
* Eilish wanted this release called 'Totoro', after one of her
  favourite Hayao Miyazaki films,
  http://en.wikipedia.org/wiki/My_Neighbour_Totoro.


2008-10-08 "Dawn Treader" Release (r485)

* GPL license.
* Internal defaults now work properly.
* Support for multiple gatekeepers on the same site. These are
  configured as space separated lists and the factory round robins
  between them each cycle (see sample configuration file). If you use
  this a lot then an expansion syntax could be supported - ask.
* Added a new parameter "supression". If this is > 1 then an idling
  pilot is only submitted to a site every "supression" cycles. Use
  this to mollify sysadmins in your cloud who get grumpy when pilots
  don't pick up. (When the site has activated jobs submission is as
  it was before.) Caveat Emptor: this will affect brokering.
* Added new panda server parameters "server" and "port" which let the
  pilot look for jobs from a different panda server. These do not need
  to be set for most factories (default settings:
  https://pandasrv.usatlas.bnl.gov:25443).
* Added a new parameter "env" which can list extra environment
  variables to be set for the pilot by condor. (N.B. be careful if you
  set a [QueueDefaults] value for "env" - the site's value will
  _overwrite_, not add to, the environment. If there is sufficient
  demand I can change this behaviour.)
* Changed runpilot3-script-stub.sh to unset all https_proxy
  environment variables at a site as this proxy mechanism is broken
  and rarely necessary for panda server ports.
* Support for multiple configuration files. These are all loaded and
  all are reloaded if any of them change. e.g.,
  --conf=generic.conf,uk.conf,analysis.conf
  N.B. Configuration files are loaded left to right and latter
  (re)defined options overwrite earlier ones.
* Print a warning if queue "status" value is invalid.
* Ruben named this release "Dawn Treader" after his favourite Narnia
  book so far. http://en.wikipedia.org/wiki/Dawn_Treader.


2008-06-17 "Moominsummer Madness" Release (r458)

* Updated pilot http download to pandamon.usatlas.bnl.gov.
* Added a timeout to curl for pilot tarball download (30s on
  connection, 180s total).
* Fixed the cleanLogs.py script so it now deletes old archived log
  files.
* Minor changes to documentation.
* No longer including a wrapper with a pilot uuencoded into it as it's
  recommended to download the latest pilot on the fly.
* Dedication: http://en.wikipedia.org/wiki/Tove_Jansson


2008-04-16 "Umber Hulk" Release. (r442)

* Upgraded runpilot3.sh to UHURA 18f pilot.
* Modified the order of the search for an LFC compatible python. The
  ATLAS release python is now tested first (it's usually more recent
  than the OS version). N.B. This may provoke a run time python
  warning about an API mismatch between the python versions.
* Added factoryId to configuration. This allows multiple factories to
  run on the same machine. This factory ID is used as the PANDA_JSID
  passed to the panda dashboard.
* Added baseLogFileUrl to pass the correct URL for the pilot wrapper's
  logfiles to the panda dashboard.
* Added optional "user" field to allow pilots to be sent to pick up
  particular types of jobs from panda, using the "-u USER" argument.
  If this is absent or None then nothing is passed with the pilot
  arguments (i.e., pickup normal production jobs).
* Changed configuration so that each configuration section is a
  "queue" which can have a gatekeeper section. This allows pilots to
  be sent to the same queue on the CE, but having different "user"
  parameters. If there is no "gatekeeper" specified then the name of
  the configuration section name is the gatekeeper contact string, as
  before.
* New example factory configuration file, factory.conf-example makes
  use of the above clearer.
* Many internal changes to support the above feature.
* Modified the main submit code to sort alphabetically by queue name.
* cleanLogs.py now much improved. Will read factory log file directory
  location from factory.conf. Options for verbosity, days to compress
  after and days to delete after.
* This release is dedicated to Gary Gygax. d20... 19 hit!
  http://en.wikipedia.org/wiki/Gary_Gygax


2008-02-28 "Ice Moon" Release. (r436)

* Upgraded tarball to UHURA12a pilot.
* Added makefile target to rebuild the wrapper script with pilot
  tarball downloaded from subversion (you need svn installed!).
* Changed tarball http download to use BNL cache first, with fallback
  to Glasgow (Glasgow will rebuild nightly).
* Much better exception handling if there is a problem polling condor
  status or panda status - factory raises an internal exception and
  then skips a cycle.
* Added an outer level exception handler which gives instructions as
  to how to report the error then reraises error.

2008-02-08 "It's REALLY all about me" Release. (r416)

* Updated to UHURA11b2-2 pilot.


2008-02-08 "It's all about me" Release. (r410)

* Major change to pilot delivery mechanism, attaching pilot tarball as
  a uuencoded suffix of the wrapper script. This cures problems with
  batch systems which do not deliver "input" files properly on STDIN
  (bqs - there was nothing there; condor - crashes job).
  *** YOU SHOULD REMOVE THE input PARAMETER FROM factory.conf.
* Added GTAG environment variable to move towards integration with
  panda dashboard (as autopilot does).
* Fixed a bug where the factory would crash if the condor_q query
  failed (factory now raises a CondorStatusFailure exception, catches
  it and skips a cycle).
* Fixed a bug where exception arguments were improperly specified.
* Added the "-k MEM" flag to pilot where a site has a memory limit
  specified (should be used for sites lacking memory).
* Cosmetic: print timestamp at the beginning of each submit cycle.
* Upgraded to UHURA11a pilot as default.
* Improved cleanLogs.py script and renamed mkPilotWrapper.py.
</pre>

</body></html>
