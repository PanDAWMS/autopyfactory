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
<p />
<h1><a name="PanDA_based_workflow_with_AutoPy"></a> PanDA-based workflow with AutoPyFactory </h1>
<li> 1  About this Document</a>
</li> <li> 2  Applicable versions</a>
</li> <li> 3  Configuration</a>
</li> <li> 4  Wrapper</a> <ul>
<li> 4.1  Applicable version</a>
</li> <li> 4.2  Deployment</a>
</li> <li> 4.3  Input options</a>
</li></ul> 
</li></ul> 
<p />
<h1><a name="1_About_this_Document"></a> 1  About this Document </h1>
<p />
This document describes how to configure AutoPyFactory when the WMS is PanDA.
<p />
<p />
<h1><a name="2_Applicable_versions"></a> 2  Applicable versions </h1>
<p />
This documentation applies to the latest version of APF: 2.4.9
<p />
<p />
<h1><a name="3_Configuration"></a> 3  Configuration </h1>
<p />
To use PanDA as WMS system, set accordingly the wmsstatus plugin:
<p />
<pre class="file">
wmsstatusplugin = Panda
</pre>
<p />
Each APFQueue must include the PanDA queue and the PanDA resource names, needed by the ATLAS pilot to request a job from the Dispatcher. For example:
<p />
<pre class="file">
batchqueue = ANALY_BNL_LONG-condor
wmsqueue = ANALY_BNL_LONG
</pre>
<p />
The rest of the configuration depends on the type of submission, documented in other sections.
<p />
<h1><a name="4_Wrapper"></a> 4  Wrapper </h1>
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_1_Applicable_version"></a> 4.1  Applicable version </span></h2>
<p />
This section describes how to use wrapper version 0.9.16
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_2_Deployment"></a> 4.2  Deployment </span></h2>
<p />
Install the RACF yum repo files:
<p />
<pre class="rootscreen">
[root@factory ~]$ rpm -Uhv http://dev.racf.bnl.gov/yum/grid/production/rhel/6Workstation/x86_64/racf-grid-release-latest.noarch.rpm
Retrieving http://dev.racf.bnl.gov/yum/grid/production/rhel/6Workstation/x86_64/racf-grid-release-latest.noarch.rpm
warning: /var/tmp/rpm-tmp.TaawC5: Header V3 DSA/SHA1 Signature, key ID e6f6b87c: NOKEY
Preparing...                ########################################### [100%]
   1:racf-grid-release      ########################################### [100%]
</pre>
<p />
Install the wrapper:
<p />
<pre class="rootscreen">
[root@factory ~]$ yum install wrapper
</pre>
<p />
NOTE: if you want to to keep older versions of the wrapper when installing or updating to latest one, add the package <code>wrapper</code> to the yum configuration variable <code>installonlypckgs</code> in file <code>/etc/yum.conf</code>, and ensure <code>installonly_limit</code> is high enough (documentation can be found in manpage for yum.conf). Example: 
<p />
<pre class="file">
installonly_limit=3
installonlypkgs=wrapper
</pre>
<p />
<p />
<h2 class="twikinetRoundedAttachments"><span class="twikinetHeader"><a name="4_3_Input_options"></a> 4.3  Input options </span></h2>
<p />
<table class="tg">
  <tr>
    <th class="tg-header">variable</th>
    <th class="tg-header">description</th>
    <th class="tg-header">comments</th>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperloglevel=</td>
    <td class="tg-031e"> the log level. Current valid values are <code>info</code> and <code>debug</code> </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperpilotcode=</td>
    <td class="tg-031e"> the location of the pilot code. <br> Accepts more than one value, split by comma, to pick one randomly (see --wrapperpilotcoderatio). <br> It is possible to set a set of URIs to just failover from one to another, without randomization. In that case, the list is enclosed between square parenthesis <code>[</code> and <code>]</code>. </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperpilotcodechecksum=</td>
    <td class="tg-031e"> the checksum of the pilot code. If more than one URIs for the pilotcode are provided, then a checksum value for each one of them is required, in a list split-by-comma </td>
    <td class="tg-031e"> Optional</td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperpilotcoderatio=</td>
    <td class="tg-031e"> when a list of values split-by-comma are passed to the variable wrapprepilotcode, it sets the ratio to pick randomly between them </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperplatform=</td>
    <td class="tg-031e">set the type of grid middleware available on the node. The wrapper may take some actions depending on the value. <br> Current valid values are: <code>OSG</code> and <code>EGI</code>. </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperplugin=</td>
    <td class="tg-031e"> the exact plugin that will be invoked to run the final payload. <br> Current valid values are: <code>atlasosg</code>, <code>atlasegi</code>, and <code>trivial</code>. </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperspecialcmd=</td>
    <td class="tg-031e"> a special command needed to be performed before anything else. Very rearly needed. When needed, usually is a command to source an had-oc setup.sh file. <br> Use with caution.</td>
    <td class="tg-031e"> Optional</td>
  </tr>
<tr>
    <td class="tg-raw1">--wrappertarballchecksum=</td>
    <td class="tg-031e"> the checksum of the wrapper tarball</td>
    <td class="tg-031e"> Optional</td>
  </tr>
<tr>
    <td class="tg-raw1">--wrappertarballuri=</td>
    <td class="tg-031e"> it is the URI with the location of the wrapper tarball. </td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">--wrapperwmsqueue=</td>
    <td class="tg-031e">the name of the queue in the WMS system (for example, in PanDA).</td>
    <td class="tg-031e"> </td>
  </tr>
<tr>
    <td class="tg-raw1">other options</td>
    <td class="tg-031e"> Any other input option, not starting with string <code>--wrapper</code>, will be passed <strong>verbatim</strong> to the payload job (a.k.a. pilot) </td>
    <td class="tg-031e"> </td>
  </tr>
</table>
<p />
A typical example for ATLAS is like this:
<p />
<pre class="file">
executable = /usr/libexec/wrapper-0.9.16.sh
arguments = --wrapperloglevel=debug \
           --wrapperplatform=OSG \
           --wrapperwmsqueue=ANALY_BNL_SHORT \
           --wrappertarballuri=http://dev.racf.bnl.gov/dist/wrapper/wrapperplugins-dev12.tar.gz \
           --wrapperpilotcode=http://pandaserver.cern.ch:25085/cache/pilot/pilotcode-PICARD.tar.gz \
           --wrapperplugin=atlasosg \
           -w https://pandaserver.cern.ch -p 25443 -u user
</pre>
<p />
Here are examples on how to pass more than one value to <code>--wrapperpilotcode</code>.
<p />
<pre class="file">
arguments = ... \
           --wrapperpilotcode = ['file:///cvmfs/atlas.cern.ch/pilot.py', 'http://atlas.cern.ch/pilot.tar.gz'], 'http://atlas.cern.ch/pilot-devel.tar.gz' \
           --wrapperpilotcoderatio = 99,1 \
           ...
</pre>
<p />
In that case, 99% of the times the pilot code from CVMFS will be tried, failing over the tarball at the CERN URL in case of failures. 1% of the times the development pilot code will be downloaded.
<p />
<pre class="file">
arguments = ... \
           --wrapperpilotcode = ['file:///cvmfs/atlas.cern.ch/pilot.py', 'http://atlas.cern.ch/pilot.tar.gz'], 'http://atlas.cern.ch/pilot-devel.tar.gz' \
           --wrapperpilotcodechecksum = 123,456,789 \
           --wrapperpilotcoderatio = 99,1 \
           ...
</pre>
<p />
In the second case, the same logic applies, but a checksum is passed for each possible case. 
<p />

</body></html>
