%define name autopyfactory
%define version 2.4.6
%define unmangled_version 2.4.6
%define release 1

Summary: autopyfactory package
Name: %{name}
Version: %{version}
Release: %{release}%{?dist}
Source0: %{name}-%{unmangled_version}.tar.gz
License: GPL
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Jose Caballero <jcaballero@bnl.gov>
Provides: autopyfactory
Obsoletes: panda-autopyfactory
Url: https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA

%description
This package contains autopyfactory

##############################################
#   PREP
##############################################

%prep
%setup -n %{name}-%{unmangled_version}

##############################################
#   BUILD
##############################################

%build
python setup.py build

##############################################
#   INSTALL
##############################################

%install
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

# ----------------------------------------------------------------------------
#       treat the config files properly:
#       with a non-override policy on RPM update 
# ----------------------------------------------------------------------------
sed -i '/\/etc\/autopyfactory\/.*\.conf/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/logrotate\.d\/autopyfactory/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/sysconfig\/autopyfactory/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/sysconfig\/proxymanager/ s/^/%config(noreplace) /'  INSTALLED_FILES

# ----------------------------------------------------------------------------
#       Files for autopyfactory-common subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES COMMON_FILES
sed -i '/proxymanager/d' COMMON_FILES
sed -i '/plugins\/sched/d' COMMON_FILES
sed -i '/plugins\/monitor/d' COMMON_FILES
sed -i '/plugins\/batchstatus/d' COMMON_FILES
grep '/plugins/batchstatus/__init__' INSTALLED_FILES >> COMMON_FILES
grep '/plugins/batchstatus/CondorBatchStatus.*' INSTALLED_FILES >> COMMON_FILES
sed -i '/plugins\/wmsstatus/d' COMMON_FILES
grep '/plugins/wmsstatus/__init__' INSTALLED_FILES >> COMMON_FILES
sed -i '/plugins\/batchsubmit/d' COMMON_FILES
grep '/plugins/batchsubmit/__init__' INSTALLED_FILES >> COMMON_FILES
sed -i '/\etc\/autopyfactory\/proxy\.conf/d' COMMON_FILES
sed -i '/external\/panda/d' COMMON_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-proxymanager subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PROXYMANAGER_FILES
sed -i '/proxymanager/!d' PROXYMANAGER_FILES
grep '/etc/autopyfactory/proxy\.conf' INSTALLED_FILES >> PROXYMANAGER_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-monitor subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-MONITOR_FILES
sed -i '/plugins\/monitor\//!d' PLUGINS-MONITOR_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-local subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-LOCAL_FILES
sed -i '/plugins\/batchsubmit\/.*Local.*/!d' PLUGINS-LOCAL_FILES
grep "/plugins/wmsstatus/CondorWMSStatusPlugin" INSTALLED_FILES >> PLUGINS-LOCAL_FILES
grep "/plugins/batchsubmit/.*Exec.*" INSTALLED_FILES >> PLUGINS-LOCAL_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-remote subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-REMOTE_FILES
sed -i '/plugins\/batchsubmit\/.*Condor.*/!d' PLUGINS-REMOTE_FILES
sed -i '/plugins\/batchsubmit\/.*EC2.*/d' PLUGINS-REMOTE_FILES
sed -i '/plugins\/batchsubmit\/.*Local.*/d' PLUGINS-REMOTE_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-cloud subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-CLOUD_FILES
sed -i '/plugins\/.*\/.*EC2.*/!d' PLUGINS-CLOUD_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-scheds subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-SCHEDS_FILES
sed -i '/plugins\/sched\//!d' PLUGINS-SCHEDS_FILES


# ----------------------------------------------------------------------------
#       Files for autopyfactory-plugins-panda subpackage
# ----------------------------------------------------------------------------
cp INSTALLED_FILES PLUGINS-PANDA_FILES
sed -i '/plugins\/wmsstatus\/.*Panda.*/!d' PLUGINS-PANDA_FILES
grep '/external/panda/' INSTALLED_FILES >> PLUGINS-PANDA_FILES


mkdir -pm0755 $RPM_BUILD_ROOT%{_var}/log/autopyfactory

##############################################
#   CLEAN
##############################################

%clean
rm -rf $RPM_BUILD_ROOT

##############################################
#   SCRIPTS
##############################################

%pre
if id autopyfactory > /dev/null 2>&1; then
    : # do nothing
else
    /usr/sbin/useradd --comment "AutoPyFactory service account" --shell /bin/bash autopyfactory
fi 

%post
/sbin/chkconfig --add autopyfactory

%preun
#if [ -x /etc/init.d/autopyfactory ] ; then
#  /etc/init.d/autopyfactory stop > /dev/null 2>&1
#fi


##############################################
#   SUB PACKAGE AUTOPYFACTORY-COMMON
##############################################

%package -n autopyfactory-common
Summary: autopyfactory common 
Group: Development/Libraries
#Requires: autopyfactory-proxymanager
Requires: condor
Requires: python-simplejson
Requires: python-pycurl
%description -n autopyfactory-common
This package contains autopyfactory common

%files -n autopyfactory-common -f COMMON_FILES
%defattr(-,root,root)
%doc README    


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PROXYMANAGER
##############################################

%package -n autopyfactory-proxymanager
Summary: autopyfactory proxymanager 
Group: Development/Libraries
Requires: voms-clients
Requires: myproxy
%description -n autopyfactory-proxymanager
This package contains autopyfactory proxymanger 

%files -n autopyfactory-proxymanager -f PROXYMANAGER_FILES


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-PANDA
##############################################

%package -n autopyfactory-plugins-panda
Summary: autopyfactory plugins panda 
Group: Development/Libraries
%description -n autopyfactory-plugins-panda
This package contains autopyfactory plugins panda 

%files -n autopyfactory-plugins-panda -f PLUGINS-PANDA_FILES
%defattr(-,root,root)


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-LOCAL
##############################################

%package -n autopyfactory-plugins-local
Summary: autopyfactory plugins local 
Group: Development/Libraries
%description -n autopyfactory-plugins-local
This package contains autopyfactory plugins local 

%files -n autopyfactory-plugins-local -f PLUGINS-LOCAL_FILES
%defattr(-,root,root)


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-REMOTE
##############################################

%package -n autopyfactory-plugins-remote
Summary: autopyfactory plugins remote 
Group: Development/Libraries
%description -n autopyfactory-plugins-remote
This package contains autopyfactory plugins remote 

%files -n autopyfactory-plugins-remote -f PLUGINS-REMOTE_FILES
%defattr(-,root,root)


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-CLOUD
##############################################

%package -n autopyfactory-plugins-cloud
Summary: autopyfactory plugins cloud 
Group: Development/Libraries
%description -n autopyfactory-plugins-cloud
This package contains autopyfactory plugins cloud 

%files -n autopyfactory-plugins-cloud -f PLUGINS-CLOUD_FILES
%defattr(-,root,root)


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-SCHED
##############################################

%package -n autopyfactory-plugins-scheds
Summary: autopyfactory plugins scheds
Group: Development/Libraries
%description -n autopyfactory-plugins-scheds
This package contains autopyfactory plugins scheds

%files -n autopyfactory-plugins-scheds -f PLUGINS-SCHEDS_FILES
%defattr(-,root,root)

##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-MONITOR
##############################################

%package -n autopyfactory-plugins-monitor
Summary: autopyfactory plugins monitor
Group: Development/Libraries
%description -n autopyfactory-plugins-monitor
This package contains autopyfactory plugins monitor

%files -n autopyfactory-plugins-monitor -f PLUGINS-MONITOR_FILES
%defattr(-,root,root)


##############################################
#   META RPMs
##############################################

%package -n autopyfactory-remote
Summary: META RPM for more standard scenario
Group: Development/Libraries
Requires: autopyfactory-common
Requires: autopyfactory-plugins-remote
Requires: autopyfactory-proxymanager
#Requires: voms-clients
#Requires: myproxy
%description -n autopyfactory-remote
meta rpm autopyfactory-remote
%files -n autopyfactory-remote


%package -n autopyfactory-panda
Summary: META RPM for PanDA
Group: Development/Libraries
Requires: autopyfactory-common
Requires: autopyfactory-plugins-panda
#Requires: voms-clients
#Requires: myproxy
%description -n autopyfactory-panda
meta rpm autopyfactory-panda
%files -n autopyfactory-panda


%package -n autopyfactory-wms
Summary: META RPM for autopyfactory-wms 
Group: Development/Libraries
Requires: autopyfactory-common
Requires: autopyfactory-plugins-local
#Requires: voms-clients
%description -n autopyfactory-wms
meta rpm autopyfactory-wms
%files -n autopyfactory-wms


%package -n autopyfactory-cloud
Summary: META RPM for autopyfactory-cloud
Group: Development/Libraries
Requires: autopyfactory-common
Requires: autopyfactory-plugins-cloud
Requires: autopyfactory-proxymanager
%description -n autopyfactory-cloud
meta rpm autopyfactory-cloud
%files -n autopyfactory-cloud

##############################################
