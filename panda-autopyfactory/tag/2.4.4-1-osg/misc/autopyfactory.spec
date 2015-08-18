%define name autopyfactory
%define version 2.4.4
%define unmangled_version 2.4.4
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

# ----- treat the config files properly:
#       with a non-override policy on RPM update 
sed -i '/\/etc\/autopyfactory\/.*\.conf/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/logrotate\.d\/autopyfactory/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/sysconfig\/autopyfactory/ s/^/%config(noreplace) /'  INSTALLED_FILES
sed -i '/\/etc\/sysconfig\/proxymanager/ s/^/%config(noreplace) /'  INSTALLED_FILES

# ----- Files for autopyfactory-core subpackage
cp INSTALLED_FILES CORE_FILES
sed -i '/proxymanager/d' CORE_FILES
sed -i '/plugins\/.*\/.*Condor.*/d' CORE_FILES

# ----- Files for autopyfactory-proxymanager subpackage
cp INSTALLED_FILES PROXYMANAGER_FILES
sed -i '/proxymanager/!d' PROXYMANAGER_FILES

# ----- Files for autopyfactory-plugins-condor subpackage
cp INSTALLED_FILES PLUGINS-CONDOR_FILES
sed -i '/plugins\/.*\/.*Condor.*/!d' PLUGINS-CONDOR_FILES

# ----- Files for autopyfactory-plugins-panda subpackage
cp INSTALLED_FILES PLUGINS-PANDA_FILES
sed -i '/plugins\/wmsstatus\/.*Panda.*/!d' PLUGINS-PANDA_FILES

# ----- Files for autopyfactory-plugins-cloud subpackage
cp INSTALLED_FILES PLUGINS-CLOUD_FILES
sed -i '/plugins\/.*\/.*EC2.*/!d' PLUGINS-CLOUD_FILES

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
#   SUB PACKAGE AUTOPYFACTORY-CORE
##############################################

%package -n autopyfactory-core
Summary: autopyfactory core 
Group: Development/Libraries
Requires: autopyfactory-proxymanager
Requires: autopyfactory-plugins-condor
%description -n autopyfactory-core
This package contains autopyfactory core

#%files -n autopyfactory-core -f INSTALLED_FILES
%files -n autopyfactory-core -f CORE_FILES
%defattr(-,root,root)
## FIXME !!!
%doc README    


##############################################
#   SUB PACKAGE AUTOPYFACTORY-PROXYMANAGER
##############################################

%package -n autopyfactory-proxymanager
Summary: autopyfactory proxymanager 
Group: Development/Libraries
%description -n autopyfactory-proxymanager
This package contains autopyfactory proxymanger 

%files -n autopyfactory-proxymanager -f PROXYMANAGER_FILES
%defattr(-,root,root)
#%doc docs/* etc/*-example etc/logrotate/ etc/sysconfig/ README  # ?? 

##############################################
#   SUB PACKAGE AUTOPYFACTORY-PLUGINS-CONDOR
##############################################

%package -n autopyfactory-plugins-condor
Summary: autopyfactory plugins condor 
Group: Development/Libraries
%description -n autopyfactory-plugins-condor
This package contains autopyfactory plugins condor

%files -n autopyfactory-plugins-condor -f PLUGINS-CONDOR_FILES
%defattr(-,root,root)

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
#   META RPMs
##############################################

%package -n autopyfactory-panda
Summary: META RPM for PanDA
Group: Development/Libraries
Requires: autopyfactory-core
Requires: autopyfactory-plugins-panda
Requires: panda-client
#Requires: autopyfactory-wrappers, voms-client, myproxy
%description -n autopyfactory-panda
meta rpm autopyfactory-panda
%files -n autopyfactory-panda


%package -n autopyfactory-wms
Summary: META RPM for autopyfactory-wms 
Group: Development/Libraries
Requires: autopyfactory-core
#Requires: autopyfactory-wrappers, voms-client
%description -n autopyfactory-wms
meta rpm autopyfactory-wms
%files -n autopyfactory-wms


%package -n autopyfactory-cloud
Summary: META RPM for autopyfactory-cloud
Group: Development/Libraries
Requires: autopyfactory-core
Requires: autopyfactory-plugins-cloud
#Requires: autopyfactory-wrappers
%description -n autopyfactory-cloud
meta rpm autopyfactory-cloud
%files -n autopyfactory-cloud

##############################################

