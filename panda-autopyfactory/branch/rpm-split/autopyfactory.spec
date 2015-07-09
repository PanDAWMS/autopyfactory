%define name autopyfactory
%define version 2.4.3
%define unmangled_version 2.4.3
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

# ----- Files for autopyfactory-core subpackage
cp INSTALLED_FILES CORE_FILES
sed -i '/proxymanager/d' CORE_FILES

# ----- Files for autopyfactory-proxymanager subpackage
cp INSTALLED_FILES PROXYMANAGER_FILES
sed -i '/proxymanager/!d' PROXYMANAGER_FILES

# ----- Files for autopyfactory-plugins-condor subpackage
cp INSTALLED_FILES PLUGINS-CONDOR_FILES
sed -i '/plugins\/.*\/.*Condor.*/!d' PLUGINS-CONDOR_FILES



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
%description -n autopyfactory-core
This package contains autopyfactory core

#%files -n autopyfactory-core -f INSTALLED_FILES
%files -n autopyfactory-core -f CORE_FILES
%defattr(-,root,root)
%doc docs/* etc/*-example etc/logrotate/ etc/sysconfig/ README

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


