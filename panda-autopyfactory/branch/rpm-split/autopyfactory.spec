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

%prep
%setup -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

# --- Files for autopyfactory-core subpackage
cp INSTALLED_FILES CORE_FILES
sed -i '/proxymanager/d' CORE_FILES

# --- Files for autopyfactory-proxymanager subpackage
cp INSTALLED_FILES PROXYMANAGER_FILES
sed -i '/proxymanager/!d' PROXYMANAGER_FILES




mkdir -pm0755 $RPM_BUILD_ROOT%{_var}/log/autopyfactory

%clean
rm -rf $RPM_BUILD_ROOT

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


####################################

%package -n autopyfactory-core
Summary: autopyfactory core 
Group: Development/Libraries
%description -n autopyfactory-core
This package contains autopyfactory core

#%files -n autopyfactory-core -f INSTALLED_FILES
%files -n autopyfactory-core -f CORE_FILES
%defattr(-,root,root)
%doc docs/* etc/*-example etc/logrotate/ etc/sysconfig/ README

####################################

%package -n autopyfactory-proxymanager
Summary: autopyfactory proxymanager 
Group: Development/Libraries
%description -n autopyfactory-proxymanager
This package contains autopyfactory proxymanger 

%files -n autopyfactory-proxymanager -f PROXYMANAGER_FILES
%defattr(-,root,root)
%doc docs/* etc/*-example etc/logrotate/ etc/sysconfig/ README

####################################


