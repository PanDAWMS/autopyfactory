%define name autopyfactory
%define version 2.4.1
%define unmangled_version 2.4.1
%define release 2

Summary: autopyfactory package
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: GPL
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Jose Caballero <jcaballero@bnl.gov>
Provides: autopyfactory
Obsoletes: panda-autopyfactory
Requires: panda-userinterface >= 1.0-4 
Requires: python-simplejson 
Requires: python-pycurl 
Requires: voms-clients 
Requires: myproxy
Url: https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA

%description
This package contains autopyfactory

%prep
%setup -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

mkdir -pm0755 $RPM_BUILD_ROOT%{_var}/log/autopyfactory

# add .gz extension to man files
sed -i '/\/man\/man[1-9]\/.*\.[1-9]/s/$/\.gz/' INSTALLED_FILES

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
# By default on install set factory off?
#/sbin/chkconfig autopyfactory off


%preun
# Stop factory before uninstalling or upgrading. 
#if [ -x /etc/init.d/autopyfactory ] ; then
#  /etc/init.d/autopyfactory stop > /dev/null 2>&1
#fi


%files -f INSTALLED_FILES
%defattr(-,root,root)
%doc docs/* etc/*-example etc/logrotate/ etc/sysconfig/ README
