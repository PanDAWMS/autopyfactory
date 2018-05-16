%define name autopyfactory
%define version 2.4.14
%define unmangled_version 2.4.14
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
#BuildRequires: condor-all
BuildRequires: condor-python
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


mkdir -pm0755 $RPM_BUILD_ROOT%{_var}/log/autopyfactory

##############################################
#   CLEAN
##############################################

%clean
rm -rf $RPM_BUILD_ROOT

##############################################
#   PACKAGE AUTOPYFACTORY
##############################################

%package -n autopyfactory
Summary: autopyfactory
Group: Development/Libraries
Requires: python-simplejson
Requires: python-pycurl
%description -n autopyfactory
This package contains autopyfactory


%pre -n autopyfactory
if id autopyfactory > /dev/null 2>&1; then
    : # do nothing
else
    /usr/sbin/useradd --comment "AutoPyFactory service account" --shell /bin/bash autopyfactory
fi

%post -n autopyfactory
/sbin/chkconfig --add autopyfactory

%preun -n autopyfactory
#if [ -x /etc/init.d/autopyfactory ] ; then
#  /etc/init.d/autopyfactory stop > /dev/null 2>&1
#fi

#---------------------------------------------

%files -n autopyfactory -f INSTALLED_FILES
%defattr(-,root,root)
%doc README  