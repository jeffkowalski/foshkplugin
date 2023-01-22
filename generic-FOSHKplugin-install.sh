#!/bin/bash
# installs the FOSHKplugin to connect a weather station from Fine Offset (FOSHK)
# Oliver Engel, 01.12.19, 28.12.19, 18.01.20, 22.05.20, 20.07.20, 10.10.20, 19.02.21, 08.05.21, 27.06.21, 05.03.22
# https://foshkplugin.phantasoft.de
#
# contains foshkplugin.py and foshkplugin.service as well as generic-FOSHKplugin-install.sh (this script)
#
# Preliminary work:
# Create directory
# sudo mkdir /opt/FOSHKplugin
#
# Change to the created directory
# cd /opt/FOSHKplugin
#
# Get the current version of the plugin via wget or curl
# wget -N https://foshkplugin.phantasoft.de/files/generic-FOSHKplugin.zip
# or
# curl -O https://foshkplugin.phantasoft.de/files/generic-FOSHKplugin.zip
#
# Unzip the ZIP file
# unzip -o generic-FOSHKplugin.zip
#
# Grant execute right for generic-FOSHKplugin-install.sh (this script)
# chmod u+x generic-FOSHKplugin-install.sh
#
# Run generic-FOSHKplugin-install.sh (this script)
# sudo ./generic-FOSHKplugin-install.sh --install
#
#
# for a later update of FOSHKplugin:
# sudo ./generic-FOSHKplugin-install.sh --update
# or to directly install a certain version generic-FOSHKplugin-0.0.8Beta.zip:
# sudo ./generic-FOSHKplugin-install.sh --update generic-FOSHKplugin-0.0.8Beta.zip

PRGNAME=FOSHKplugin
PRGVER=v0.11
MYDIR=`pwd`
REPLY=n
ISTR="+++ $PRGNAME +++"
setWSConfig="not written"
zipname=generic-FOSHKplugin.zip
SVC_NAME=foshkplugin

# check current user context
if [ "${SUDO_USER}" == "" ] || [ "${SUDO_USER}" == "root" ]; then ISUSR=${USER}; else ISUSR=${SUDO_USER}; fi

aptInstall() {
  sudo apt-get update
  sudo apt-get -y install --no-upgrade python3 python3-setuptools python3-pip
  #echo
}

pipInstall() {
  sudo pip3 install --upgrade requests paho-mqtt influxdb
  #echo
}

adjustPerm() {
  # Adjust the ownership of all files if necessary
  echo
  echo $ISTR change owner of $ISDIR/\* to "${ISUSR}:${ISUSR}"
  f=(*)
  if [[ -f "${f[0]}" ]]; then
    chown -vf "${ISUSR}:${ISUSR}" $ISDIR
    chown -vf "${ISUSR}:${ISUSR}" *
  fi
  echo
}

listSVC() {
  # show currently installed services containing "foshk"
  echo current services containing \"foshk\":
  sudo systemctl list-unit-files|grep foshk
  echo
}

checkWrite() {
  # check if dir is writable for current user
  ISDIR=`pwd`
  ME=`whoami`
  OWNER=`stat -c %U $ISDIR`
  echo
  if [ -w ${ISDIR} ]; then
    # writable
    echo "running in ${ISDIR} as user ${ME} - owner: ${OWNER}, USR: ${ISUSR}"
  else
    # permission denied
    echo "write permission to ${ISDIR} denied!"
    echo "you have to run this script as user ${OWNER} (given: ${ME}/${ISUSR}) or change owner"
    echo "of ${ISDIR} to ${ME} with chown -R ${ME} ${ISDIR} and try again"
    echo
    echo "nothing done"
    exit
  fi
  echo
}

getVars() {
  while IFS=$' =\t\n' read -r key value garbage
  do
    #if [ "${key:0:1}" == "[" ]; then echo "******************** " $key; fi          # shows section name
    #echo "*${key}* --> *${value}*"
    if [ "${value:0:1}" == "#" ]; then value=""; fi
    if [ "$key" == "LOX_IP" ]; then LOX_IP=$value; fi
    if [ "$key" == "LOX_PORT" ]; then LOX_PORT=$value; fi
    if [ "$key" == "LB_IP" ]; then LB_IP=$value; fi
    if [ "$key" == "LBH_PORT" ]; then LBH_PORT=$value; fi
    if [ "$key" == "WS_IP" ]; then WS_IP=$value; fi
    if [ "$key" == "WS_PORT" ]; then WS_PORT=$value; fi
    if [ "$key" == "WS_INTERVAL" ]; then WS_INTERVAL=$value; fi
    if [ "$key" == "SVC_NAME" ]; then SVC_NAME=$value; fi
  done <"foshkplugin.conf"
}

cls() {
  echo
echo
}

clear
echo
echo Phantasoft install-Script for $PRGNAME $PRGVER
echo -----------------------------------------------
echo

# read current config
getVars

if [ "$1" = "-update" ] || [ "$1" = "--update" ] || [ "$1" = "-upgrade" ] || [ "$1" = "--upgrade" ]; then
  echo $ISTR going to update $PRGNAME

  # check write permission
  checkWrite

  echo $ISTR save the current configuration
  cp -v foshkplugin.conf foshkplugin.save

  # if you want to upgrade to a specific FOSHKplugin-version
  #if [ "$2" != "" ]; then zipname=$2; fi
  if [ "$2" != "" ]; then 
    zipname=$2
    zipname="${zipname##*/}"
  fi

  # install wget & unzip
  if [ "`which wget`" == "" ] || [ "`which unzip`" == "" ]; then
    echo $ISTR ensure existence of necessary programs wget and unzip
    apt-get update
    apt-get -y install --no-upgrade wget unzip
  fi

  # download new version from web
  echo $ISTR get new $PRGNAME $zipname from the web
  wget -N https://foshkplugin.phantasoft.de/files/$zipname 2>/dev/null || curl -O https://foshkplugin.phantasoft.de/files/$zipname

  echo $ISTR unzipping the new file
  unzip -o $zipname

  echo $ISTR recover saved Config
  cp -v foshkplugin.conf foshkplugin.new
  cp -v foshkplugin.save foshkplugin.conf

  echo $ISTR set executable to foshkplugin.py
  chmod 711 -v foshkplugin.py
  chmod 711 -v generic-FOSHKplugin-install.sh

  # which servicename to use?
  mySVC_NAME=$SVC_NAME
  echo "$ISTR"
  echo
  read -p "Define the name of the running service to restart: [$mySVC_NAME]: " SVC_NAME
  SVC_NAME=${SVC_NAME:-$mySVC_NAME}

  # retain old configuration in case of updating a not running FOSHKplugin
  if diff -q foshkplugin.save foshkplugin.conf >/dev/null && test -f foshkplugin.conf.foshkbackup; then
    cp -p foshkplugin.conf.foshkbackup foshkplugin.conf
    echo old configuration file foshkplugin.conf.foshkbackup retained as foshkplugin.conf
  fi

  # adjust file ownerships
  adjustPerm

  echo $ISTR restarting $PRGNAME-service $SVC_NAME if running
  sudo systemctl is-active --quiet $SVC_NAME && sudo systemctl restart $SVC_NAME

  echo
  echo $ISTR upgrade-installation complete
elif [ "$1" = "-uninstall" ] || [ "$1" = "--uninstall" ]; then
  # uninstall service
  echo $ISTR uninstalling $PRGNAME as service $SVC_NAME
  echo
  # which servicename to use?
  if [ "$2" != "" ]; then
    SVC_NAME=$2
  else
    # which servicename to use?
    listSVC
    mySVC_NAME=$SVC_NAME
    echo "$ISTR"
    echo
    read -p "Define the name of current service to uninstall: [$mySVC_NAME]: " SVC_NAME
    SVC_NAME=${SVC_NAME:-$mySVC_NAME}
  fi
  echo $ISTR disable system-service $SVC_NAME
  sudo systemctl stop $SVC_NAME
  sudo systemctl disable $SVC_NAME
  sudo systemctl daemon-reload

  echo $ISTR remove system-service $SVC_NAME from auto-start
  sudo rm -f /etc/systemd/system/$SVC_NAME.service

  echo
  echo $ISTR system-service $SVC_NAME should be uninstalled
  echo $ISTR all files are still in $MYDIR
  echo
elif [ "$1" = "-install" ] || [ "$1" = "--install" ]; then
  echo $ISTR install and configure $PRGNAME

  # check write permission
  checkWrite

  # install required packages
  read -t 1 -n 10000 discard
  echo
  echo $ISTR we will now install Python3 via apt
  aptInstall

  # install required Python packages
  echo
  echo $ISTR now we have to install the python-libs
  pipInstall

  # Adjust file rights
  echo
  echo $ISTR set executable to foshkplugin.py
  chmod 711 -v foshkplugin.py
  # Adjust path names for log in the config
  if test -f foshkplugin.conf; then
    /bin/sed -i "s#REPLACEFOSHKPLUGINLOGDIR#$MYDIR#" foshkplugin.conf
  fi

  # adjust file ownerships
  adjustPerm

  # gather weather station for details - only if not set already
  if [ "$WS_IP" == "" ] || [ "$WS_PORT" == "" ] || [ "$WS_INTERVAL" == "" ]; then
    echo "$ISTR attempt auto configuration"
    ./foshkplugin.py -autoConfig
  fi

  # Configuration
  until [[ $REPLY =~ ^[YyJj]$ ]]; do
    # query Target-IP
    if [ "$LOX_IP" == "" ] || [ "$LOX_IP" == "none" ]; then myLOX_IP=""; else myLOX_IP=$LOX_IP; fi
    cls
    echo "$ISTR (1/6)"
    echo
    echo "FOSHKplugin can automatically forward any incoming message from the weather"
    echo "station via UDP to a global destination - to be defined here."
    echo "If this forwarding is required, please enter the IP address of the destination"
    echo "here. If not, enter "-" without inverted commas - simply type a -"
    echo "ENTER accepts the specified value in square brackets."
    echo
    read -p "ip address of UDP-target system (use - for no UDP) [$myLOX_IP]: " LOX_IP
    LOX_IP=${LOX_IP:-$myLOX_IP}
    if [ ${#LOX_IP} -le 1 ]; then LOX_IP=""; fi

    # query Target-Port
    if [ "$LOX_PORT" == "" ] || [ "$LOX_PORT" == "none" ]; then myLOX_PORT=""; else myLOX_PORT=$LOX_PORT; fi
    cls
    echo "$ISTR (2/6)"
    echo
    echo "FOSHKplugin can automatically forward any incoming message from the weather"
    echo "station via UDP to a global destination - to be defined here."
    echo "If this forwarding is required, please enter the UDP port of the destination"
    echo "here. If not, enter "-" without inverted commas - simply -."
    echo "ENTER accepts the specified value in square brackets."
    echo
    read -p "udp port on UDP-target system (use - for no UDP) [$myLOX_PORT]: " LOX_PORT
    LOX_PORT=${LOX_PORT:-$myLOX_PORT}
    if [ ${#LOX_PORT} -le 1 ]; then LOX_PORT=""; fi

    # query local IP-Adress
    if [ "$LB_IP" == "" ] || [ "$LB_IP" == "none" ]; then myLB_IP=`hostname -I | cut -d' ' -f1`; else myLB_IP=$LB_IP; fi
    cls
    echo "$ISTR (3/6)"
    echo
    echo "The weather station must know to which destination it should send the data."
    echo "To do this, the IP address of the host on which FOSHKplugin is running (this"
    echo "system) must be specified."
    echo "ENTER accepts the specified value in square brackets."
    echo
    read -p "enter the ip address of this local system (use - for none) [$myLB_IP]: " LB_IP
    LB_IP=${LB_IP:-$myLB_IP}
    if [ ${#LB_IP} -le 1 ]; then LB_IP=""; fi

    # query the local HTTP port; default: 8080
    cls
    echo "$ISTR (4/6)"
    echo
    echo "The weather station must know to which destination it should send the data."
    echo "For this purpose, in addition to specifying the IP address of the host (just"
    echo "done), also a TCP port must be specified on which FOSHKplugin listens for"
    echo "incoming data from the weather stations."
    echo "A free and thus usable port should be found automatically by this installation"
    echo "routine."
    echo "ENTER accepts the specified value in square brackets."
    if [ "$LBH_PORT" == "" ] || [ "$LBH_PORT" == "unknown" ]; then
      echo
      myLBH_PORT=8080
      tries=50
      v=0
      while [ "`./foshkplugin.py -checkLBHPort $myLBH_PORT`" != "ok" ] && [ $v -lt $tries ]; do
        echo port $myLBH_PORT is already in use
        let myLBH_PORT=$myLBH_PORT+1
        let v=$v+1
      done
      if [ $v -eq $tries ]; then myLBH_PORT="unknown"; fi
    else
      myLBH_PORT=$LBH_PORT
    fi
    echo
    read -p "http port on local system - accept with ENTER [$myLBH_PORT]: " LBH_PORT
    LBH_PORT=${LBH_PORT:-$myLBH_PORT}
    if [ ${#LBH_PORT} -le 1 ]; then LBH_PORT=""; fi

    # inform about all found weather stations
    cls
    echo "$ISTR (5/6)"
    echo
    echo "Please select the weather station that should send the data to this FOSHKplugin"
    echo "installation."
    echo "In addition to the IP address, the command port (default: 45000) and the desired"
    echo "transmission interval of the station to FOSHKplugin are also required."
    echo "The transmission interval specifies the interval at which the weather station"
    echo "sends data to FOSHKplugin (in seconds)."
    echo
    echo "If no station can be found, the configuration can also be set up subsequently"
    echo "via the app or on the station itself. To do this, simply continue here."
    echo "If more than one station is found, enter the IP address of the desired station:"
    ./foshkplugin.py -scanWS
    echo "ENTER accepts the specified value in square brackets."

    # query the IP address of the weather station
    if [ "$WS_IP" == "" ]; then myWS_IP=`./foshkplugin.py -getWSIP`; else myWS_IP=$WS_IP; fi
    if [ "$myWS_IP" == "not found - try again!" ]; then myWS_IP=""; fi
    echo
    read -p "ip address of weather station [$myWS_IP]: " WS_IP
    WS_IP=${WS_IP:-$myWS_IP}
    if [ ${#WS_IP} -le 1 ]; then WS_IP=""; fi

    # query the command port of the weather station
    if [ "$WS_PORT" == "" ]; then myWS_PORT=`./foshkplugin.py -getWSPORT`; else myWS_PORT=$WS_PORT; fi
    if [ "$myWS_PORT" == "not found - try again!" ]; then myWS_PORT="45000"; fi
    echo
    read -p "command port of weather station [$myWS_PORT]: " WS_PORT
    WS_PORT=${WS_PORT:-$myWS_PORT}
    if [ ${#WS_PORT} -le 1 ]; then WS_PORT=""; fi

    # query the interval of the weather station
    if [ "$WS_PORT" == "" ]; then myWS_INTERVAL=`./foshkplugin.py -getWSINTERVAL`; else myWS_INTERVAL=$WS_INTERVAL; fi
    if [ "$myWS_INTERVAL" == "not found - try again!" ]; then myWS_INTERVAL="60"; fi
    echo
    read -p "transmission interval of weather station [$myWS_INTERVAL]: " WS_INTERVAL
    WS_INTERVAL=${WS_INTERVAL:-$myWS_INTERVAL}
    if [ ${#WS_INTERVAL} -le 1 ]; then WS_INTERVAL=""; fi

    # query the servicename to use
    cls
    echo "$ISTR (6/6)"
    echo
    echo "It is recommended to run FOSHKplugin as a systemd service. The name of the"
    echo "service is foshkplugin by default. However, if several parallel FOSHKplugin"
    echo "installations are desired, each service needs its own unique name."
    echo "The service name can be changed here if necessary."
    echo "ENTER accepts the specified value in square brackets."
    echo
    listSVC
    mySVC_NAME=$SVC_NAME
    echo
    read -p "Define the name of the sytemd service: [$mySVC_NAME]: " SVC_NAME
    SVC_NAME=${SVC_NAME:-$mySVC_NAME}
    if [ ${#SVC_NAME} -le 1 ]; then SVC_NAME=""; fi

    # finalise the config-file configuration
    echo
    read -p "$ISTR are these settings ok? (Y/N) " -n 1 -r
  done

  # Create / update config file
  read -t 1 -n 10000 discard
  echo
  read -p "$ISTR write settings into the config-file? (Y/N) " -n 1 -r
  if [[ $REPLY =~ ^[YyJj]$ ]]; then
    echo
    echo $ISTR configuring $PRGNAME
    # make parameter count sure
    if [ "${WS_IP}" = "" ]; then WS_IP="none"; fi
    if [ "${WS_PORT}" = "" ]; then WS_PORT="none"; fi
    if [ "${LB_IP}" = "" ]; then LB_IP="none"; fi
    if [ "${LBH_PORT}" = "" ]; then LBH_PORT="none"; fi
    if [ "${WS_INTERVAL}" = "" ]; then WS_INTERVAL="none"; fi
    if [ "${LOX_IP}" = "" ]; then LOX_IP="none"; fi
    if [ "${LOX_PORT}" = "" ]; then LOX_PORT="none"; fi
    if [ "${SVC_NAME}" = "" ]; then SVC_NAME="none"; fi
    #echo "*${WS_IP}* *${WS_PORT}* *${LB_IP}* *${LBH_PORT}* *${WS_INTERVAL}* *${LOX_IP}* *${LOX_PORT}* *${SVC_NAME}*"
    createConfig=`./foshkplugin.py -createConfig $WS_IP $WS_PORT $LB_IP $LBH_PORT $WS_INTERVAL $LOX_IP $LOX_PORT $SVC_NAME`
  fi

  # Write settings in the weather station
  if [ "$WS_IP" == "none" ] || [ "$WS_PORT" == "none" ] || [ "$LB_IP" == "none" ] || [ "$LBH_PORT" == "none" ] || [ "$WS_INTERVAL" == "none" ]; then
    echo
    echo "use WS View to configure the custom server of your weather station"
    echo "choose your station in the Device List, tap on More on the upper right,"
    echo "then Weather Services"
    echo "tap four times Next on the upper right to get into the Customized section"
    echo "select there Enable, Ecowitt, ip address of the host running FOSHKplugin"
    echo "(this device or it's host address if running in WSL),"
    echo "/data/report/ as path and the port on which FOSHKplugin is listen (${LBH_PORT})"
    echo "tap on the Save button to enable these settisngs in your weather station"
  else
    read -t 1 -n 10000 discard
    echo
    read -p "$ISTR write settings into the weather station? (Y/N) " -n 1 -r
    if [[ $REPLY =~ ^[YyJj]$ ]]; then
      echo
      echo $ISTR configuring weather station
      setWSConfig=`./foshkplugin.py -setWSconfig $WS_IP $WS_PORT $LB_IP $LBH_PORT $WS_INTERVAL`
    fi
  fi
  echo

  # Set up and start the service
  read -t 1 -n 10000 discard
  echo
  read -p "$ISTR enable and start the service? (Y/N) " -n 1 -r
  if [[ $REPLY =~ ^[YyJj]$ ]]; then
    echo $ISTR installing system-service via systemd
    cp -f foshkplugin.service $SVC_NAME.service.tmp
    /bin/sed -i "s#REPLACEFOSHKPLUGINDATADIR#$MYDIR#" $SVC_NAME.service.tmp
    # replace SyslogIdentifier in service.tmp
    /bin/sed -i "s#SyslogIdentifier=foshkplugin#SyslogIdentifier=$SVC_NAME#" $SVC_NAME.service.tmp
    cp -f $SVC_NAME.service.tmp /etc/systemd/system/$SVC_NAME.service
    chown -v "${ISUSR}:${ISUSR}" $SVC_NAME.service.tmp
    sudo systemctl import-environment
    sudo systemctl daemon-reload && sudo systemctl enable $SVC_NAME && sudo systemctl start $SVC_NAME --no-block
  fi

  # adjust file ownerships
  adjustPerm

  # ready - now add the missing settings with an editor if necessary
  echo
  echo
  echo $ISTR everything should be ok now, $PRGNAME should be up and ready
  ps ax|grep foshkplugin|grep -v grep
  echo
  echo $ISTR current configuration is:
  cat foshkplugin.conf
  echo
  echo createConfig: $createConfig
  echo setWSConfig: $setWSConfig
elif [ "$1" = "-repair" ] || [ "$1" = "--repair" ]; then
  echo $ISTR repair installation of $PRGNAME

  # check write permission
  checkWrite

  # install required packages
  aptInstall

  # install required Python packages
  pipInstall

  # adjust file ownerships
  adjustPerm

  # restart the service
  if [ "$2" != "" ]; then SVC_NAME=$2; fi
  sudo systemctl restart $SVC_NAME
  echo
  echo $ISTR repairing installation complete
elif [ "$1" = "-enable" ] || [ "$1" = "--enable" ]; then
  echo
  # which servicename to use?
  if [ "$2" != "" ]; then
    SVC_NAME=$2
  else
    listSVC
    mySVC_NAME=$SVC_NAME
    echo
    read -p "$ISTR Define the name of service to enable: [$mySVC_NAME]: " SVC_NAME
    SVC_NAME=${SVC_NAME:-$mySVC_NAME}
  fi
  echo $ISTR installing $PRGNAME as a system-service $SVC_NAME via systemd
  cp -f foshkplugin.service $SVC_NAME.service.tmp
  /bin/sed -i "s#REPLACEFOSHKPLUGINDATADIR#$MYDIR#" $SVC_NAME.service.tmp
  # replace SyslogIdentifier in service.tmp
  /bin/sed -i "s#SyslogIdentifier=foshkplugin#SyslogIdentifier=$SVC_NAME#" $SVC_NAME.service.tmp
  sudo cp -f $SVC_NAME.service.tmp /etc/systemd/system/$SVC_NAME.service
  chown -v "${ISUSR}:${ISUSR}" $SVC_NAME.service.tmp
  sudo systemctl import-environment
  sudo systemctl daemon-reload && sudo systemctl enable $SVC_NAME && sudo systemctl start $SVC_NAME --no-block
  # replace new service name in config-file
  repl=`grep "^SVC_NAME" foshkplugin.conf`
  /bin/sed -i "s#$repl#SVC_NAME = $SVC_NAME#" foshkplugin.conf
else
  echo you have to use parameter -install, -uninstall, -upgrade, -repair or -enable
  echo
  echo "-install    starts the configuration process"
  echo "-uninstall  uninstalls the given sytemd service"
  echo "-upgrade    may contain a specific filename as 2nd parameter"
  echo "-repair     accepts the service name as a 2nd parameter"
  echo "-enable     accepts the service name as a 2nd parameter"
  echo
  echo "Example:"
  echo "$0 -install"
  echo "$0 -upgrade generic-FOSHKplugin-0.0.9Beta.zip"
  echo "$0 -repair foshkplugin"
  echo
  echo nothing done
fi
echo

