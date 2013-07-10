#!/bin/sh
bin=`dirname "$0"`
JOBKEEPER_HOME=`cd "$bin"; pwd -P`
cd $JOBKEEPER_HOME

MAIN_CLASS="com.sohu.cyril.EtlJob"
SERVER_PID=`ps auxf | grep "${MAIN_CLASS}" | grep -v "grep"| awk '{print $2}'`
if [ -z "$SERVER_PID" ]
then
	su - metl /opt/DATA/goldmine/sohuwl/EtlJob-0.0.1-SNAPSHOT/bin/job-daemon.sh job start
fi
