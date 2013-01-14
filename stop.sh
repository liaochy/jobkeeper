#!/usr/bin/env bash

bin=`dirname "$0"`
JOBKEEPER_HOME=`cd "$bin"; pwd -P`

cd $JOBKEEPER_HOME

if [ -f "${JOBKEEPER_HOME}/env.sh" ]; then
  . "${JOBKEEPER_HOME}/env.sh"
fi

MAIN_CLASS="com.sohu.dc.jobkeeper.Jobkeeper"

SERVER_PID=`ps auxf | grep "${MAIN_CLASS}"| grep ${JOBKEEPER_ID} | grep -v "grep"| awk '{print $2}'`
echo "active interface server pid is ${SERVER_PID}"
if [ -n $SERVER_PID ] 
then
  kill $SERVER_PID
  echo "$SERVER_PID is killed!"
fi
