#!/usr/bin/env bash
bin=`dirname "$0"`
JOBKEEPER_HOME=`cd "$bin"; pwd -P`

cd $JOBKEEPER_HOME

LIB_DIR=${JOBKEEPER_HOME}/lib
LOGS_DIR=${JOBKEEPER_HOME}/logs

chmod -R 777 ${LOGS_DIR}
JOBKEEPER_NICENESS=0

MAIN_CLASS="com.sohu.dc.jobkeeper.Jobkeeper"
JAVA_ARGS="-server -Xms1024m -Xmx1024m -XX:NewSize=128m -XX:MaxNewSize=128m -XX:ParallelGCThreads=4 -XX:+DisableExplicitGC -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC  -XX:CMSInitiatingOccupancyFraction=68 -XX:PermSize=96m -XX:MaxPermSize=96m -XX:ThreadStackSize=160 -Dlog.dir=$LOGS_DIR"

#CLASSPATH=./:/usr/local/java/jdk/default/lib:/usr/local/java/jdk/default/lib/tools.jar
CLASSPATH=./:$CLASSPATH:${JOBKEEPER_HOME}/classes/
files=`ls -1 ${LIB_DIR}`
for file in ${files} ;do
        CLASSPATH=$CLASSPATH:${LIB_DIR}/${file}
done

if [ -f "${JOBKEEPER_HOME}/env.sh" ]; then
  . "${JOBKEEPER_HOME}/env.sh"
fi
OLD=`ps auxf | grep "${MAIN_CLASS}" | grep ${JOBKEEPER_ID}|grep -v "grep"| awk '{print $2}'`
if [ x$OLD = x ] 
  then
  	  touch "${LOGS_DIR}/sys.log"
      nohup nice -n $JOBKEEPER_NICENESS  java ${JAVA_ARGS} -classpath $CLASSPATH ${MAIN_CLASS} ${JOBKEEPER_ID}  $@>>${LOGS_DIR}/sys.log 2>&1 < /dev/null  &
  else
      echo "Jobkeeper is started ,you need to stop it!!!"
      exit 1;
fi
