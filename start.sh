#!/usr/bin/env bash
###################
. /etc/profile
. ~/.bash_profile
##################

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

# Attempt to set JAVA_HOME if it is not set
if [[ -z $JAVA_HOME ]]; then
  # On OSX use java_home (or /Library for older versions)
  if [ "Darwin" == "$(uname -s)" ]; then
    if [ -x /usr/libexec/java_home ]; then
      export JAVA_HOME=($(/usr/libexec/java_home))
    else
      export JAVA_HOME=(/Library/Java/Home)
    fi
  fi

  # Bail if we did not detect it
  if [[ -z $JAVA_HOME ]]; then
    echo "Error: JAVA_HOME is not set and could not be found." 1>&2
    exit 1
  fi
fi

JAVA=$JAVA_HOME/bin/java

OLD=`ps auxf | grep "${MAIN_CLASS}" | grep ${JOBKEEPER_ID}|grep -v "grep"| awk '{print $2}'`
if [ x$OLD = x ] 
  then
  	  touch "${LOGS_DIR}/sys.log"
      nohup nice -n $JOBKEEPER_NICENESS  ${JAVA} ${JAVA_ARGS} -classpath $CLASSPATH ${MAIN_CLASS} ${JOBKEEPER_ID}  $@>>${LOGS_DIR}/sys.log 2>&1 < /dev/null  &
  else
      echo "Jobkeeper is started ,you need to stop it!!!"
      exit 1;
fi
