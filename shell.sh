#!/usr/bin/env bash
###################
. /etc/profile
. ~/.bash_profile
##################
TIME=`date +%Y%m%d%H%M`
echo "$TIME:$@"
bin=$1
shift
user=$1
shift

su - $user $bin $@ >>${bin}.log 2>&1 &