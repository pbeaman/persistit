#!/bin/bash

sleep_time=10
datapath="/tmp/persistit_test_data"
scriptpath="core/src/test/resources/tscripts"
jvm_options="-Xmx2G -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y"

testrunner_command="java $jvm_options -cp `find . -name akiban-persistit-core*with-dependencies*.jar` -Dstress_recovery_test_signature=1 com.persistit.test.TestRunner "

command="$testrunner_command script=$scriptpath/recovery_1.plan datapath=$datapath"
echo "$command  > $datapath/recovery.log"
$command > $datapath/recovery.log &

echo "Sleeping for $sleep_time seconds"
sleep $sleep_time

background_pid=`pgrep -f "stress_recovery_test_signature=1"`
echo "Killing pid $background_pid"
kill -9 $background_pid
sleep 2

command="$testrunner_command script=$scriptpath/recovery_2.plan datapath=$datapath"
echo $command
$command

