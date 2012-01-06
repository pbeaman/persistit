#!/bin/bash
#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
#-----------------------------
# This is a working template for a simple script to perform crash
# recovery testing in Persistit.  This script should be run from the
# root directory of the Persistit project. As configured, this
# script runs the recovery_1.plan stress test for 60 seconds, 
# kills the JVM and then runs the recovery_2.plan to verify
# consistency.
#-----------------------------

sleep_time=60
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

