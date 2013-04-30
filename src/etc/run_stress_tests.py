#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2011-2012 Akiban Technologies, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import os
import optparse
import commands
import shutil
import time

usage = """%prog [options]"""

parser = optparse.OptionParser(usage=usage)

parser.add_option(
    "--xmx",
    default = "2G",
    help = "Maximum heap size for the JVM when running stress tests. [default: %default]"
)

parser.add_option(
    "--jvm-opts",
    default = "-ea -Dcom.sun.management.jmxremote.port=8082 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y",
    help = "Extra options to pass to the JVM (e.g. JMX, debug). [default: %default]"
)

parser.add_option(
    "--test-dir",
    default = "./src/test/resources/tscripts",
    help = "Directory where test scripts are located. [default: %default]"
)

parser.add_option(
    "--test-run-dir",
    default = "./target/stress_test_runs",
    help = "Directory where all artifacts from stress tests are placed. [default: %default]"
)

parser.add_option(
    "--tests",
    default = "",
    help = "Comma separated list of tests to run. (By default all tests are run)"
)

parser.add_option(
    "--jar-file",
    default = "target",
    help = "Path to JAR file to use. [default: %default]"
)

(options, args) = parser.parse_args()

if not os.path.exists(options.test_dir):
    print "Test script directory is not valid."
    sys.exit(1)

# we create the test run directory
# if it already exists, we blow it away
if not os.path.exists(options.test_run_dir):
    os.makedirs(options.test_run_dir)
else:
    shutil.rmtree(options.test_run_dir)
    os.makedirs(options.test_run_dir)

tests = []
if options.tests != "":
    tests = [test for test in options.tests.split(",")]

# if the list of tests specified by the user is empty then
# we populate this list based on the *.plan files in the
# test_dir
if not tests:
    for root, dirs, files in os.walk(options.test_dir):
        for filename in files:
            # ignore any of the stress tests with 10 in them for now
            if filename.find("10") == -1:
                tests.append(filename)

jar_file = options.jar_file
# pick up the default jar file generated if no jar file is specified
if jar_file == "target":
    cmd = "grep version pom.xml | grep SNAPSHOT"
    (retcode, output) = commands.getstatusoutput(cmd)
    version = output.lstrip()[9:output.lstrip().find('SNAPSHOT')-1]
    jar_file = "target/akiban-persistit-%s-SNAPSHOT-jar-with-dependencies-and-tests.jar" % version

if not os.path.isfile(jar_file):
    print "Persistit JAR file does not exist! Did you run mvn install?"
    sys.exit(1)

test_failures = 0

print "starting test run at: %s\n\n" % time.asctime()

for test in tests:
    full_test_path = options.test_dir + "/" + test
    test_data_path = options.test_run_dir + "/" + test
    os.makedirs(test_data_path)
    run_cmd = "java %s -Xmx%s -cp %s com.persistit.test.TestRunner script=%s datapath=%s logpath=%s" % (options.jvm_opts, options.xmx, jar_file, full_test_path, test_data_path, test_data_path)
    print "%s\t\t\t" % test,
    (retcode, output) = commands.getstatusoutput(run_cmd)
    if retcode:
        print "[FAIL]"
        test_failures = test_failures + 1
    else:
        print "[PASS]"

print "\n\nfinished test run at: %s\n" % time.asctime()
print "tests run    : %d" % len(tests)
print "test failures: %d" % test_failures

if test_failures:
    sys.exit(1)
