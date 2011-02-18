#!/usr/bin/env python

import sys
import os
import optparse
import commands
import shutil

usage = """%prog [options]"""

parser = optparse.OptionParser(usage=usage)

parser.add_option(
    "--xmx",
    default = "2G",
    help = "Maximum heap size for the JVM when running stress tests. [default: %default]"
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
            tests.append(filename)

jar_file = "target/akiban-persistit-core-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar"
if not os.path.isfile(jar_file):
    print "PersistIT JAR file does not exist! Did you run mvn install?"
    sys.exit(1)

for test in tests:
    full_test_path = options.test_dir + "/" + test
    test_data_path = options.test_run_dir + "/" + test
    os.makedirs(test_data_path)
    run_cmd = "java -cp %s com.persistit.test.TestRunner script=%s datapath=%s logpath=%s" % (jar_file, full_test_path, test_data_path, test_data_path)
    print "%s\t\t\t" % test,
    (retcode, output) = commands.getstatusoutput(run_cmd)
    if retcode:
        print "[FAIL]"
        sys.exit(1)
    print "[PASS]"
