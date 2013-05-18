====
    Copyright 2011-2012 Akiban Technologies, Inc.
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
====


SimpleBench is a simple demonstration of Persistit's insert and retrieval
and performance. This class performs the following cycle:

-  Initializes com.persistit.Persistit using built-in default properties.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a tree.
-  Removes any data formerly stored in the tree.
-  Inserts numerous String values, indexed by  small integers.
-  Traverses all keys in forward-sequential order.
-  Traverses all keys in reverse-sequential order.
-  Updates numerous String values with equal length using random keys.
-  Lengthens all String values in forward-seqential key order.
-  Fetches numerous records, using random keys.
-  Flushes all pending updates to disk

To build SimpleBench:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleBench.java

To run SimpleBench:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleBench

By default, SimpleBench will create its volume, prewrite journal and log files
in your current working directory. You can change the location of these files
by specifying system properties, for example:

	java -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;.
		-Dcom.persistit.datapath=/myDataPath
		-Dcom.persistit.logpath=/myLogPath
		 SimpleBench
