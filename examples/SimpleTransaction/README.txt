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


SimpleTransaction performs the following steps:

-  Initializes com.persistit.Persistit using a Properties file.
-  Launches multiple threads that concurrently perform simple transactions. 
-  Each transaction randomly transfers "money" between "accounts"
   such that the sum of all "accounts" remains unchanged.
-  Closes Persistit to ensure all records have been written to disk.

To build SimpleTransaction:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleTransaction.java

To run SimpleTransaction:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleTransaction

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.

To verify that transactional integrity is preserved across failures:

	Interrupt SimpleTransaction as it runs by stopping the JVM, shutting down
	the operating system or powering off the computer.
	Restart SimpleTransaction to get a recalculated "balance".  The 
	result should be zero.
	
