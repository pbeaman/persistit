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


Copyright (c) 2004 Persistit Corporation. All Rights Reserved.

SimpleDemo is a very simple example to demonstrate basic operations of
Persistit. This class performs the following operations:

-  Initializes com.persistit.Persistit using a Properties file.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a com.persistit.Tree.
-  Reads some names from the console and stores records in the <tt>Tree</tt>
   indexed by those names.
-  Dispays the names in alphabetical order.
-  Closes Persistit to ensure all records have been written to disk.

To build SimpleDemo:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
    javac -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleDemo.java

To run SimpleDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
