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


HelloWorld performs the following steps:

-  Initializes com.persistit.Persistit using a Properties file.
-  Creates a com.persistit.Exchange with which to store and fetch
   data to and from a com.persistit.Tree.
-  Stores a key/value pair.
-  Traverses the tree to find and display the key and value.
-  Closes Persistit to ensure all records have been written to disk.

To build HelloWorld:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../core.target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar HelloWorld.java

To run HelloWorld:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../core/target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;. HelloWorld

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
