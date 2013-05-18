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


PersistitMapDemo uses a com.persistit.PersistitMap, which
implements java.util.SortedMap, to store a copy of the system
properties. Each time you run this program it will compare the current
system properties to those that were stored previously and will
display any differences.

To build PersistitMapDemo:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../core/target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar PersistitMapDemo.java

To run PersistitMapDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../core/target/akiban-persistit-x.y.z-SNAPSHOT-jar-with-dependencies-and-tests.jar;. PersistitMapDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
