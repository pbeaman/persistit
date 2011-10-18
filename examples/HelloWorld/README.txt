====
    Copyright (C) 2011 Akiban Technologies Inc.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see http://www.gnu.org/licenses.
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
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar HelloWorld.java

To run HelloWorld:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. HelloWorld

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
