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

SimpleTransaction performs the following steps:

-  Initializes com.persistit.Persistit using a Properties file.
-  Launches multiple threads that concurrently perform simple transactions. 
-  Each transaction randomly transfers "money" between "accounts"
   such that the sum of all "accounts" remains unchanged.
-  Closes Persistit to ensure all records have been written to disk.

To build SimpleTransaction:

	Run Ant on build.xml in this directory (target "compile")
	
	- or -
	
	javac -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar SimpleTransaction.java

To run SimpleTransaction:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../target/akiban-persistit-2.1-SNAPSHOT-jar-with-dependencies-and-tests.jar;. SimpleTransaction

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.

To verify that transactional integrity is preserved across failures:

	Interrupt SimpleTransaction as it runs by stopping the JVM, shutting down
	the operating system or powering off the computer.
	Restart SimpleTransaction to get a recalculated "balance".  The 
	result should be zero.
	
