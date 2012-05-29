====
    Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, version 3 (only) of the
    License.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
    
    This program may also be available under different license terms. For more
    information, see www.akiban.com or contact licensing@akiban.com.
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
	
	javac -classpath ../../lib/persistit_jsa110.jar SimpleDemo.java

To run SimpleDemo:

	Run Ant on build.xml in this directory (target "run")
	
	- or -
	
	java -classpath ../../lib/persistit_jsa110.jar;. SimpleDemo

To run SimpleDemo without building it first:

	java -classpath ../../lib/persistit_jsa110.jar;../../lib/examples.jar SimpleDemo

Persistit will place a volume file in paths specified by persistit.properties.
You can change the location of these files by modifying the datapath property
of this configuration file.
